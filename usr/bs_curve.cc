/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: 2021-12-22
 * Author: XuYifeng
 */

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <libcurve.h>

extern "C" {
#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "target.h"
#include "scsi.h"
}

#define __predict_true(exp)     __builtin_expect((exp), 1)
#define __predict_false(exp)    __builtin_expect((exp), 0)

struct bs_curve_info {
	int curve_fd;
	int evt_fd;
	char *curve_name;
	pthread_mutex_t mutex;
	struct list_head cmd_inflight_list;
	struct list_head cmd_complete_list;
	unsigned int ninflight;
	unsigned int ncomplete;
	struct scsi_lu *lu;

	pthread_t *worker_thread;
	int nr_worker_threads;
	int stop;

	/* workers sleep on this and signaled by tgtd */
	pthread_cond_t pending_cond;
	/* locked by tgtd and workers */
	pthread_mutex_t pending_lock;
	/* protected by pending_lock */
	struct list_head cmd_pending_list;
};

typedef struct bs_curve_iocb {
	struct CurveAioContext ctx;
	struct scsi_cmd *cmd;
	struct bs_curve_info *info;

#define WRITE_SAME_MAX_DUP 128
	struct write_same {
		uint64_t offset;
		uint32_t tl;
		uint32_t tmplen;
		char *tmpbuf;
	} ws;
} bs_curve_iocb_t;

using namespace curve::client;
static curve::client::CurveClient *g_curve;

static void cmd_submit(struct bs_curve_info *info, struct scsi_cmd *cmd);
static void cmd_done(struct bs_curve_info *info, struct scsi_cmd *cmd);

#define CURVE_CONF_PATH "/etc/curve/client.conf"
                                                                                
static int tgt_init_curve(void)
{
	if (g_curve)
		return 0;

	int ret = 0;
	curve::client::CurveClient *curve_client;
	curve_client = new curve::client::CurveClient;
	if (curve_client->Init(CURVE_CONF_PATH)) {
		eprintf("can not init curve client, errno=%d\n", errno);
		delete curve_client;
		ret = -1;
	}
	g_curve = curve_client;
	return ret;
}

static int tgt_curvedev_get_path(const char *curve_path, char* path)
{
	strcpy(path, curve_path);
	char *p = strstr(path, "//");
	if (p == NULL) {
		errno = EINVAL;
		return -1;
	}
    strcpy(path, p+1);                                                          
    path[0] = '/'; 
	return 0;
}

static inline struct bs_curve_info *BS_CURVE_I(struct scsi_lu *lu)
{
	return (struct bs_curve_info *) ((char *)lu + sizeof(*lu));
}

static void *thread_cmd_worker(void *arg)
{
	struct bs_curve_info *info = (struct bs_curve_info *)arg;
	struct scsi_cmd *cmd;

	while (1) {
		pthread_mutex_lock(&info->pending_lock);
		while (!info->stop && list_empty(&info->cmd_pending_list))
			pthread_cond_wait(&info->pending_cond,
					  &info->pending_lock);
		if (__predict_false(info->stop != 0)) {
			pthread_mutex_unlock(&info->pending_lock);
			break;
		}

		struct list_head temp_list;
		INIT_LIST_HEAD(&temp_list);
		list_splice_init(&info->cmd_pending_list, &temp_list);
		pthread_mutex_unlock(&info->pending_lock);

		while (!list_empty(&temp_list)) {
			cmd = list_first_entry(&temp_list, struct scsi_cmd, bs_list);
			list_del(&cmd->bs_list);
			cmd_submit(info, cmd);
		}
	}

	pthread_exit(NULL);
}

static void thread_group_stop(struct bs_curve_info *info)
{
	pthread_mutex_lock(&info->pending_lock);
	info->stop = 1;
	pthread_cond_broadcast(&info->pending_cond);
	pthread_mutex_unlock(&info->pending_lock);
}

static tgtadm_err thread_group_open(struct bs_curve_info *info, int nr_threads)
{
	int i, ret;

	info->stop = 0;
	info->worker_thread = (pthread_t *)zalloc(sizeof(pthread_t) * nr_threads);
	if (!info->worker_thread)
		return TGTADM_NOMEM;

	for (i = 0; i < nr_threads; i++) {
		ret = pthread_create(&info->worker_thread[i], NULL,
				     thread_cmd_worker, info);

		if (ret) {
			eprintf("failed to create a worker thread, %d %s\n",
				i, strerror(ret));
			goto destroy_threads;
		}
	}
	info->nr_worker_threads = nr_threads;

	return TGTADM_SUCCESS;

destroy_threads:
	thread_group_stop(info);
	for (; i > 0; i--) {
		if (info->worker_thread[i - 1]) {
			pthread_join(info->worker_thread[i - 1], NULL);
			dprintf("stopped the worker thread %d\n", i - 1);
		}
	}

	free(info->worker_thread);
	info->worker_thread = NULL;
	return TGTADM_NOMEM;
}

static void thread_group_close(struct bs_curve_info *info)
{
	int i;

	thread_group_stop(info);
	for (i = 0; i < info->nr_worker_threads && info->worker_thread[i]; i++)
		pthread_join(info->worker_thread[i], NULL);
	free(info->worker_thread);
	info->worker_thread = NULL;
	info->nr_worker_threads = 0;
}

static inline void bs_curve_enq_inflight_io(struct bs_curve_info *info,
		struct scsi_cmd *cmd)
{
	list_add_tail(&cmd->bs_list, &info->cmd_inflight_list);
	info->ninflight++;
}

static inline void bs_curve_deq_inflight_io(struct bs_curve_info *info,
		struct scsi_cmd *cmd)
{
	list_del(&cmd->bs_list);
	info->ninflight--;
}

static inline int bs_curve_enq_complete_io(struct bs_curve_info *info,
		struct scsi_cmd *cmd)
{
	int empty = list_empty(&info->cmd_complete_list);
	list_add_tail(&cmd->bs_list, &info->cmd_complete_list);
	info->ncomplete++;
	return empty;
}

/*
static inline void bs_curve_deq_complete_io(struct bs_curve_info *info,
		struct scsi_cmd *cmd)
{
	list_del(&cmd->bs_list);
	info->ncomplete--;
}
*/

static inline void set_medium_error_r(int *result, uint8_t *key, uint16_t *asc)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = MEDIUM_ERROR;
	*asc = ASC_READ_ERROR;
}

static inline void set_medium_error_w(int *result, uint8_t *key, uint16_t *asc)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = MEDIUM_ERROR;
	*asc = ASC_WRITE_ERROR;
}

static void cmd_done(struct bs_curve_info *info, struct scsi_cmd *cmd)
{
	pthread_mutex_lock(&info->mutex);
	bs_curve_deq_inflight_io(info, cmd);
	int empty = bs_curve_enq_complete_io(info, cmd);
	pthread_mutex_unlock(&info->mutex);

	if (!empty)
		return;

	/* This is first entry, needs to signal other threads */
	int64_t counter = 1;

	while (write(info->evt_fd, &counter, 8) < 0) {
		if (errno == EINTR)
			continue;
		break;
	}
}

static void bs_curve_aio_callback(struct CurveAioContext* curve_ctx)
{
	bs_curve_iocb_t *iocb = container_of(curve_ctx, bs_curve_iocb_t, ctx);
	struct scsi_cmd *cmd = iocb->cmd;
	struct bs_curve_info *info = iocb->info;
	int result = SAM_STAT_GOOD;
	int asc = 0;

	switch (cmd->scb[0]) {
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		asc = ASC_WRITE_ERROR;
		break;
	default:
		asc = ASC_READ_ERROR;
		break;
	}

	if ((size_t)iocb->ctx.ret == iocb->ctx.length)
		result = SAM_STAT_GOOD;
	else {
		result = SAM_STAT_CHECK_CONDITION;
		eprintf("io error %p %x %d %d %ld %" PRIu64 ", %m\n",
                        cmd, cmd->scb[0], iocb->ctx.op, iocb->ctx.ret, iocb->ctx.length,
			iocb->ctx.offset);
		sense_data_build(cmd, MEDIUM_ERROR, asc);
	}

	scsi_set_result(cmd, result);
	cmd_done(info, cmd);

	free(iocb);
}

static void bs_curve_get_completions(struct tgt_evloop* evloop, int fd, int events, void *data)
{
	struct bs_curve_info *info = (struct bs_curve_info *)data;
	struct list_head temp_list;
	struct scsi_cmd *cmd;
	/* read from eventfd returns 8-byte int, fails with the error EINVAL
	   if the size of the supplied buffer is less than 8 bytes */
	uint64_t evts_complete;
	int ret;

retry_read:
	ret = read(info->evt_fd, &evts_complete, sizeof(evts_complete));
	if (unlikely(ret < 0)) {
		eprintf("failed to read AIO completions, %m\n");
		if (errno == EAGAIN || errno == EINTR)
			goto retry_read;

		// return;
	}

	INIT_LIST_HEAD(&temp_list);
	pthread_mutex_lock(&info->mutex);
	list_splice_init(&info->cmd_complete_list, &temp_list);
	info->ncomplete = 0;
	pthread_mutex_unlock(&info->mutex);

	while (!list_empty(&temp_list)) {
		cmd = list_first_entry(&temp_list, struct scsi_cmd, bs_list);
		list_del(&cmd->bs_list);
		dprintf("back to tgtd, %p\n", cmd);
		target_cmd_io_done(cmd, scsi_get_result(cmd));
	}
}

static int bs_curve_io_prep_pread(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	iocb->ctx.offset = iocb->cmd->offset;
	iocb->ctx.length = scsi_get_in_length(iocb->cmd);
	iocb->ctx.buf = scsi_get_in_buffer(iocb->cmd);
	iocb->ctx.op = LIBCURVE_OP_READ;
	iocb->ctx.cb = bs_curve_aio_callback;
	return 0;
}

static int bs_curve_io_prep_pwrite(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	iocb->ctx.offset = iocb->cmd->offset;
	iocb->ctx.length = scsi_get_out_length(iocb->cmd);
	iocb->ctx.buf = scsi_get_out_buffer(iocb->cmd);
	iocb->ctx.op = LIBCURVE_OP_WRITE;
	iocb->ctx.cb = bs_curve_aio_callback;
	return 0;
}

static void bs_curve_aio_write_same_callback(struct CurveAioContext* curve_ctx);
static int bs_curve_io_prep_write_same_2(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	struct scsi_cmd *cmd = iocb->cmd;
	off_t offset = iocb->ws.offset;
	size_t blocksize = 1 << cmd->dev->blk_shift;
        int tmpblockcount = (iocb->ws.tmplen >> cmd->dev->blk_shift);

	for (int i = 0; i < tmpblockcount; ++i) {
		char *tmpbuf = iocb->ws.tmpbuf + i * blocksize;
		off_t tmpoff = offset + i * blocksize;
		switch(cmd->scb[1] & 0x06) {
		case 0x02: /* PBDATA==0 LBDATA==1 */
			put_unaligned_be32(tmpoff, tmpbuf);
			break;
		case 0x04: /* PBDATA==1 LBDATA==0 */
			/* physical sector format */
			put_unaligned_be64(tmpoff, tmpbuf);
			break;
		}
	}
	iocb->ctx.offset = iocb->ws.offset;
	iocb->ctx.length = min(iocb->ws.tl, iocb->ws.tmplen);
	iocb->ctx.buf = iocb->ws.tmpbuf;
	iocb->ctx.op = LIBCURVE_OP_WRITE;
	iocb->ctx.cb = bs_curve_aio_write_same_callback;
	return 0;
}

static void bs_curve_aio_write_same_callback(struct CurveAioContext* curve_ctx)
{
	bs_curve_iocb_t *iocb = container_of(curve_ctx, bs_curve_iocb_t, ctx);
	struct scsi_cmd *cmd = iocb->cmd;
	struct bs_curve_info *info = iocb->info;
	int result = SAM_STAT_GOOD;
	uint8_t key = 0;
	uint16_t asc = 0;

	// update WRITE_SAME state machine
	iocb->ws.tl -= iocb->ctx.length;
	iocb->ws.offset += iocb->ctx.length;

	if (iocb->ws.tl <= 0 || iocb->ctx.ret < 0) {
		// complete or failure
stop:
		free(iocb->ws.tmpbuf);

		if (iocb->ctx.ret < 0) {
			eprintf("%s io error %p %x %d %d %ld %" PRIu64 ", %m\n", __func__,
                        	cmd, cmd->scb[0], iocb->ctx.op, iocb->ctx.ret, iocb->ctx.length,
				iocb->ctx.offset);
			set_medium_error_w(&result, &key, &asc);
			sense_data_build(cmd, key, asc);
		}
		scsi_set_result(cmd, result);
		cmd_done(info, cmd);
	} else {
		// continue WRITE_SAME
		bs_curve_io_prep_write_same_2(info, iocb);
		if (g_curve->AioWrite(info->curve_fd, &iocb->ctx, UserDataType::RawBuffer)) {
			iocb->ctx.ret = -1;
			goto stop;
		}
	}
}

static int bs_curve_io_prep_write_same(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	struct scsi_cmd *cmd = iocb->cmd;
	char *buf = (char *)scsi_get_out_buffer(cmd);
	off_t offset = cmd->offset;
	uint32_t blocksize = (1 << cmd->dev->blk_shift);
	int tmpblockcount = 0;

	iocb->ws.tl = cmd->tl;
	if (iocb->ws.tl == 0) {
		/*
		 * A NUMBER OF BLOCKS value of zero requests that all
		 * the remaining logical blocks on the medium be written.
		 */
		iocb->ws.tl = cmd->dev->size;
	}
	iocb->ws.tmplen = min(cmd->tl, blocksize * WRITE_SAME_MAX_DUP);
	iocb->ws.offset = cmd->offset;
	iocb->ws.tmpbuf = (char *)malloc(iocb->ws.tmplen);
	if (iocb->ws.tmpbuf == NULL) {
		eprintf("%s can not allocate memory", __func__);
		return -1;
	}
	tmpblockcount = iocb->ws.tmplen >> (cmd->dev->blk_shift);
	for (int i = 0; i < tmpblockcount; ++i) {
		char *tmpbuf = iocb->ws.tmpbuf + i * blocksize;
		off_t tmpoff = offset + i * blocksize;
		memcpy(tmpbuf, buf, blocksize);
		switch(cmd->scb[1] & 0x06) {
		case 0x02: /* PBDATA==0 LBDATA==1 */
			put_unaligned_be32(tmpoff, tmpbuf);
			break;
		case 0x04: /* PBDATA==1 LBDATA==0 */
			/* physical sector format */
			put_unaligned_be64(tmpoff, tmpbuf);
			break;
		}
	}

	iocb->ctx.offset = iocb->ws.offset;
	iocb->ctx.length = min(iocb->ws.tl, iocb->ws.tmplen);
	iocb->ctx.buf = iocb->ws.tmpbuf;
	iocb->ctx.op = LIBCURVE_OP_WRITE;
	iocb->ctx.cb = bs_curve_aio_write_same_callback;
	return 0;
}

static void cmd_submit(struct bs_curve_info *info, struct scsi_cmd *cmd)
{
	//struct scsi_lu *lu = cmd->dev;
	bs_curve_iocb_t *iocb = NULL;
	unsigned int scsi_op = (unsigned int)cmd->scb[0];
	int result = SAM_STAT_GOOD;
	uint8_t key = 0;
	uint16_t asc = 0;
	int length = 0;

	pthread_mutex_lock(&info->mutex);
	bs_curve_enq_inflight_io(info, cmd);
	pthread_mutex_unlock(&info->mutex);

	switch (scsi_op) {
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		length = scsi_get_out_length(cmd);
		iocb = (bs_curve_iocb_t *)calloc(1, sizeof(bs_curve_iocb_t));
		iocb->cmd = cmd;
		iocb->info = info;
		if (bs_curve_io_prep_pwrite(info, iocb)) {
			set_medium_error_w(&result, &key, &asc);
			break;
		}
		if (g_curve->AioWrite(info->curve_fd, &iocb->ctx, UserDataType::RawBuffer)) {
			set_medium_error_w(&result, &key, &asc);
			break;
		}
		break;
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		length = scsi_get_in_length(cmd);
		iocb = (bs_curve_iocb_t *)calloc(1, sizeof(bs_curve_iocb_t));
		iocb->cmd = cmd;
		iocb->info = info;
		if (bs_curve_io_prep_pread(info, iocb)) {
			set_medium_error_w(&result, &key, &asc);
			break;
		}
		if (g_curve->AioRead(info->curve_fd, &iocb->ctx, UserDataType::RawBuffer)) {
			set_medium_error_r(&result, &key, &asc);
			break;
		}
		break;
	case WRITE_SAME:
	case WRITE_SAME_16:
		/* WRITE_SAME used to punch hole in file */
		if (cmd->scb[1] & 0x08) {
			eprintf("get WRITE_SAME with unmap!");
			set_medium_error_w(&result, &key, &asc);
			break;
		}

		dprintf("got WRITE_SAME command, offset=%ld bufsz=%d tl=%d\n",
			 cmd->offset, scsi_get_out_length(cmd), cmd->tl);

		length = cmd->tl;
		iocb = (bs_curve_iocb_t *)calloc(1, sizeof(bs_curve_iocb_t));
		iocb->cmd = cmd;
		iocb->info = info;
		if (bs_curve_io_prep_write_same(info, iocb)) {
			set_medium_error_w(&result, &key, &asc);
			break;
		}
		if (g_curve->AioWrite(info->curve_fd, &iocb->ctx, UserDataType::RawBuffer)) {
			set_medium_error_w(&result, &key, &asc);
			break;
		}

		break;
	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
	default:
		dprintf("skipped cmd:%p op:%x\n", cmd, scsi_op);
		scsi_set_result(cmd, result);
		cmd_done(info, cmd);
		return;
	}

	if (__predict_false(result != SAM_STAT_GOOD)) {
		eprintf("io error %p %x %d %d %" PRIu64 ", %m\n",
			cmd, cmd->scb[0], result, length, cmd->offset);
		scsi_set_result(cmd, result);
		sense_data_build(cmd, key, asc);
		cmd_done(info, cmd);
		free(iocb);
	}
}

static int bs_curve_cmd_submit(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu = cmd->dev;
	struct bs_curve_info *info = BS_CURVE_I(lu);

	set_cmd_async(cmd);

	pthread_mutex_lock(&info->pending_lock);
	list_add_tail(&cmd->bs_list, &info->cmd_pending_list);
	pthread_mutex_unlock(&info->pending_lock);
	pthread_cond_signal(&info->pending_cond);

	return 0;
}

static int bs_curve_open(struct scsi_lu *lu, char *dev, int *fd, uint64_t *size)
{
	char path[PATH_MAX];
	struct bs_curve_info *info = BS_CURVE_I(lu);
	int ret, afd;
	uint32_t blksize = 0;
	int64_t curve_size = 0;
	OpenFlags openflags;

	ret = tgt_curvedev_get_path(dev, path);
	if (ret != 0) {
		eprintf("curvedev get path, errno=%d\n", ret);
		return -1;
	}

	openflags.exclusive = false;
	info->curve_fd = g_curve->Open(path, openflags);
	if (info->curve_fd < 0) {
		eprintf("can not open curve volume %s, %s\n", path, strerror(errno));
		return -1;
	}
	blksize = 4096; //FIXME

	curve_size = g_curve->StatFile(path);
	if (curve_size < 0) {
		g_curve->Close(info->curve_fd);
        	info->curve_fd = -1;
	    	eprintf("failed to StateFile %s\n", path);
		return -1;
	}

	eprintf("opened curve volume %s for tgt:%d lun:%" PRId64 ", curve_fd:%d, size:%zd\n",
		 path, info->lu->tgt->tid, info->lu->lun, info->curve_fd, curve_size);

	*size = curve_size;

	afd = eventfd(0, O_NONBLOCK);
	if (afd < 0) {
		eprintf("failed to create eventfd for tgt:%d lun:%" PRId64 ", %m\n",
			info->lu->tgt->tid, info->lu->lun);
		ret = afd;
		goto close_curve;
	}
	dprintf("eventfd:%d for tgt:%d lun:%" PRId64 "\n",
		afd, info->lu->tgt->tid, info->lu->lun);

	ret = tgt_event_insert(lu->tgt->evloop, afd, EPOLLIN, bs_curve_get_completions, info);
	if (ret)
		goto close_eventfd;
	info->evt_fd = afd;

	if (!lu->attrs.no_auto_lbppbe)
		update_lbppbe(lu, blksize);
	/*
	 * curve cloud storage does not have local cache, and all data written
	 * to chunk servers will be persisted on disk immediately.
	 */
	lu->attrs.dpofua = 1;
	info->curve_name = strdup(path);
	ret = thread_group_open(info, 1);
	if (ret != 0)
		goto delete_event;
	return 0;

delete_event:
	info->curve_name = NULL;
	tgt_event_delete(lu->tgt->evloop, afd);
close_eventfd:
	close(afd);
	info->evt_fd = -1;
close_curve:
	g_curve->Close(info->curve_fd);
	info->curve_fd = -1;
	free(info->curve_name);
	info->curve_name = NULL;
	return ret;
}

static void bs_curve_close(struct scsi_lu *lu)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);

	thread_group_close(info);
	if (info->curve_fd >= 0) {
		g_curve->Close(info->curve_fd);
		info->curve_fd = -1;
	}
	if (info->evt_fd >= 0) {
		tgt_event_delete(lu->tgt->evloop, info->evt_fd);
		info->evt_fd = -1;
	}
	free(info->curve_name);
}

static tgtadm_err bs_curve_init(struct scsi_lu *lu, char *bsopts)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);

	if (tgt_init_curve())
		return TGTADM_UNKNOWN_ERR;

	memset(info, 0, sizeof(*info));
	info->curve_fd = -1;
	info->evt_fd = -1;
	pthread_mutex_init(&info->mutex, NULL);
	INIT_LIST_HEAD(&info->cmd_inflight_list);
	INIT_LIST_HEAD(&info->cmd_complete_list);
	info->lu = lu;

	pthread_mutex_init(&info->pending_lock, NULL);
	pthread_cond_init(&info->pending_cond, NULL);
	INIT_LIST_HEAD(&info->cmd_pending_list);
	return TGTADM_SUCCESS;
}

static void bs_curve_exit(struct scsi_lu *lu)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);

	pthread_mutex_destroy(&info->mutex);
	pthread_mutex_destroy(&info->pending_lock);
	pthread_cond_destroy(&info->pending_cond);
}

static int bs_curve_getlength(struct scsi_lu *lu, uint64_t *size)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);
	int64_t curve_size = g_curve->StatFile(info->curve_name);
	if (curve_size >= 0) {
		*size = curve_size;
	    eprintf("StateFile %zd\n", curve_size);
		return 0;
	}
	eprintf("failed to StateFile %s\n", info->curve_name);
	return -1;
}

static struct backingstore_template curve_bst = {
	.bs_name		= "curve",
	.bs_datasize    	= sizeof(struct bs_curve_info),
	.bs_open		= bs_curve_open,
	.bs_close       	= bs_curve_close,
	.bs_init		= bs_curve_init,
	.bs_exit		= bs_curve_exit,
	.bs_cmd_submit  	= bs_curve_cmd_submit,
	.bs_getlength		= bs_curve_getlength,
	.bs_oflags_supported    = O_SYNC | O_DIRECT
};

extern "C" void register_bs_module(void)
{
	unsigned char opcodes[] = {
		ALLOW_MEDIUM_REMOVAL,
		FORMAT_UNIT,
		INQUIRY,
		MAINT_PROTOCOL_IN,
		MODE_SELECT,
		MODE_SELECT_10,
		MODE_SENSE,
		MODE_SENSE_10,
		PERSISTENT_RESERVE_IN,
		PERSISTENT_RESERVE_OUT,
		READ_10,
		READ_12,
		READ_16,
		READ_6,
		READ_CAPACITY,
		RELEASE,
		REPORT_LUNS,
		REQUEST_SENSE,
		RESERVE,
		SEND_DIAGNOSTIC,
		SERVICE_ACTION_IN,
		START_STOP,
		SYNCHRONIZE_CACHE,
		SYNCHRONIZE_CACHE_16,
		TEST_UNIT_READY,
		WRITE_10,
		WRITE_12,
		WRITE_16,
		WRITE_6,
		WRITE_SAME,
		WRITE_SAME_16
	};

	bs_create_opcode_map(&curve_bst, opcodes, ARRAY_SIZE(opcodes));
	register_backingstore_template(&curve_bst);
}

