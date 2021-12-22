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
#include <nebd/libnebd.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "target.h"
#include "scsi.h"

struct bs_curve_info {
	int curve_fd;
	int evt_fd;
	pthread_mutex_t mutex;
	pthread_rwlock_t close_lock;
	struct list_head cmd_inflight_list;
	struct list_head cmd_complete_list;
	unsigned int ninflight;
	unsigned int ncomplete;
	struct scsi_lu *lu;
};

typedef struct bs_curve_iocb {
	struct NebdClientAioContext ctx;
	struct scsi_cmd *cmd;
	struct bs_curve_info *info;
	struct list_head bs_list;
} bs_curve_iocb_t;

static inline struct bs_curve_info *BS_CURVE_I(struct scsi_lu *lu)
{
	return (struct bs_curve_info *) ((char *)lu + sizeof(*lu));
}

static void bs_curve_enq_inflight_io(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	list_add_tail(&iocb->bs_list, &info->cmd_inflight_list);
	info->ninflight++;
}

static void bs_curve_deq_inflight_io(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	list_del(&iocb->bs_list);
	info->ninflight--;
}

static void bs_curve_enq_complete_io(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	list_add_tail(&iocb->bs_list, &info->cmd_complete_list);
	info->ncomplete++;
}

/*
static void bs_curve_deq_complete_io(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	list_del(&iocb->bs_list);
	info->ncomplete--;
}
*/

static void bs_curve_aio_callback(struct NebdClientAioContext* curve_ctx)
{
	bs_curve_iocb_t *iocb = container_of(curve_ctx, bs_curve_iocb_t, ctx);
	struct bs_curve_info *info = iocb->info;
	int64_t count = 1;

	pthread_rwlock_rdlock(&info->close_lock);

	pthread_mutex_lock(&info->mutex);
	bs_curve_deq_inflight_io(info, iocb);
	bs_curve_enq_complete_io(info, iocb);
	pthread_mutex_unlock(&info->mutex);

	write(info->evt_fd, &count, 8);

	pthread_rwlock_unlock(&info->close_lock);
}

static void bs_curve_complete_one(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	struct scsi_cmd *cmd = iocb->cmd;
//	uint32_t length;
	int result = 0;

	switch (cmd->scb[0]) {
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
//		length = scsi_get_out_length(cmd);
		break;
	default:
//		length = scsi_get_in_length(cmd);
		break;
	}

	if (iocb->ctx.ret == 0)
		result = SAM_STAT_GOOD;
	else {
		sense_data_build(cmd, MEDIUM_ERROR, 0);
		result = SAM_STAT_CHECK_CONDITION;
	}

	dprintf("cmd: %p\n", cmd);
	target_cmd_io_done(cmd, result);

	free(iocb);
}

static void bs_curve_get_completions(int fd, int events, void *data)
{
	struct bs_curve_info *info = data;
	struct list_head temp_list;
	struct bs_curve_iocb *iocb;
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
		iocb = list_first_entry(&temp_list, bs_curve_iocb_t, bs_list);
		list_del(&iocb->bs_list);
		bs_curve_complete_one(info, iocb);
	}
}

static int bs_curve_io_prep_pread(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	iocb->ctx.offset = iocb->cmd->offset;
	iocb->ctx.length = scsi_get_in_length(iocb->cmd);
	iocb->ctx.buf = scsi_get_in_buffer(iocb->cmd);
	iocb->ctx.op = LIBAIO_OP_READ;
	iocb->ctx.cb = bs_curve_aio_callback;
	return 0;
}

static int bs_curve_io_prep_pwrite(struct bs_curve_info *info,
		bs_curve_iocb_t *iocb)
{
	iocb->ctx.offset = iocb->cmd->offset;
	iocb->ctx.length = scsi_get_out_length(iocb->cmd);
	iocb->ctx.buf = scsi_get_out_buffer(iocb->cmd);
	iocb->ctx.op = LIBAIO_OP_WRITE;
	iocb->ctx.cb = bs_curve_aio_callback;
	return 0;
}

static int bs_curve_cmd_submit(struct scsi_cmd *cmd)
{
	enum IOMode { IO_NOOP, IO_READ, IO_WRITE } mode = IO_NOOP;
	struct scsi_lu *lu = cmd->dev;
	struct bs_curve_info *info = BS_CURVE_I(lu);
	bs_curve_iocb_t *iocb = NULL;
	unsigned int scsi_op = (unsigned int)cmd->scb[0];
	int err = 0;

	switch (scsi_op) {
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		mode = IO_WRITE;
		break;
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		mode = IO_READ;
		break;

	case WRITE_SAME:
	case WRITE_SAME_16:
		eprintf("WRITE_SAME not yet supported for AIO backend.\n");
		{ int result, key, asc; 
                result = SAM_STAT_CHECK_CONDITION;
                key = HARDWARE_ERROR;
                asc = ASC_INTERNAL_TGT_FAILURE;       
		scsi_set_result(cmd, result);
		if (result != SAM_STAT_GOOD) {
//			eprintf("io error %p %x %d %d %" PRIu64 ", %m\n",
//				cmd, cmd->scb[0], ret, length, offset);
			sense_data_build(cmd, key, asc);
		}
		}
		return -1;
//		return -1;

	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
	default:
		dprintf("skipped cmd:%p op:%x\n", cmd, scsi_op);
		return 0;
	}

	iocb = calloc(1, sizeof(bs_curve_iocb_t));
	iocb->cmd = cmd;
	iocb->info = info;
	pthread_mutex_lock(&info->mutex);
	bs_curve_enq_inflight_io(info, iocb);
	pthread_mutex_unlock(&info->mutex);

	set_cmd_async(cmd);

	switch(mode) {
	case IO_READ:
		bs_curve_io_prep_pread(info, iocb);
		if (nebd_lib_aio_pread(info->curve_fd, &iocb->ctx)) {
			err = -1;
		}
		break;
	case IO_WRITE:
		bs_curve_io_prep_pwrite(info, iocb);
		if (nebd_lib_aio_pwrite(info->curve_fd, &iocb->ctx)) {
			err = -1;
		}
		break;
	default:
		err = -1;
		eprintf("%s:%d, Invalid mode %d\n", __FILE__, __LINE__, mode);
		abort();
	}

	if (err) {
		pthread_mutex_lock(&info->mutex);
		bs_curve_deq_inflight_io(info, iocb);
		pthread_mutex_unlock(&info->mutex);
		free(iocb);
	}

	return err;
}

static int bs_curve_open(struct scsi_lu *lu, char *path, int *fd, uint64_t *size)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);
	NebdOpenFlags openflags;
	int ret, afd;
	uint32_t blksize = 0;

	memset(&openflags, 0, sizeof(openflags));
	openflags.exclusive = 0;
	info->curve_fd = nebd_lib_open_with_flags(path, &openflags);
	if (info->curve_fd < 0) {
		eprintf("can not open curve volume %s, %s\n", path, strerror(errno));
		return -1;
	}
	blksize = 4096; //FIXME

	eprintf("opened curve volume %s for tgt:%d lun:%"PRId64 ", fd:%d\n",
		 path, info->lu->tgt->tid, info->lu->lun, info->curve_fd);

	*size = nebd_lib_filesize(info->curve_fd);

	afd = eventfd(0, O_NONBLOCK);
	if (afd < 0) {
		eprintf("failed to create eventfd for tgt:%d lun:%"PRId64 ", %m\n",
			info->lu->tgt->tid, info->lu->lun);
		ret = afd;
		goto close_curve;
	}
	dprintf("eventfd:%d for tgt:%d lun:%"PRId64 "\n",
		afd, info->lu->tgt->tid, info->lu->lun);

	ret = tgt_event_add(afd, EPOLLIN, bs_curve_get_completions, info);
	if (ret)
		goto close_eventfd;
	info->evt_fd = afd;

	if (!lu->attrs.no_auto_lbppbe)
		update_lbppbe(lu, blksize);

	return 0;

	tgt_event_del(afd);
close_eventfd:
	close(afd);
close_curve:
	nebd_lib_close(info->curve_fd);
	info->curve_fd = -1;
	return ret;
}

static void bs_curve_close(struct scsi_lu *lu)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);

	if (info->curve_fd >= 0) {
		nebd_lib_close(info->curve_fd);
		info->curve_fd = -1;
	}
}

static int
init_curve_nebd(void)
{
	static int nebd_inited;
	int ret = 0;

	if (nebd_inited)
		return 0;

	if (nebd_lib_init()) {
		eprintf("can not init nebd client, errno=%d\n", errno);
		ret = -1;
	} else {
		nebd_inited = 1;
	}
	return ret;
}

static tgtadm_err bs_curve_init(struct scsi_lu *lu, char *bsopts)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);

	if (init_curve_nebd())
		return TGTADM_UNKNOWN_ERR;

	memset(info, 0, sizeof(*info));
	pthread_mutex_init(&info->mutex, NULL);
	pthread_rwlock_init(&info->close_lock, NULL);
	info->curve_fd = -1;
	info->evt_fd = -1;
	INIT_LIST_HEAD(&info->cmd_inflight_list);
	INIT_LIST_HEAD(&info->cmd_complete_list);
	info->lu = lu;
	return TGTADM_SUCCESS;
}

static void bs_curve_exit(struct scsi_lu *lu)
{
	struct bs_curve_info *info = BS_CURVE_I(lu);

	pthread_rwlock_wrlock(&info->close_lock);
	close(info->evt_fd);
	pthread_rwlock_unlock(&info->close_lock);
	pthread_rwlock_destroy(&info->close_lock);
	pthread_mutex_destroy(&info->mutex);
}

static struct backingstore_template curve_bst = {
	.bs_name		= "curve",
	.bs_datasize    	= sizeof(struct bs_curve_info),
	.bs_init		= bs_curve_init,
	.bs_exit		= bs_curve_exit,
	.bs_open		= bs_curve_open,
	.bs_close       	= bs_curve_close,
	.bs_cmd_submit  	= bs_curve_cmd_submit,
};

void register_bs_module(void)
{
	register_backingstore_template(&curve_bst);
}
