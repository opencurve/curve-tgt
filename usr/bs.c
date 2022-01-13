/*
 * backing store routine
 *
 * Copyright (C) 2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2007 Mike Christie <michaelc@cs.wisc.edu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, version 2 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
#include <dirent.h>
#include <dlfcn.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <syscall.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <linux/types.h>
#include <unistd.h>

#include "list.h"
#include "tgtd.h"
#include "tgtadm_error.h"
#include "util.h"
#include "bs_thread.h"
#include "scsi.h"

LIST_HEAD(bst_list);

/* used by both bs_rdwr.c and bs_rbd.c */
int nr_iothreads = 16;

void bs_create_opcode_map(struct backingstore_template *bst,
			  unsigned char *opcodes, int num)
{
	int i;

	for (i = 0; i < num; i++)
		set_bit(opcodes[i], bst->bs_supported_ops);
}

int is_bs_support_opcode(struct backingstore_template *bst, int op)
{
	/*
	 * assumes that this bs doesn't support supported_ops yet so
	 * returns success for the compatibility.
	 */
	if (!test_bit(TEST_UNIT_READY, bst->bs_supported_ops))
		return 1;

	return test_bit(op, bst->bs_supported_ops);
}

int register_backingstore_template(struct backingstore_template *bst)
{
	list_add(&bst->backingstore_siblings, &bst_list);

	return 0;
}

struct backingstore_template *get_backingstore_template(const char *name)
{
	struct backingstore_template *bst;

	list_for_each_entry(bst, &bst_list, backingstore_siblings) {
		if (!strcmp(name, bst->bs_name))
			return bst;
	}
	return NULL;
}

static void bs_thread_request_done(struct tgt_evloop *evloop, int fd, int events, void *data)
{
	struct bs_thread_info *info = data;
	struct scsi_cmd *cmd;
	struct list_head tmp_list;
	eventfd_t value;
	int ret;

	INIT_LIST_HEAD(&tmp_list);
	ret = eventfd_read(info->sig_fd, &value);
	if (ret < 0) {
		if (errno != EAGAIN) {
			eprintf("error read eventfd, %m\n");
			abort();
		}
	}

	pthread_mutex_lock(&info->finished_lock);
	list_splice_init(&info->finished_list, &tmp_list);
	pthread_mutex_unlock(&info->finished_lock);

	while (!list_empty(&tmp_list)) {
		cmd = list_first_entry(&tmp_list,
				       struct scsi_cmd, bs_list);

		dprintf("back to tgtd, %p\n", cmd);

		list_del(&cmd->bs_list);
		target_cmd_io_done(cmd, scsi_get_result(cmd));
	}
}

/* Unlock mutex even if thread is cancelled */
static void mutex_cleanup(void *mutex)
{
	pthread_mutex_unlock(mutex);
}

static void *bs_thread_worker_fn(void *arg)
{
	struct bs_thread_info *info = arg;
	struct scsi_cmd *cmd;
	int empty;

	while (1) {
		pthread_mutex_lock(&info->pending_lock);
		pthread_cleanup_push(mutex_cleanup, &info->pending_lock);

		while (list_empty(&info->pending_list))
			pthread_cond_wait(&info->pending_cond,
					  &info->pending_lock);

		cmd = list_first_entry(&info->pending_list,
				       struct scsi_cmd, bs_list);

		list_del(&cmd->bs_list);
		pthread_cleanup_pop(1); /* Unlock pending_lock mutex */

		info->request_fn(cmd);

		pthread_mutex_lock(&info->finished_lock);
		empty = list_empty(&info->finished_list);
		list_add_tail(&cmd->bs_list, &info->finished_list);
		pthread_mutex_unlock(&info->finished_lock);

		if (empty &&eventfd_write(info->sig_fd, 1) == -1 &&errno != EAGAIN) {
			eprintf("failed to write to sig_fd: %s\n", strerror(errno));
			abort();
		}
	}

	pthread_exit(NULL);
}

static int bs_init_modules(void)
{
	int ret;
	DIR *dir;

	dir = opendir(BSDIR);
	if (dir == NULL) {
		eprintf("could not open backing-store module directory %s\n",
			BSDIR);
	} else {
		struct dirent *dirent;
		void *handle;
		while ((dirent = readdir(dir))) {
			char *soname;
			void (*register_bs_module)(void);

			if (dirent->d_name[0] == '.') {
				continue;
			}

			ret = asprintf(&soname, "%s/%s", BSDIR,
					dirent->d_name);
			if (ret == -1) {
				eprintf("out of memory\n");
				continue;
			}
			handle = dlopen(soname, RTLD_NOW|RTLD_LOCAL);
			if (handle == NULL) {
				eprintf("failed to dlopen backing-store "
					"module %s error %s \n",
					soname, dlerror());
				free(soname);
				continue;
			}
			register_bs_module = dlsym(handle, "register_bs_module");
			if (register_bs_module == NULL) {
				eprintf("could not find register_bs_module "
					"symbol in module %s\n",
					soname);
				free(soname);
				continue;
			}
			register_bs_module();
			free(soname);
		}
		closedir(dir);
	}

	return 0;
}

int bs_init(void)
{
	return bs_init_modules();
}

tgtadm_err bs_thread_open(struct tgt_evloop *evloop, struct bs_thread_info *info, request_func_t *rfn,
			  int nr_threads)
{
	int i, ret;

	info->worker_thread = zalloc(sizeof(pthread_t) * nr_threads);
	if (!info->worker_thread)
		return TGTADM_NOMEM;

	info->sig_fd = eventfd(0, O_NONBLOCK);
	if (info->sig_fd < 0)
		return TGTADM_UNKNOWN_ERR;
	info->evloop = evloop;

	eprintf("%d\n", nr_threads);
	info->request_fn = rfn;
	INIT_LIST_HEAD(&info->pending_list);
	INIT_LIST_HEAD(&info->finished_list);

	pthread_cond_init(&info->pending_cond, NULL);
	pthread_mutex_init(&info->pending_lock, NULL);
	pthread_mutex_init(&info->finished_lock, NULL);

	for (i = 0; i < nr_threads; i++) {
		ret = pthread_create(&info->worker_thread[i], NULL,
				     bs_thread_worker_fn, info);

		if (ret) {
			eprintf("failed to create a worker thread, %d %s\n",
				i, strerror(ret));
			if (ret)
				goto destroy_threads;
		}
	}
	info->nr_worker_threads = nr_threads;
	ret = tgt_event_insert(evloop, info->sig_fd, EPOLLIN, bs_thread_request_done, info);
	if (ret) {
		goto destroy_threads;
	}

	return TGTADM_SUCCESS;
destroy_threads:

	for (; i > 0; i--) {
		if (info->worker_thread[i - 1]) {
			pthread_cancel(info->worker_thread[i - 1]);
			pthread_join(info->worker_thread[i - 1], NULL);
			eprintf("stopped the worker thread %d\n", i - 1);
		}
	}

	pthread_cond_destroy(&info->pending_cond);
	pthread_mutex_destroy(&info->pending_lock);
	pthread_mutex_destroy(&info->finished_lock);
	free(info->worker_thread);
	info->worker_thread = NULL;
	close(info->sig_fd);
	info->sig_fd = -1;
	info->evloop = NULL;
	return TGTADM_NOMEM;
}

void bs_thread_close(struct bs_thread_info *info)
{
	int i;

	for (i = 0; i < info->nr_worker_threads && info->worker_thread[i]; i++) {
		pthread_cancel(info->worker_thread[i]);
		pthread_join(info->worker_thread[i], NULL);
	}

	pthread_cond_destroy(&info->pending_cond);
	pthread_mutex_destroy(&info->pending_lock);
	pthread_mutex_destroy(&info->finished_lock);
	free(info->worker_thread);
	info->worker_thread = NULL;
	close(info->sig_fd);
	tgt_event_delete(info->evloop, info->sig_fd);
	info->sig_fd = -1;
	info->evloop = NULL;
}

int bs_thread_cmd_submit(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu = cmd->dev;
	struct bs_thread_info *info = BS_THREAD_I(lu);

	set_cmd_async(cmd);

	pthread_mutex_lock(&info->pending_lock);

	list_add_tail(&cmd->bs_list, &info->pending_list);

	pthread_mutex_unlock(&info->pending_lock);

	pthread_cond_signal(&info->pending_cond);

	return 0;
}

