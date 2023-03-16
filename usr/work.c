/*
 * work scheduler, loosely timer-based
 *
 * Copyright (C) 2006-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2006-2007 Mike Christie <michaelc@cs.wisc.edu>
 * Copyright (C) 2011 Alexander Nezhinsky <alexandern@voltaire.com>
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
#include <stdlib.h>
#include <stdint.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/time.h>

#include "list.h"
#include "util.h"
#include "log.h"
#include "work.h"
#include "tgtd.h"

#define WORK_TIMER_INT_MSEC     500LL
#define WORK_TIMER_INT_USEC     (WORK_TIMER_INT_MSEC * 1000)
#define WORK_TIMER_INT_NSEC     (WORK_TIMER_INT_USEC * 1000)

struct work_timer {
	unsigned long elapsed_msecs;
	int timer_fd;
	int pipe_fd[2];
	int first;
	int timer_pending;
	struct list_head active_work_list;
	struct list_head inactive_work_list;
};

static void execute_work(struct work_timer *);

static inline unsigned int timeval_to_msecs(struct timeval t)
{
	return t.tv_sec * 1000 + t.tv_usec / 1000;
}

static void work_timer_schedule_evt(struct work_timer *wt)
{
	unsigned int n = 0;
	int err;

	if (wt->timer_pending)
		return;

	wt->timer_pending = 1;

	err = write(wt->pipe_fd[1], &n, sizeof(n));
	if (err < 0)
		eprintf("Failed to write to pipe, %m\n");
}

static void work_timer_evt_handler(struct tgt_evloop *evloop, int fd, int events, void *data)
{
	struct work_timer *wt = data;
	struct timeval cur_time;
	unsigned long long s;
	int err;

	err = read(wt->timer_fd, &s, sizeof(s));
	if (err < 0) {
		if (err != -EAGAIN)
			eprintf("failed to read from timerfd, %m\n");
		return;
	}

	if (wt->first) {
		wt->first = 0;
		err = gettimeofday(&cur_time, NULL);
		if (err) {
			eprintf("gettimeofday failed, %m\n");
			exit(1);
		}
		wt->elapsed_msecs = timeval_to_msecs(cur_time);
		return;
	}

	wt->elapsed_msecs += (unsigned int)s * WORK_TIMER_INT_MSEC;
	wt->timer_pending = 0;
	execute_work(wt);
}

int work_timer_start(struct tgt_evloop *evloop)
{
	struct work_timer *wt = NULL;
	struct timeval t;
	int err;

	if (tgt_event_userdata(evloop, EV_DATA_WORK_TIMER))
		return -1;

	err = gettimeofday(&t, NULL);
	if (err) {
		eprintf("gettimeofday failed, %m\n");
		exit(1);
	}

	wt = zalloc(sizeof(*wt));

	INIT_LIST_HEAD(&wt->active_work_list);
	INIT_LIST_HEAD(&wt->inactive_work_list);

	wt->first = 1;
	wt->elapsed_msecs = timeval_to_msecs(t);
	wt->timer_fd = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK|TFD_CLOEXEC);
	if (wt->timer_fd < 0) {
		eprintf("can not create timerfd, %m\n");
		free(wt);
		return -1;
	}
	err = pipe(wt->pipe_fd);
	if (err) {
		eprintf("can not create pipe, %m\n");
		close(wt->timer_fd);
		free(wt);
		return -1;
	}

 	struct itimerspec its;

	its.it_interval.tv_sec = 0;
	its.it_interval.tv_nsec = WORK_TIMER_INT_NSEC;
	its.it_value.tv_sec = 0;
	its.it_value.tv_nsec = WORK_TIMER_INT_NSEC;

	err = timerfd_settime(wt->timer_fd, 0, &its, NULL);
	if (err) {
		eprintf("timerfd_settime failed, %m\n");
		close(wt->timer_fd);
		close(wt->pipe_fd[0]);
		close(wt->pipe_fd[1]);
		free(wt);
		return err;
	}

	err = tgt_event_insert(evloop, wt->timer_fd, EPOLLIN, work_timer_evt_handler, wt);
	if (err) {
		eprintf("failed to add timer event, fd:%d\n", wt->timer_fd);
		close(wt->timer_fd);
		close(wt->pipe_fd[0]);
		close(wt->pipe_fd[1]);
		free(wt);
		return err;
	}

#if 0
	err = tgt_event_insert(evloop, wt->pipe_fd[0], EPOLLIN, work_timer_evt_handler, wt);
	if (err) {
		eprintf("failed to add timer pipe fd:%d\n", wt->pipe_fd[0]);
		tgt_event_delete(evloop, wt->timer_fd);
		close(wt->timer_fd);
		close(wt->pipe_fd[0]);
		close(wt->pipe_fd[1]);
		free(wt);
		return err;
	}
#endif

	tgt_event_set_userdata(evloop, EV_DATA_WORK_TIMER, wt);

	return 0;
}

void work_timer_stop(struct tgt_evloop *evloop)
{
	struct work_timer *wt = tgt_event_userdata(evloop, EV_DATA_WORK_TIMER);

	if (!wt)
		return;

	wt->elapsed_msecs = 0;

	tgt_event_delete(evloop, wt->timer_fd);
	close(wt->timer_fd);

	tgt_event_delete(evloop, wt->pipe_fd[0]);
	close(wt->pipe_fd[0]);
	close(wt->pipe_fd[1]);

	tgt_event_set_userdata(evloop, EV_DATA_WORK_TIMER, NULL);
	free(wt);
}

void ev_add_work(struct tgt_evloop *evloop, struct tgt_work *work, unsigned int second)
{
	struct work_timer *wt = tgt_event_userdata(evloop, EV_DATA_WORK_TIMER);
	struct tgt_work *ent;
	struct timeval t;
	int err;

	if (second) {
		err = gettimeofday(&t, NULL);
		if (err) {
			eprintf("gettimeofday failed, %m\n");
			exit(1);
		}
		work->when = timeval_to_msecs(t) + second * 1000;

		list_for_each_entry(ent, &wt->inactive_work_list, entry) {
			if (before(work->when, ent->when))
				break;
		}

		list_add_tail(&work->entry, &ent->entry);
	} else {
		list_add_tail(&work->entry, &wt->active_work_list);
		work_timer_schedule_evt(wt);
	}
	work->evloop = evloop;
}

void ev_del_work(struct tgt_evloop *evloop, struct tgt_work *work)
{
	if (evloop != work->evloop) {
		eprintf("delete from wrong evloop\n");
	}
	work->evloop = NULL;
	list_del_init(&work->entry);
}

void add_work(struct tgt_work *work, unsigned int second)
{
	ev_add_work(main_evloop, work, second);
}

void del_work(struct tgt_work *work)
{
	ev_del_work(main_evloop, work);
}

static void execute_work(struct work_timer *wt)
{
	struct tgt_work *work, *n;

	list_for_each_entry_safe(work, n, &wt->inactive_work_list, entry) {
		if (before(wt->elapsed_msecs, work->when))
			break;

		list_del(&work->entry);
		list_add_tail(&work->entry, &wt->active_work_list);
	}

	while (!list_empty(&wt->active_work_list)) {
		work = list_first_entry(&wt->active_work_list,
					struct tgt_work, entry);
		list_del_init(&work->entry);
		work->func(work);
	}
}
