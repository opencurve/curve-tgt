#ifndef __WORK_H
#define __WORK_H

#include <sys/timerfd.h>

struct tgt_work {
	struct list_head entry;
	void (*func)(struct tgt_work *);
	void *data;
	unsigned int when;
	struct tgt_evloop *evloop;
};

struct tgt_evloop;

extern int work_timer_start(struct tgt_evloop *evloop);
extern void work_timer_stop(struct tgt_evloop *evloop);

extern void ev_add_work(struct tgt_evloop *evloop, struct tgt_work *work, unsigned int second);
extern void ev_del_work(struct tgt_evloop *evloop, struct tgt_work *work);
extern void add_work(struct tgt_work *work, unsigned int second);
extern void del_work(struct tgt_work *work);

#endif
