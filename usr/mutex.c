#include "mutex.h"
#include "log.h"
#include <errno.h>
#include <stdlib.h>

void mutex_init(tgt_mutex_t *m)
{
	pthread_mutex_init(&m->raw_mutex, NULL);
	m->owner = 0;
}

void mutex_destroy(tgt_mutex_t *m)
{
	if (m->owner != 0) {
		eprintf("mutex_destroy ower exists\n");
	}
	pthread_mutex_destroy(&m->raw_mutex);
}

void mutex_lock(tgt_mutex_t *m)
{
	int err = pthread_mutex_lock(&m->raw_mutex);
	if (err) {
		errno = err;
		eprintf("mutex lock failed %m\n");
		abort();
	}

	m->owner = pthread_self();
}

void mutex_unlock(tgt_mutex_t *m)
{
	if (m->owner != pthread_self()){
		eprintf("mutex unlock, mutex not owned\n");
		abort();
	}

	m->owner = 0;
	int err = pthread_mutex_unlock(&m->raw_mutex);
	if (err) {
		errno = err;
		eprintf("pthread_mutex_unlock failed %m\n");
		abort();
	}
}

int mutex_is_locked(tgt_mutex_t *m)
{
	return pthread_self() == m->owner;
}

void mutex_assert_locked(tgt_mutex_t *m)
{
	if (m->owner != pthread_self()){
		eprintf("mutex not owned\n");
		abort();
	}
}

void mutex_assert_unlocked(tgt_mutex_t *m)
{
	if (m->owner == pthread_self()){
		eprintf("mutex owned\n");
		abort();
	}
}

