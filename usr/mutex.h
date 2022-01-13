#pragma once

#include <pthread.h>

struct tgt_mutex {
	pthread_mutex_t raw_mutex;
	pthread_t owner;
};

typedef struct tgt_mutex tgt_mutex_t;

void mutex_init(tgt_mutex_t *m);
void mutex_destroy(tgt_mutex_t *m);
void mutex_lock(tgt_mutex_t *m);
void mutex_unlock(tgt_mutex_t *m);
int  mutex_is_locked(tgt_mutex_t *m);
void mutex_assert_locked(tgt_mutex_t *m);
void mutex_assert_unlocked(tgt_mutex_t *m);

