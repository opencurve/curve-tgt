#ifndef __TARGET_H__
#define __TARGET_H__

#include <limits.h>
#include <pthread.h>

#include "mutex.h"

struct acl_entry {
	char *address;
	struct list_head aclent_list;
};

struct iqn_acl_entry {
	char *name;
	struct list_head iqn_aclent_list;
};

struct tgt_account {
	int out_aid;
	int nr_inaccount;
	int max_inaccount;
	int *in_aids;
};

struct target {
	char *name;

	int tid;
	int lid;

	enum scsi_target_state target_state;

	struct list_head target_siblings;

	struct list_head device_list;

	struct list_head it_nexus_list;

	struct backingstore_template *bst;

	struct list_head acl_list;

	struct list_head iqn_acl_list;

	struct tgt_account account;

	struct list_head lld_siblings;

	struct tgt_evloop *evloop;

	pthread_t ev_td;

	tgt_mutex_t mutex;
};

struct it_nexus {
	uint64_t itn_id;
	long ctime;

	struct list_head cmd_list;

	struct target *nexus_target;

	/* the list of i_t_nexus belonging to a target */
	struct list_head nexus_siblings;

	/* dirty hack for IBMVIO */
	int host_no;

	struct list_head itn_itl_info_list;

	/* only used for show operation */
	char *info;
};

enum {
	TGT_QUEUE_BLOCKED,
	TGT_QUEUE_DELETED,
};

#define QUEUE_FNS(bit, name)						\
static inline void set_queue_##name(struct tgt_cmd_queue *q)		\
{									\
	(q)->state |= (1UL << TGT_QUEUE_##bit);				\
}									\
static inline void clear_queue_##name(struct tgt_cmd_queue *q)		\
{									\
	(q)->state &= ~(1UL << TGT_QUEUE_##bit);			\
}									\
static inline int queue_##name(const struct tgt_cmd_queue *q)		\
{									\
	return ((q)->state & (1UL << TGT_QUEUE_##bit));			\
}

QUEUE_FNS(BLOCKED, blocked)
QUEUE_FNS(DELETED, deleted)

static inline int queue_active(const struct tgt_cmd_queue *q)
{
	return ((q)->active_cmd);
}

static inline void target_lock(struct target *t)
{
	mutex_lock(&t->mutex);
}

static inline void target_unlock(struct target *t)
{
	mutex_unlock(&t->mutex);
}

static inline int target_is_locked(struct target *t)
{
	return mutex_is_locked(&t->mutex);
}

static inline void target_assert_locked(struct target *t)
{
	mutex_assert_locked(&t->mutex);
}

static inline void target_assert_unlocked(struct target *t)
{
	mutex_assert_unlocked(&t->mutex);
}

struct target *target_lookup(int tid);

void target_list_rdlock(void);
void target_list_wrlock(void);
void target_list_unlock(void);

#endif
