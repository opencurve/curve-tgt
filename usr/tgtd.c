/*
 * SCSI target daemon
 *
 * Copyright (C) 2005-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2005-2007 Mike Christie <michaelc@cs.wisc.edu>
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
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/resource.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <poll.h>
#include <pthread.h>

#include "list.h"
#include "tgtd.h"
#include "driver.h"
#include "work.h"
#include "util.h"
#include "lib/tree.h"

unsigned long pagesize, pageshift;

int system_active = 1;
static char program_name[] = "tgtd";
struct tgt_evloop *main_evloop;

RB_PROTOTYPE(ev_list, event_data, e_node,);

static struct option const long_options[] = {
	{"foreground", no_argument, 0, 'f'},
	{"control-port", required_argument, 0, 'C'},
	{"nr_iothreads", required_argument, 0, 't'},
	{"debug", required_argument, 0, 'd'},
	{"version", no_argument, 0, 'V'},
	{"help", no_argument, 0, 'h'},
	{0, 0, 0, 0},
};

static char *short_options = "fC:d:t:Vh";
static char *spare_args;

static void usage(int status)
{
	if (status) {
		fprintf(stderr, "Try `%s --help' for more information.\n",
			program_name);
		exit(status);
	}

	printf("Linux SCSI Target framework daemon, version %s\n\n"
		"Usage: %s [OPTION]\n"
		"-f, --foreground        make the program run in the foreground\n"
		"-C, --control-port NNNN use port NNNN for the mgmt channel\n"
		"-t, --nr_iothreads NNNN specify the number of I/O threads\n"
		"-d, --debug debuglevel  print debugging information\n"
		"-V, --version           print version and exit\n"
		"-h, --help              display this help and exit\n",
		TGT_VERSION, program_name);
	exit(0);
}

static void bad_optarg(int ret, int ch, char *optarg)
{
	if (ret == ERANGE)
		fprintf(stderr, "-%c argument value '%s' out of range\n",
			ch, optarg);
	else
		fprintf(stderr, "-%c argument value '%s' invalid\n",
			ch, optarg);
	usage(ret);
}

static void version(void)
{
	printf("%s\n", TGT_VERSION);
	exit(0);
}

/* Default TGT mgmt port */
short int control_port;

static void signal_catch(int signo)
{
}

static int oom_adjust(void)
{
	int fd, err;
	const char *path, *score;
	struct stat st;

	/* Avoid oom-killer */
	path = "/proc/self/oom_score_adj";
	score = "-1000\n";

	if (stat(path, &st)) {
		/* oom_score_adj cannot be used, try oom_adj */
		path = "/proc/self/oom_adj";
		score = "-17\n";
	}

	fd = open(path, O_WRONLY);
	if (fd < 0) {
		fprintf(stderr, "can't adjust oom-killer's pardon %s, %m\n",
			path);
		return errno;
	}

	err = write(fd, score, strlen(score));
	if (err < 0) {
		fprintf(stderr, "can't adjust oom-killer's pardon %s, %m\n",
			path);
		close(fd);
		return errno;
	}
	close(fd);
	return 0;
}

static int nr_file_adjust(void)
{
	int ret, fd, max = 1024 * 1024;
	char path[] = "/proc/sys/fs/nr_open";
	char buf[64];
	struct rlimit rlim;

	/* Avoid oom-killer */
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "can't open %s, %m\n", path);
		goto set_rlimit;
	}
	ret = read(fd, buf, sizeof(buf));
	if (ret < 0) {
		fprintf(stderr, "can't read %s, %m\n", path);
		close(fd);
		return errno;
	}
	close(fd);
	max = atoi(buf);

set_rlimit:
	rlim.rlim_cur = rlim.rlim_max = max;

	ret = setrlimit(RLIMIT_NOFILE, &rlim);
	if (ret < 0)
		fprintf(stderr, "can't adjust nr_open %d %m\n", max);

	return 0;
}

static inline int event_cmp(struct event_data *a, struct event_data *b)
{
	if (a->fd < b->fd)
		return -1;
	else if (a->fd > b->fd)
		return 1;
	return 0;
}

RB_GENERATE2(ev_list, event_data, e_node, event_cmp, int, fd);

static int event_insert(struct ev_list *list, struct event_data *ev)
{
	struct event_data *tmp = ev_list_RB_INSERT(list, ev);
	if (tmp) {
		eprintf("event_insert match for fd %d", ev->fd);
	}
	return tmp == NULL;
}

int tgt_event_insert(struct tgt_evloop *evloop, int fd, int events, event_handler_t handler, void *data)
{
	struct epoll_event ev;
	struct event_data *tev;
	int err;

	tev = zalloc(sizeof(*tev));
	if (!tev)
		return -ENOMEM;

	tev->data = data;
	tev->handler = handler;
	tev->fd = fd;
	tev->events = events;

	memset(&ev, 0, sizeof(ev));
	ev.events = events;
	ev.data.ptr = tev;
	err = epoll_ctl(evloop->ep_fd, EPOLL_CTL_ADD, fd, &ev);
	if (err) {
		eprintf("Cannot add fd, %m\n");
		free(tev);
	} else
		event_insert(&evloop->root, tev);

	return err;
}

static struct event_data *tgt_event_lookup(struct tgt_evloop *evloop, int fd)
{
	return ev_list_RB_LOOKUP(&evloop->root, fd);
}

void tgt_event_delete(struct tgt_evloop *evloop, int fd)
{
	struct event_data *tev;
	int ret;

	tev = tgt_event_lookup(evloop, fd);
	if (!tev) {
		eprintf("Cannot find event %d\n", fd);
		abort();
		return;
	}

	ret = epoll_ctl(evloop->ep_fd, EPOLL_CTL_DEL, fd, NULL);
	if (ret < 0)
		eprintf("fail to remove epoll event, %s\n", strerror(errno));

	ev_list_RB_REMOVE(&evloop->root, tev);
	free(tev);

	evloop->event_need_refresh = 1;
}

int tgt_event_change(struct tgt_evloop *evloop, int fd, int events)
{
	struct epoll_event ev;
	struct event_data *tev;
	int ret;

	tev = tgt_event_lookup(evloop, fd);
	if (!tev) {
		eprintf("Cannot find event %d\n", fd);
		return -EINVAL;
	}

	memset(&ev, 0, sizeof(ev));
	ev.events = events;
	ev.data.ptr = tev;

	ret = epoll_ctl(evloop->ep_fd, EPOLL_CTL_MOD, fd, &ev);
	if (!ret)
		tev->events = events;
	return ret;
}

int tgt_event_migrate(int fd, struct tgt_evloop *old, struct tgt_evloop *new)
{
	struct event_data ev, *evp;

	evp = tgt_event_lookup(old, fd);
	if (evp == NULL)
		return -EINVAL;

	ev = *evp;
	tgt_event_delete(old, fd);

	return tgt_event_insert(new, fd, ev.events, ev.handler, ev.data);
}

int tgt_event_get(struct tgt_evloop *evloop, int fd)
{
	struct event_data *tev;

	tev = tgt_event_lookup(evloop, fd);
	if (!tev) {
		eprintf("Cannot find event %d\n", fd);
		return 0;
	}

	return tev->events;
}

void tgt_init_sched_event(struct event_data *evt,
			  sched_event_handler_t sched_handler, void *data)
{
	evt->sched_handler = sched_handler;
	evt->scheduled = 0;
	evt->data = data;
	INIT_LIST_HEAD(&evt->e_list);
}

void tgt_append_sched_event(struct tgt_evloop *evloop, struct event_data *evt)
{
	if (!evt->scheduled) {
		evt->scheduled = 1;
		int empty = list_empty(&evloop->sched_events_list);
		list_add_tail(&evt->e_list, &evloop->sched_events_list);
		if (empty)
			eventfd_write(evloop->async_fd, 1);
	}
}

/*
void tgt_add_sched_event(struct event_data *evt)
{
	tgt_append_sched_event(main_evloop, evt);
}
*/

void tgt_remove_sched_event(struct event_data *evt)
{
	if (evt->scheduled) {
		evt->scheduled = 0;
		list_del_init(&evt->e_list);
	}
}

/* strcpy, while eating multiple white spaces */
void str_spacecpy(char **dest, const char *src)
{
	const char *s = src;
	char *d = *dest;

	while (*s) {
		if (isspace(*s)) {
			if (!*(s+1))
				break;
			if (isspace(*(s+1))) {
				s++;
				continue;
			}
		}
		*d++ = *s++;
	}
	*d = '\0';
}

int call_program(const char *cmd, void (*callback)(void *data, int result),
		void *data, char *output, int op_len, int flags)
{
	pid_t pid;
	int fds[2], ret, i;
	char *pos, arg[256];
	char *argv[sizeof(arg) / 2];

	i = 0;
	pos = arg;
	str_spacecpy(&pos, cmd);
	if (strchr(cmd, ' ')) {
		while (*pos != '\0')
			argv[i++] = strsep(&pos, " ");
	} else
		argv[i++] = arg;
	argv[i] =  NULL;

	ret = pipe(fds);
	if (ret < 0) {
		eprintf("pipe create failed for %s, %m\n", cmd);
		return ret;
	}

	dprintf("%s, pipe: %d %d\n", cmd, fds[0], fds[1]);

	pid = fork();
	if (pid < 0) {
		eprintf("fork failed for: %s, %m\n", cmd);
		close(fds[0]);
		close(fds[1]);
		return pid;
	}

	if (!pid) {
		close(1);
		ret = dup(fds[1]);
		if (ret < 0) {
			eprintf("dup failed for: %s, %m\n", cmd);
			exit(-1);
		}
		close(fds[0]);
		execv(argv[0], argv);

		eprintf("execv failed for: %s, %m\n", cmd);
		exit(-1);
	} else {
		struct pollfd ev;
		int ret_sel;

		close(fds[1]);
		/* 0.1 second is okay, as the initiator will retry anyway */
		do {
			ev.fd = fds[0];
			ev.events = POLLIN;
			ev.revents = 0;
			ret_sel = poll(&ev, 1, 100);
		} while (ret_sel < 0 && errno == EINTR);
		if (ret_sel <= 0) { /* error or timeout */
			eprintf("timeout on redirect callback, terminating "
				"child pid %d\n", pid);
			kill(pid, SIGTERM);
		}
		do {
			ret = waitpid(pid, &i, 0);
		} while (ret < 0 && errno == EINTR);
		if (ret < 0) {
			eprintf("waitpid failed for: %s, %m\n", cmd);
			close(fds[0]);
			return ret;
		}
		if (ret_sel > 0) {
			ret = read(fds[0], output, op_len);
			if (ret < 0) {
				eprintf("failed to get output from: %s\n", cmd);
				close(fds[0]);
				return ret;
			}
		}

		if (callback)
			callback(data, WEXITSTATUS(i));
		close(fds[0]);
	}

	return 0;
}

static int tgt_exec_scheduled(struct tgt_evloop *evloop)
{
	struct list_head *last_sched;
	struct event_data *tev, *tevn;
	int work_remains = 0;

	if (!list_empty(&evloop->sched_events_list)) {
		/* execute only work scheduled till now */
		last_sched = evloop->sched_events_list.prev;
		list_for_each_entry_safe(tev, tevn, &evloop->sched_events_list,
					 e_list) {
			tgt_remove_sched_event(tev);
			tev->sched_handler(tev);
			if (&tev->e_list == last_sched)
				break;
		}
		if (!list_empty(&evloop->sched_events_list))
			work_remains = 1;
	}
	return work_remains;
}

void tgt_event_loop(struct tgt_evloop *evloop)
{
	int nevent, i, sched_remains, timeout;
	struct epoll_event events[1024];
	struct event_data *tev;

retry:
	sched_remains = tgt_exec_scheduled(evloop);
	timeout = sched_remains ? 0 : -1;

	if (evloop->release)
		evloop->release(evloop);
	nevent = epoll_wait(evloop->ep_fd, events, ARRAY_SIZE(events), timeout);
	if (evloop->acquire)
		evloop->acquire(evloop);
	if (evloop->event_need_refresh) {
		evloop->event_need_refresh = 0;
		goto next;
	}

	if (nevent < 0) {
		if (errno != EINTR) {
			eprintf("%m\n");
			exit(1);
		}
	} else if (nevent) {
		for (i = 0; i < nevent; i++) {
			tev = (struct event_data *) events[i].data.ptr;
			if (tev != &evloop->async_event)
				tev->handler(evloop, tev->fd, events[i].events, tev->data);
			else {
				printf("async event\n");
				eventfd_t value;
				eventfd_read(evloop->async_fd, &value);
			}

			if (evloop->event_need_refresh) {
				evloop->event_need_refresh = 0;
				goto next;
			}
		}
	}

next:
	if (!evloop->stop && system_active)
		goto retry;
	eprintf("exit\n");
}

void tgt_event_stop(struct tgt_evloop *evloop)
{
	evloop->stop = 1;
	eventfd_write(evloop->async_fd, 1);
}

void tgt_event_kick(struct tgt_evloop *evloop)
{
	eventfd_write(evloop->async_fd, 1);
}

int lld_init_one(int lld_index)
{
	int err;

	INIT_LIST_HEAD(&tgt_drivers[lld_index]->target_list);
	if (tgt_drivers[lld_index]->init) {
		err = tgt_drivers[lld_index]->init(lld_index, spare_args);
		if (err) {
			tgt_drivers[lld_index]->drv_state = DRIVER_ERR;
			return err;
		}
		tgt_drivers[lld_index]->drv_state = DRIVER_INIT;
	}
	return 0;
}

static int lld_init(void)
{
	int i, nr;

	for (i = nr = 0; tgt_drivers[i]; i++) {
		if (!lld_init_one(i))
			nr++;
	}
	return nr;
}

static void lld_exit(void)
{
	int i;

	for (i = 0; tgt_drivers[i]; i++) {
		if (tgt_drivers[i]->exit)
			tgt_drivers[i]->exit();
		tgt_drivers[i]->drv_state = DRIVER_EXIT;
	}
}

static int lld_init_evloop_one(int lld_index, struct tgt_evloop *evloop)
{
	int err;

	if (tgt_drivers[lld_index]->init_evloop) {
		err = tgt_drivers[lld_index]->init_evloop(evloop);
		if (err) {
			//tgt_drivers[lld_index]->drv_state = DRIVER_ERR;
			//return err;
		}
	}
	return 0;
}

int lld_init_evloop(struct tgt_evloop *evloop)
{
	int i, nr;

	for (i = nr = 0; tgt_drivers[i]; i++) {
		if (!lld_init_evloop_one(i, evloop))
			nr++;
	}
	return nr;
}

void lld_fini_evloop(struct tgt_evloop *evloop)
{
	int i;

	for (i = 0; tgt_drivers[i]; i++) {
		if (tgt_drivers[i]->fini_evloop)
			tgt_drivers[i]->fini_evloop(evloop);
	}
}

struct tgt_param {
	int (*parse_func)(char *);
	char *name;
};

static struct tgt_param params[64];

int setup_param(char *name, int (*parser)(char *))
{
	int i;

	for (i = 0; i < ARRAY_SIZE(params); i++)
		if (!params[i].name)
			break;

	if (i < ARRAY_SIZE(params)) {
		params[i].name = name;
		params[i].parse_func = parser;

		return 0;
	} else
		return -1;
}

static int parse_params(char *name, char *p)
{
	int i;

	for (i = 0; i < ARRAY_SIZE(params) && params[i].name; i++) {
		if (!strcmp(name, params[i].name))
			return params[i].parse_func(p);
	}

	fprintf(stderr, "'%s' is an unknown option\n", name);

	return -1;
}

struct tgt_evloop *tgt_new_evloop(void)
{
	struct epoll_event ev;
	struct tgt_evloop *evloop;
	int err;

	evloop = calloc(1, sizeof(*evloop));
	if (evloop == NULL) {
		fprintf(stderr, "can't malloc evloop, %m\n");
		return NULL;
	}

	RB_INIT(&evloop->root);
	INIT_LIST_HEAD(&evloop->sched_events_list);
	evloop->ep_fd = epoll_create(4096);
	if (evloop->ep_fd < 0) {
		fprintf(stderr, "can't create epoll fd, %m\n");
		free(evloop);
		return NULL;
	}
	evloop->async_fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
	if (evloop->async_fd < 0) {
		fprintf(stderr, "can't create async ev fd, %m\n");
		close(evloop->async_fd);
		goto close_ep_fd;
	}

	memset(&ev, 0, sizeof(ev));
	ev.events   = EPOLLIN;
	ev.data.ptr = &evloop->async_event;
	err = epoll_ctl(evloop->ep_fd, EPOLL_CTL_ADD, evloop->async_fd, &ev);
	if (err) {
		fprintf(stderr, "can't add async ev fd, %m\n");
		goto close_async_fd;
	}

	return evloop;

close_async_fd:
	close(evloop->async_fd);
close_ep_fd:
	close(evloop->ep_fd);
	free(evloop);
	return NULL;
}

void tgt_destroy_evloop(struct tgt_evloop *evloop)
{
	int err;

	err = epoll_ctl(evloop->ep_fd, EPOLL_CTL_DEL, evloop->async_fd, NULL);
	if (err) {
		fprintf(stderr, "can not EPOLL_CTL_DEL async fd");
	}
	close(evloop->ep_fd);
	close(evloop->async_fd);
	if (!RB_EMPTY(&evloop->root))
		fprintf(stderr, "destroy evloop with non-empty evlist\n");
	free(evloop);
}

void tgt_event_set_loop_release_cb(struct tgt_evloop *evloop,
        void (*release)(struct tgt_evloop *), void (*acquire)(struct tgt_evloop *))
{
	evloop->acquire = acquire;
	evloop->release = release;
}

void *tgt_event_userdata(struct tgt_evloop *evloop, int idx)
{
	if ((unsigned)idx >= sizeof(evloop->userdata) / sizeof(evloop->userdata[0])) {
		eprintf("out of uesrdata range\n");
		abort();
	}
	return evloop->userdata[idx];
}

void *tgt_event_set_userdata(struct tgt_evloop *evloop, int idx, void *data)
{
	if ((unsigned)idx >= sizeof(evloop->userdata) / sizeof(evloop->userdata[0])) {
		eprintf("out of uesrdata range\n");
		abort();
	}
	return evloop->userdata[idx] = data;
}

int main(int argc, char **argv)
{
	struct sigaction sa_old;
	struct sigaction sa_new;
	int err, ch, longindex, nr_lld = 0;
	int is_daemon = 1, is_debug = 0;
	int ret;

	sa_new.sa_handler = signal_catch;
	sigemptyset(&sa_new.sa_mask);
	sa_new.sa_flags = 0;
	sigaction(SIGPIPE, &sa_new, &sa_old);
	sigaction(SIGTERM, &sa_new, &sa_old);

	pagesize = sysconf(_SC_PAGESIZE);
	for (pageshift = 0;; pageshift++)
		if (1UL << pageshift == pagesize)
			break;

	opterr = 0;

	while ((ch = getopt_long(argc, argv, short_options, long_options,
				 &longindex)) >= 0) {
		switch (ch) {
		case 'f':
			is_daemon = 0;
			break;
		case 'C':
			ret = str_to_int_ge(optarg, control_port, 0);
			if (ret)
				bad_optarg(ret, ch, optarg);
			break;
		case 't':
			ret = str_to_int_gt(optarg, nr_iothreads, 0);
			if (ret)
				bad_optarg(ret, ch, optarg);
			break;
		case 'd':
			ret = str_to_int_range(optarg, is_debug, 0, 1);
			if (ret)
				bad_optarg(ret, ch, optarg);
			break;
		case 'V':
			version();
			break;
		case 'h':
			usage(0);
			break;
		default:
			if (strncmp(argv[optind - 1], "--", 2))
				usage(1);

			ret = parse_params(argv[optind - 1] + 2, argv[optind]);
			if (ret)
				usage(1);

			break;
		}
	}

	spare_args = optind < argc ? argv[optind] : NULL;

	if (is_daemon && daemon(0, 0))
		exit(1);

	err = log_init(program_name, LOG_SPACE_SIZE, is_daemon, is_debug);
	if (err)
		exit(1);

	main_evloop = tgt_new_evloop();
	if (main_evloop == NULL)
		exit(1);

	err = work_timer_start(main_evloop);
	if (err)
		exit(1);

	err = ipc_init();
	if (err)
		exit(1);

	nr_lld = lld_init();
	if (!nr_lld) {
		fprintf(stderr, "No available low level driver!\n");
		exit(1);
	}

	lld_init_evloop(main_evloop);

	err = oom_adjust();
	if (err)
		exit(1);

	err = nr_file_adjust();
	if (err)
		exit(1);

	bs_init();

#ifdef USE_SYSTEMD
	sd_notify(0, "READY=1\nSTATUS=Starting event loop...");
#endif

	tgt_event_loop(main_evloop);

	lld_fini_evloop(main_evloop);

	lld_exit();

	ipc_exit();

	work_timer_stop(main_evloop);

	log_close();

	return 0;
}
