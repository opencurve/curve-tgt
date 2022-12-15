/*
 * Software iSCSI target over TCP/IP Data-Path
 *
 * Copyright (C) 2006-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2006-2007 Mike Christie <michaelc@cs.wisc.edu>
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
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <linux/errqueue.h>

#include "iscsid.h"
#include "tgtd.h"
#include "util.h"
#include "work.h"
#include "target.h"

/* The value is advised by linux kernel document msg_zerocopy.html */
#define LINUX_ZEROCOPY_THRESHOLD (1024 * 10)

static int g_enable_zerocopy = 1;
static int listen_fds[128];
static struct iscsi_transport iscsi_tcp;
static LIST_HEAD(g_conn_gc_list);
static pthread_mutex_t g_conn_gc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_gc_thread;
static int g_gc_efd = -1;

struct zerocopy_info {
	struct list_head link;
	char *buf;
	size_t len;
	uint64_t sn;
	int free;
};

struct iscsi_tcp_connection {
	int fd;

	struct list_head tcp_conn_siblings;
	int nop_inflight_count;
	int nop_interval;
	int nop_tick;
	int nop_count;
	long ttt;

	struct zerocopy {
		int enable;
		unsigned next_completion;
		uint64_t completions;
		uint64_t expected_completions;
	} zerocopy;

	struct iscsi_connection iscsi_conn;
	struct list_head zc_buf_out_list;
	struct list_head zc_buf_defer_free_list;
	struct list_head zc_info_free_list;
	struct timeval stamp;
};

static void iscsi_tcp_event_handler(struct tgt_evloop *evloop, int fd, int events, void *data);
static void iscsi_tcp_release(struct iscsi_connection *conn);
static struct iscsi_task *iscsi_tcp_alloc_task(struct iscsi_connection *conn,
						size_t ext_len);
static void iscsi_tcp_free_task(struct iscsi_task *task);
static void iscsi_tcp_retire_bufs(struct iscsi_tcp_connection *conn);
static int iscsi_tcp_show(struct iscsi_connection *conn, char *buf, int rest);

static struct zerocopy_info *alloc_zerocopy_info(
	struct iscsi_tcp_connection *conn)
{
	struct zerocopy_info *pos;

	if (!list_empty(&conn->zc_info_free_list)) {
		pos = list_first_entry(&conn->zc_info_free_list,
			struct zerocopy_info, link);
		list_del(&pos->link);
		return pos;
	}
	return malloc(sizeof(*pos));
}

static void release_zerocopy_info(struct iscsi_tcp_connection *conn,
	struct zerocopy_info *pos)
{
	list_add(&pos->link, &conn->zc_info_free_list);
}

static void free_zerocopy_info(struct iscsi_tcp_connection *conn)
{
	struct zerocopy_info *pos;

	while (!list_empty(&conn->zc_info_free_list)) {
		pos = list_first_entry(&conn->zc_info_free_list,
			struct zerocopy_info, link);
                list_del(&pos->link);
		free(pos);
	}
	while (!list_empty(&conn->zc_buf_out_list)) {
		pos = list_first_entry(&conn->zc_buf_out_list,
			struct zerocopy_info, link);
		free(pos->buf);
		list_del(&pos->link);
		free(pos);
	}
	while (!list_empty(&conn->zc_buf_defer_free_list)) {
		pos = list_first_entry(&conn->zc_buf_defer_free_list,
			struct zerocopy_info, link);
		free(pos->buf);
		list_del(&pos->link);
		free(pos);
	}
}

static inline struct iscsi_tcp_connection *TCP_CONN(struct iscsi_connection *conn)
{
	return container_of(conn, struct iscsi_tcp_connection, iscsi_conn);
}

struct iscsi_tcp_private {
	struct tgt_work nop_work;
	struct list_head iscsi_tcp_conn_list;
	long nop_ttt;
};

static int iscsi_send_ping_nop_in(struct iscsi_tcp_connection *tcp_conn)
{
	struct iscsi_connection *conn = &tcp_conn->iscsi_conn;
	struct iscsi_task *task = NULL;

	task = iscsi_tcp_alloc_task(&tcp_conn->iscsi_conn, 0);
	task->conn = conn;

	task->tag = ISCSI_RESERVED_TAG;
	task->req.opcode = ISCSI_OP_NOOP_IN;
	task->req.itt = cpu_to_be32(ISCSI_RESERVED_TAG);
	task->req.ttt = cpu_to_be32(tcp_conn->ttt);

	list_add_tail(&task->c_list, &task->conn->tx_clist);
	task->conn->tp->ep_event_modify(task->conn, EPOLLIN | EPOLLOUT);

	return 0;
}

static void iscsi_tcp_nop_work_handler(struct tgt_work *w)
{
	struct tgt_evloop *evloop = w->evloop;
	struct iscsi_tcp_private *prv = tgt_event_userdata(evloop,
		EV_DATA_TCPIP);
	struct iscsi_tcp_connection *tcp_conn;

	list_for_each_entry(tcp_conn, &prv->iscsi_tcp_conn_list, tcp_conn_siblings) {
		if (tcp_conn->nop_interval == 0)
			continue;

		tcp_conn->nop_tick--;
		if (tcp_conn->nop_tick > 0)
			continue;

		tcp_conn->nop_tick = tcp_conn->nop_interval;

		tcp_conn->nop_inflight_count++;
		if (tcp_conn->nop_inflight_count > tcp_conn->nop_count) {
			eprintf("tcp connection timed out after %d failed " \
				"NOP-OUT\n", tcp_conn->nop_count);
			conn_close(&tcp_conn->iscsi_conn);
			/* cant/shouldnt delete tcp_conn from within the loop */
			break;
		}
		prv->nop_ttt++;
		if (prv->nop_ttt == ISCSI_RESERVED_TAG)
			prv->nop_ttt = 1;

		tcp_conn->ttt = prv->nop_ttt;
		iscsi_send_ping_nop_in(tcp_conn);
	}

	ev_add_work(w->evloop, w, 1);
}

static void iscsi_tcp_nop_reply(struct tgt_evloop *evloop, long ttt)
{
	struct iscsi_tcp_private *prv = tgt_event_userdata(evloop,
		EV_DATA_TCPIP);
	 struct iscsi_tcp_connection *tcp_conn;

	list_for_each_entry(tcp_conn, &prv->iscsi_tcp_conn_list, tcp_conn_siblings) {
		if (tcp_conn->ttt != ttt)
			continue;
		tcp_conn->nop_inflight_count = 0;
	}
}

int iscsi_update_target_nop_count(int tid, int count)
{
	struct iscsi_target *target;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (target->tid != tid)
			continue;
		target->nop_count = count;
		return 0;
	}
	return -1;
}

int iscsi_update_target_nop_interval(int tid, int interval)
{
	struct iscsi_target *target;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (target->tid != tid)
			continue;
		target->nop_interval = interval;
		return 0;
	}
	return -1;
}

void iscsi_set_nop_interval(int interval)
{
	default_nop_interval = interval;
}

void iscsi_set_nop_count(int count)
{
	default_nop_count = count;
}

static int set_keepalive(int fd)
{
	int ret, opt;

	opt = 1;
	ret = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
	if (ret)
		return ret;

	opt = 1800;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &opt, sizeof(opt));
	if (ret)
		return ret;

	opt = 6;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &opt, sizeof(opt));
	if (ret)
		return ret;

	opt = 300;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &opt, sizeof(opt));
	if (ret)
		return ret;

	return 0;
}

static int set_nodelay(int fd)
{
	int ret, opt;

	opt = 1;
	ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
	return ret;
}

static int set_zerocopy(int fd)
{
	int one = 1;
	if (setsockopt(fd, SOL_SOCKET, SO_ZEROCOPY, &one, sizeof(one))) {
		eprintf("setsockopt zerocopy failed, %d, %s", errno, strerror(errno));
		return -1;
	}
	return 0;
}

static void accept_connection(struct tgt_evloop *evloop, int afd, int events, void *data)
{
	struct iscsi_tcp_private *prv = tgt_event_userdata(evloop,
		EV_DATA_TCPIP);
	struct sockaddr_storage from;
	socklen_t namesize;
	struct iscsi_connection *conn;
	struct iscsi_tcp_connection *tcp_conn;
	int fd, ret=0;

	namesize = sizeof(from);
	fd = accept(afd, (struct sockaddr *) &from, &namesize);
	if (fd < 0) {
		eprintf("can't accept, %m\n");
		return;
	}

	if (!is_system_available())
		goto out;

	if (list_empty(&iscsi_targets_list))
		goto out;

	ret = set_keepalive(fd);
	if (ret)
		goto out;

	ret = set_nodelay(fd);
	if (ret)
		goto out;

	tcp_conn = zalloc(sizeof(*tcp_conn));
	if (!tcp_conn)
		goto out;

	if (g_enable_zerocopy)
		tcp_conn->zerocopy.enable = (set_zerocopy(fd) == 0);

	conn = &tcp_conn->iscsi_conn;

	ret = conn_init(conn);
	if (ret) {
		free(tcp_conn);
		goto out;
	}
	INIT_LIST_HEAD(&tcp_conn->zc_buf_out_list);
	INIT_LIST_HEAD(&tcp_conn->zc_buf_defer_free_list);
	INIT_LIST_HEAD(&tcp_conn->zc_info_free_list);
	tcp_conn->fd = fd;
	conn->evloop = evloop;
	conn->tp = &iscsi_tcp;

	conn_read_pdu(conn);
	set_non_blocking(fd);

	ret = tgt_event_insert(evloop, fd, EPOLLIN, iscsi_tcp_event_handler, conn);
	if (ret) {
		conn_exit(conn);
		free(tcp_conn);
		goto out;
	}

	list_add(&tcp_conn->tcp_conn_siblings, &prv->iscsi_tcp_conn_list);

	return;
out:
	close(fd);
	return;
}

/* Receive zerocopy notification */
static void iscsi_tcp_event_err_handler(struct iscsi_connection *conn_)
{
	struct iscsi_tcp_connection *conn = TCP_CONN(conn_);
	struct sock_extended_err *serr;
	struct msghdr msg = {};
	struct cmsghdr *cm;
	char control[100];
	uint32_t hi, lo, range;
	int ret, zerocopy;

	msg.msg_control = control;
	msg.msg_controllen = sizeof(control);
	ret = recvmsg(conn->fd, &msg, MSG_ERRQUEUE);
	if (ret == -1) {
		if (errno != EAGAIN)
			eprintf("recvmsg notification: failed, errno %d", errno);
		return;
	}
	if (msg.msg_flags & MSG_CTRUNC) {
		eprintf("recvmsg notification: truncated");
		abort();
		return;
	}

	cm = CMSG_FIRSTHDR(&msg);
	if (!cm) {                                                               
		eprintf("cmsg: no cmsg");                                   
		abort();
	}

	if (!((cm->cmsg_level == SOL_IP && cm->cmsg_type == IP_RECVERR) ||
				(cm->cmsg_level == SOL_IPV6 && cm->cmsg_type == IPV6_RECVERR))) {
		eprintf("serr: wrong type: %d.%d", cm->cmsg_level, cm->cmsg_type); 
		abort();
	}

	serr = (void *)CMSG_DATA(cm);
	if (serr->ee_origin != SO_EE_ORIGIN_ZEROCOPY) {
		eprintf("serr: wrong origin: %u", serr->ee_origin);
		abort();
	}
	if (serr->ee_errno != 0) {
		eprintf("serr: wrong error code: %u", serr->ee_errno);
		abort();
	}

	hi = serr->ee_data;
	lo = serr->ee_info;
	range = hi - lo + 1;

	/* Detect notification gaps. These should not happen often, if at all.
	 * Gaps can occur due to drops, reordering and retransmissions.         
	 */
	if (lo != conn->zerocopy.next_completion) {
		eprintf("gap: %u..%u does not append to %u\n",
				lo, hi, conn->zerocopy.next_completion);
	}
	conn->zerocopy.next_completion = hi + 1;

	zerocopy = !(serr->ee_code & SO_EE_CODE_ZEROCOPY_COPIED);
	if (!zerocopy) {
		char addr[128];
		addr[0] = 0;
		iscsi_tcp_show(conn_, addr, sizeof(addr));
		eprintf("peer %s, SO_EE_CODE_ZEROCOPY_COPIED detected, disabling zerocopy\n", addr);
		conn->zerocopy.enable = 0; /* disable zerocopy */
	}

	conn->zerocopy.completions += range;

	iscsi_tcp_retire_bufs(conn);
}

static void do_iscsi_tcp_event_handler(struct tgt_evloop *evloop, int fd,
	int events, void *data, int *closed)
{
	struct iscsi_connection *conn = (struct iscsi_connection *) data;

	*closed = 0;

	if (events & EPOLLERR)
		iscsi_tcp_event_err_handler(conn);

	if (events & EPOLLIN)
		iscsi_rx_handler(conn);

	if (conn->state == STATE_CLOSE)
		dprintf("connection closed\n");

	if (conn->state != STATE_CLOSE && events & EPOLLOUT)
		iscsi_tx_handler(conn);

	if (conn->state == STATE_CLOSE) {
		dprintf("connection closed %p\n", conn);
		
		struct target *target = NULL;
		if (conn->session) {
			target = conn->session->target->base_target;
			if (target->evloop != evloop)
				target_lock(target);
			else
				target = NULL;
		}
		conn_close(conn);
		if (target)
			target_unlock(target);
		*closed = 1;
	}
}

static void iscsi_tcp_event_handler(struct tgt_evloop *evloop, int fd, int events, void *data)
{
	struct iscsi_connection *conn = (struct iscsi_connection *) data;
	struct target *migrate_to = NULL;
	int closed;

	do_iscsi_tcp_event_handler(evloop, fd, events, data, &closed);
	if (!closed) {
		if ((migrate_to = conn->migrate_to)) {
			target_lock(migrate_to);
			conn->migrate_to = NULL;
			dprintf("connection %p migrate_to %p\n", conn, migrate_to);
			conn->tp->ep_migrate_evloop(conn, evloop, migrate_to->evloop);
			conn->migrate_to = NULL;
			conn->evloop = migrate_to->evloop;
			target_unlock(migrate_to);
			return;
		}
	}
}

int iscsi_tcp_init_portal(char *addr, int port, int tpgt)
{
	struct addrinfo hints, *res, *res0;
	char servname[64];
	int ret, fd, opt, nr_sock = 0;
	struct iscsi_portal *portal = NULL;
	char addrstr[64];
	void *addrptr = NULL;

	if (port < 0 || port > 65535) {
		errno = EINVAL;
		eprintf("port out of range, %m\n");
		return -errno;
	}

	memset(servname, 0, sizeof(servname));
	snprintf(servname, sizeof(servname), "%d", port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	ret = getaddrinfo(addr, servname, &hints, &res0);
	if (ret) {
		eprintf("unable to get address info, %m\n");
		return -errno;
	}

	for (res = res0; res; res = res->ai_next) {
		fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (fd < 0) {
			if (res->ai_family == AF_INET6)
				dprintf("IPv6 support is disabled.\n");
			else
				eprintf("unable to create fdet %d %d %d, %m\n",
					res->ai_family,	res->ai_socktype,
					res->ai_protocol);
			continue;
		}

		opt = 1;
		ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt,
				 sizeof(opt));
		if (ret)
			dprintf("unable to set SO_REUSEADDR, %m\n");

		opt = 1;
		if (res->ai_family == AF_INET6) {
			ret = setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &opt,
					 sizeof(opt));
			if (ret) {
				close(fd);
				continue;
			}
		}

		ret = bind(fd, res->ai_addr, res->ai_addrlen);
		if (ret) {
			close(fd);
			eprintf("unable to bind server socket, %m\n");
			continue;
		}

		ret = listen(fd, SOMAXCONN);
		if (ret) {
			eprintf("unable to listen to server socket, %m\n");
			close(fd);
			continue;
		}

		ret = getsockname(fd, res->ai_addr, &res->ai_addrlen);
		if (ret) {
			close(fd);
			eprintf("unable to get socket address, %m\n");
			continue;
		}

		set_non_blocking(fd);
		ret = tgt_event_insert(main_evloop, fd, EPOLLIN, accept_connection, NULL);
		if (ret)
			close(fd);
		else {
			listen_fds[nr_sock] = fd;
			nr_sock++;
		}

		portal = zalloc(sizeof(struct iscsi_portal));
		switch (res->ai_family) {
		case AF_INET:
			addrptr = &((struct sockaddr_in *)
				    res->ai_addr)->sin_addr;
			port = ntohs(((struct sockaddr_in *)
					res->ai_addr)->sin_port);
			break;
		case AF_INET6:
			addrptr = &((struct sockaddr_in6 *)
				    res->ai_addr)->sin6_addr;
			port = ntohs(((struct sockaddr_in6 *)
					res->ai_addr)->sin6_port);
			break;
		}
		portal->addr = strdup(inet_ntop(res->ai_family, addrptr,
			     addrstr, sizeof(addrstr)));
		portal->port = port;
		portal->tpgt = tpgt;
		portal->fd   = fd;
		portal->af   = res->ai_family;

		list_add(&portal->iscsi_portal_siblings, &iscsi_portals_list);
	}

	freeaddrinfo(res0);

	return !nr_sock;
}

int iscsi_add_portal(char *addr, int port, int tpgt)
{
	const char *addr_str = "";

	if (iscsi_tcp_init_portal(addr, port, tpgt)) {
		if (addr) {
			addr_str = addr;
		}
		eprintf("failed to create/bind to portal %s:%d\n",
			addr_str, port);
		return -1;
	}

	return 0;
};

int iscsi_delete_portal(char *addr, int port)
{
	struct iscsi_portal *portal;

	list_for_each_entry(portal, &iscsi_portals_list,
			    iscsi_portal_siblings) {
		if (!strcmp(addr, portal->addr) && port == portal->port) {
			if (portal->fd != -1)
				tgt_event_delete(main_evloop, portal->fd);
			close(portal->fd);
			list_del(&portal->iscsi_portal_siblings);
			free(portal->addr);
			free(portal);
			return 0;
		}
	}
	eprintf("delete_portal failed. No such portal found %s:%d\n",
			addr, port);
	return -1;
}

static int iscsi_tcp_init(void)
{
	/* If we were passed any portals on the command line */
	if (portal_arguments)
		iscsi_param_parse_portals(portal_arguments, 1, 0);

	/* if the user did not set a portal we default to wildcard
	   for ipv4 and ipv6
	*/
	if (list_empty(&iscsi_portals_list)) {
		iscsi_add_portal(NULL, ISCSI_LISTEN_PORT, 1);
	}
	return 0;
}

static int iscsi_tcp_init_evloop(struct tgt_evloop *evloop)
{
	struct iscsi_tcp_private *prv = zalloc(sizeof(*prv));

	INIT_LIST_HEAD(&prv->iscsi_tcp_conn_list);

	prv->nop_work.func = iscsi_tcp_nop_work_handler;
	prv->nop_work.data = &prv->nop_work;
	ev_add_work(evloop, &prv->nop_work, 1);

	tgt_event_set_userdata(evloop, EV_DATA_TCPIP, prv);
	return 0;
}

static void iscsi_tcp_fini_evloop(struct tgt_evloop *evloop)
{
	struct iscsi_tcp_private *prv = tgt_event_userdata(evloop,
		EV_DATA_TCPIP);

	tgt_event_set_userdata(evloop, EV_DATA_TCPIP, NULL);
	free(prv);
}

static void iscsi_tcp_exit(void)
{
	struct iscsi_portal *portal, *ptmp;

	list_for_each_entry_safe(portal, ptmp, &iscsi_portals_list,
			    iscsi_portal_siblings) {
		iscsi_delete_portal(portal->addr, portal->port);
	}
}

static int iscsi_tcp_conn_login_complete(struct iscsi_connection *conn)
{
	struct iscsi_tcp_private *prv = tgt_event_userdata(conn->evloop, EV_DATA_TCPIP);
	struct iscsi_tcp_connection *tcp_conn;
	struct iscsi_target *target;

	list_for_each_entry(tcp_conn, &prv->iscsi_tcp_conn_list, tcp_conn_siblings)
		if (&tcp_conn->iscsi_conn == conn)
			break;

	if (tcp_conn == NULL)
		return 0;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (target->tid != conn->tid)
			continue;

		tcp_conn->nop_count = target->nop_count;
		tcp_conn->nop_interval = target->nop_interval;
		tcp_conn->nop_tick = target->nop_interval;
		break;
	}

	return 0;
}

static void iscsi_tcp_migrate_evloop(
	struct iscsi_connection *conn, struct tgt_evloop *old, struct tgt_evloop *new)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	/* struct iscsi_tcp_private *old_prv = tgt_event_userdata(old, EV_DATA_TCPIP); */
	struct iscsi_tcp_private *new_prv = tgt_event_userdata(new, EV_DATA_TCPIP);

	list_del_init(&tcp_conn->tcp_conn_siblings);
	list_add_tail(&tcp_conn->tcp_conn_siblings, &new_prv->iscsi_tcp_conn_list);
	if (tgt_event_migrate(tcp_conn->fd, old, new)) {
		eprintf("can not migrate conn");
		abort();
	}
}

static size_t iscsi_tcp_read(struct iscsi_connection *conn, void *buf,
			     size_t nbytes)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	return read(tcp_conn->fd, buf, nbytes);
}

static void iscsi_tcp_start_output_buf(struct iscsi_connection *conn,
	char *buf, size_t len)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	struct zerocopy_info *pos;

	if (len < LINUX_ZEROCOPY_THRESHOLD) {
		return;
	}

	if (!tcp_conn->zerocopy.enable) {
		return;
	}

	/* Normally we only have one buffer inflight */
	list_for_each_entry(pos, &tcp_conn->zc_buf_out_list, link) {
		if (pos->buf == buf)
			return;
	}

	pos = alloc_zerocopy_info(tcp_conn);
	if (pos == NULL)
		return;

	pos->buf = buf;
	pos->len = len;
	pos->sn = 0;
	pos->free = 0;
	list_add(&pos->link, &tcp_conn->zc_buf_out_list);
}

static struct zerocopy_info *iscsi_tcp_find_out_buf(
	struct iscsi_tcp_connection *tcp_conn, void *addr)
{
	struct zerocopy_info *pos;

	list_for_each_entry(pos, &tcp_conn->zc_buf_out_list, link) {
		if ((char *)addr >= pos->buf &&
                    (char *)addr < pos->buf + pos->len) {
			return pos;
		}
	}

	return NULL;
}

static void iscsi_tcp_retire_bufs(struct iscsi_tcp_connection *conn)
{
	struct zerocopy_info *pos, *next;

	list_for_each_entry_safe(pos, next, &conn->zc_buf_defer_free_list,
	    link) {
		if (conn->zerocopy.completions >= pos->sn) {
			list_del(&pos->link);
			free(pos->buf);
			release_zerocopy_info(conn, pos);
		} else {
			break;
		}
	}
}

static size_t iscsi_tcp_write_begin(struct iscsi_connection *conn, void *buf,
				    size_t nbytes)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	int opt = 1;

	setsockopt(tcp_conn->fd, SOL_TCP, TCP_CORK, &opt, sizeof(opt));
	if (tcp_conn->zerocopy.enable && nbytes >= LINUX_ZEROCOPY_THRESHOLD) {
		ssize_t ret = 0;
		struct zerocopy_info *out_buf = NULL;
		struct iovec iov = {.iov_base = buf, .iov_len = nbytes};
		struct msghdr msg = {0};

		msg.msg_iov = &iov;
		msg.msg_iovlen = 1;

		out_buf = iscsi_tcp_find_out_buf(tcp_conn, buf);
		if (!out_buf)
			goto write;
		ret = sendmsg(tcp_conn->fd, &msg, MSG_ZEROCOPY|MSG_DONTWAIT);
		if (ret == -1) {
			if (errno == ENOBUFS) {
				/* turn off zerocopy */
				eprintf("got ENOBUFS, turn off zerocopy\n");
				tcp_conn->zerocopy.enable = 0;
				goto write;
			}
			return ret;
		}
		if (ret > 0) {
			tcp_conn->zerocopy.expected_completions++;
			out_buf->sn = tcp_conn->zerocopy.expected_completions;
		}
		return ret;
	}

write:
	return write(tcp_conn->fd, buf, nbytes);
}

static void iscsi_tcp_write_end(struct iscsi_connection *conn)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	int opt = 0;

	setsockopt(tcp_conn->fd, SOL_TCP, TCP_CORK, &opt, sizeof(opt));
}

static size_t iscsi_tcp_close(struct iscsi_connection *conn)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);

	tgt_event_delete(conn->evloop, tcp_conn->fd);
	conn->state = STATE_CLOSE;
	tcp_conn->nop_interval = 0;
	return 0;
}

static int iscsi_tcp_zerocopy_finished(struct iscsi_tcp_connection *tcp_conn)
{
	return list_empty(&tcp_conn->zc_buf_out_list) &&
	       list_empty(&tcp_conn->zc_buf_defer_free_list);
}

static void iscsi_tcp_free(struct iscsi_tcp_connection *tcp_conn)
{
	close(tcp_conn->fd);
	free_zerocopy_info(tcp_conn);
	free(tcp_conn);
}

static void iscsi_tcp_gc_free(struct iscsi_tcp_connection *tcp_conn)
{
	list_del(&tcp_conn->tcp_conn_siblings);
	iscsi_tcp_free(tcp_conn);
}

static void iscsi_tcp_release(struct iscsi_connection *conn)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);

	conn_exit(conn);
	list_del(&tcp_conn->tcp_conn_siblings);
	if (iscsi_tcp_zerocopy_finished(tcp_conn)) {
		iscsi_tcp_free(tcp_conn);
	} else {
		struct epoll_event evt;

		shutdown(tcp_conn->fd, SHUT_RD);

		/* add to gc list */
		pthread_mutex_lock(&g_conn_gc_mutex);
		list_add(&tcp_conn->tcp_conn_siblings, &g_conn_gc_list);
		gettimeofday(&tcp_conn->stamp, NULL);
		pthread_mutex_unlock(&g_conn_gc_mutex);

    		evt.data.ptr = tcp_conn;
		evt.events = EPOLLERR;
		if (epoll_ctl(g_gc_efd, EPOLL_CTL_ADD, tcp_conn->fd, &evt)) {
			eprintf("%s, epoll_ctl failed\n", __func__);
			abort();
		}
	}
}

static int iscsi_tcp_show(struct iscsi_connection *conn, char *buf, int rest)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	int err, total = 0;
	socklen_t slen;
	char dst[INET6_ADDRSTRLEN];
	struct sockaddr_storage from;

	slen = sizeof(from);
	err = getpeername(tcp_conn->fd, (struct sockaddr *) &from, &slen);
	if (err < 0) {
		eprintf("%m\n");
		return 0;
	}

	err = getnameinfo((struct sockaddr *)&from, sizeof(from), dst,
			  sizeof(dst), NULL, 0, NI_NUMERICHOST);
	if (err < 0) {
		eprintf("%m\n");
		return 0;
	}

	total = snprintf(buf, rest, "IP Address: %s", dst);

	return total > 0 ? total : 0;
}

static void iscsi_event_modify(struct iscsi_connection *conn, int events)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	int ret;

	ret = tgt_event_change(conn->evloop, tcp_conn->fd, events);
	if (ret)
		eprintf("tgt_event_modify failed\n");
}

static struct iscsi_task *iscsi_tcp_alloc_task(struct iscsi_connection *conn,
					size_t ext_len)
{
	struct iscsi_task *task;

	task = malloc(sizeof(*task) + ext_len);
	if (task)
		memset(task, 0, sizeof(*task) + ext_len);
	return task;
}

static void iscsi_tcp_free_task(struct iscsi_task *task)
{
	free(task);
}

static void *iscsi_tcp_alloc_data_buf(struct iscsi_connection *conn, size_t sz)
{
	void *addr = NULL;
	if (posix_memalign(&addr, sysconf(_SC_PAGESIZE), sz) != 0)
		return addr;
	return addr;
}

static void add_defer_list(struct iscsi_tcp_connection *tcp_conn, struct zerocopy_info *buf)
{
	struct zerocopy_info *pos;

	list_for_each_entry_reverse(pos, &tcp_conn->zc_buf_defer_free_list,
	    link) {
		if (buf->sn >= pos->sn) {
			list_add(&buf->link, &pos->link);
			buf->free = 1;
			return;
		}
	}

	/* sn is smallest or list empty, add at front */
	list_add(&buf->link, &tcp_conn->zc_buf_defer_free_list);
}
 
static void iscsi_tcp_free_data_buf(struct iscsi_connection *conn, void *buf)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);
	struct zerocopy_info *pos;

	list_for_each_entry(pos, &tcp_conn->zc_buf_out_list, link) {
		if (pos->buf == buf) {
			if (tcp_conn->zerocopy.completions >= pos->sn) {
				list_del(&pos->link);
				release_zerocopy_info(tcp_conn, pos);
				break;
			} else {
				list_del(&pos->link);
				add_defer_list(tcp_conn, pos);
				return;
			}
		}
	}

	if (buf)
		free(buf);
}

static int iscsi_tcp_getsockname(struct iscsi_connection *conn,
				 struct sockaddr *sa, socklen_t *len)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);

	return getsockname(tcp_conn->fd, sa, len);
}

static int iscsi_tcp_getpeername(struct iscsi_connection *conn,
				 struct sockaddr *sa, socklen_t *len)
{
	struct iscsi_tcp_connection *tcp_conn = TCP_CONN(conn);

	return getpeername(tcp_conn->fd, sa, len);
}

static void iscsi_tcp_conn_force_close(struct iscsi_connection *conn)
{
	conn->state = STATE_CLOSE;
	conn->tp->ep_event_modify(conn, EPOLLIN|EPOLLOUT|EPOLLERR);
}

void iscsi_print_nop_settings(struct concat_buf *b, int tid)
{
	struct iscsi_target *target;

	list_for_each_entry(target, &iscsi_targets_list, tlist) {
		if (target->tid != tid)
			continue;
		if (target->nop_interval == 0)
			continue;

		concat_printf(b,
		      _TAB2 "Nop interval: %d\n"
		      _TAB2 "Nop count: %d\n",
		      target->nop_interval,
		      target->nop_count);
		break;
	}
}

static int iscsi_tcp_param_parser_zerocopy(char *p)
{
	g_enable_zerocopy = atoi(p);
	return 0;
}

static void iscsi_tcp_gc_handle_input(int ev, void *ptr)
{
	struct iscsi_tcp_connection *conn = (struct iscsi_tcp_connection *)ptr;

	iscsi_tcp_event_err_handler(&conn->iscsi_conn);

	// update stamp
	gettimeofday(&conn->stamp, NULL);
	if (iscsi_tcp_zerocopy_finished(conn)) {
		pthread_mutex_lock(&g_conn_gc_mutex);
		iscsi_tcp_gc_free(conn);
		pthread_mutex_unlock(&g_conn_gc_mutex);
	}
}

static void iscsi_tcp_gc_test_timeout(void)
{
	const struct timeval timeout = {20, 0};
	struct timeval now;
	struct iscsi_tcp_connection *conn, *next;

	pthread_mutex_lock(&g_conn_gc_mutex);
	gettimeofday(&now, NULL);
	list_for_each_entry_safe(conn, next, &g_conn_gc_list, tcp_conn_siblings) {
		struct timeval result;

		timeradd(&conn->stamp, &timeout, &result);
		if (timercmp(&now, &result, >)) {
			eprintf("Connection gc timeout, %p\n", conn);
			iscsi_tcp_gc_free(conn);
		}
	}
	pthread_mutex_unlock(&g_conn_gc_mutex);
}

static void *iscsi_tcp_gc_loop(void *arg)
{
	const int POLL_SIZE = 6;
	struct epoll_event e[POLL_SIZE];
	int i, n;

	for (;;) {
		n = epoll_wait(g_gc_efd, e, POLL_SIZE, 1000);
		for (i = 0; i < n; ++i) {
			iscsi_tcp_gc_handle_input(e[i].events, e[i].data.ptr);
		}
		iscsi_tcp_gc_test_timeout();
	}

	return NULL;
}

static void iscsi_tcp_setup_gc(void)
{
	g_gc_efd = epoll_create1(O_CLOEXEC);
	pthread_create(&g_gc_thread, NULL, iscsi_tcp_gc_loop, NULL);
}

static struct iscsi_transport iscsi_tcp = {
	.name			= "iscsi",
	.rdma			= 0,
	.data_padding		= PAD_WORD_LEN,
	.ep_init		= iscsi_tcp_init,
	.ep_exit		= iscsi_tcp_exit,
	.ep_login_complete	= iscsi_tcp_conn_login_complete,
	.alloc_task		= iscsi_tcp_alloc_task,
	.free_task		= iscsi_tcp_free_task,
	.ep_read		= iscsi_tcp_read,
	.ep_write_begin		= iscsi_tcp_write_begin,
	.ep_write_end		= iscsi_tcp_write_end,
	.ep_close		= iscsi_tcp_close,
	.ep_force_close		= iscsi_tcp_conn_force_close,
	.ep_release		= iscsi_tcp_release,
	.ep_show		= iscsi_tcp_show,
	.ep_event_modify	= iscsi_event_modify,
	.alloc_data_buf		= iscsi_tcp_alloc_data_buf,
	.free_data_buf		= iscsi_tcp_free_data_buf,
	.ep_getsockname		= iscsi_tcp_getsockname,
	.ep_getpeername		= iscsi_tcp_getpeername,
	.ep_nop_reply		= iscsi_tcp_nop_reply,
	.ep_init_evloop		= iscsi_tcp_init_evloop,
	.ep_fini_evloop		= iscsi_tcp_fini_evloop,
	.ep_migrate_evloop	= iscsi_tcp_migrate_evloop,
	.ep_start_output_buf	= iscsi_tcp_start_output_buf
};

__attribute__((constructor)) static void iscsi_transport_init(void)
{
	iscsi_transport_register(&iscsi_tcp);
	setup_param("tcp_zerocopy", iscsi_tcp_param_parser_zerocopy);
	iscsi_tcp_setup_gc();
}
