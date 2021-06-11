#include "Reactor.h"

#include <iostream>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <memory>
#include <unordered_map>
#include "Types.h"
#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::Reactor: " << this << " " \
                           << __func__ << ": "

namespace ceph::librbd::cache::pwl::rwl::replica {

Reactor::Reactor(CephContext *cct) : _stop(false), _cct(cct) {
  ldout(_cct, 20) << dendl;
  _epoll = epoll_create1(EPOLL_CLOEXEC);
  if (_epoll == -1) {
    throw std::runtime_error("epoll_create1 failed\n");
  }
}

Reactor::~Reactor() {
  ldout(_cct, 20) << dendl;
  close(_epoll);
}

int Reactor::fd_set_nonblock(int fd) {
  int ret = fcntl(fd, F_GETFL);
  if (ret < 0)
    return errno;

  int flags = ret | O_NONBLOCK;
  ret = fcntl(fd, F_SETFL, flags);
  if (ret < 0)
    return errno;

  return 0;
}

int Reactor::register_handler(EventHandlerPtr eh, EventType et) {
  ldout(_cct, 20) << "ptr: "<< eh << ", type: "<< et << dendl;
  Handle fd = eh->get_handle(et);
  if (fd == -1) {
    return -1;
  }

  int ret = fd_set_nonblock(fd);
  if (ret) {
    return -1;
  }

  //event_table.emplace(fd, EventHandle{eh, et}); 
  _event_table.emplace(fd, EventHandle());
  EventHandle &ed = _event_table[fd];
  ed.type = et;
  ed.handler = eh;

  // prepare an epoll event
  struct epoll_event event;
  event.events = EPOLLIN;
  event.data.ptr = &ed;

  if (epoll_ctl(_epoll, EPOLL_CTL_ADD, eh->get_handle(et), &event)) {
    int err = errno;
    _event_table.erase(fd);
    return err;
  }

  return 0;
}

int Reactor::remove_handler(EventHandlerPtr eh, EventType et) {
  ldout(_cct, 20) << "ptr: "<< eh << ", type: "<< et << dendl;
  Handle fd = eh->get_handle(et);
  if (fd == -1) {
    return -1;
  }

  epoll_ctl(_epoll, EPOLL_CTL_DEL, fd, NULL);
  _event_table.erase(fd);
  return 0;
}

void Reactor::shutdown() {
  ldout(_cct, 20) << dendl;
  for (auto &it : _event_table) {
    Handle fd = it.first;
    epoll_ctl(_epoll, EPOLL_CTL_DEL, fd, NULL);
  }
  _event_table.clear();
  _stop = true;
}


//int Reactor::handle_events(TimeValue *timeout = 0) {
int Reactor::handle_events() {
  int ret = 0;
  /* process epoll's events */
  struct epoll_event event;
  EventHandle *event_handle;
  while (!_stop) {
    while ((ret = epoll_wait(_epoll, &event, 1 /* # of events */,
                                0)) == 1) {
      event_handle = static_cast<EventHandle*>(event.data.ptr);
       ldout(_cct, 20) << "type: " << event_handle->type << dendl;
      event_handle->handler->handle(event_handle->type);
      if (empty()) {
        ldout(_cct, 10) << "My event_table is empty!!!" << dendl;
        _stop = true;
        break;
      }
    }
  }
  return ret;
}

} // namespace ceph::librbd::cache::pwl::rwl::replica