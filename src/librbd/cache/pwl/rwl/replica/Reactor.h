#ifndef _REACTOR_H_
#define _REACTOR_H_

#include "EventHandler.h"
#include "common/ceph_context.h"
#include <unordered_map>

namespace ceph::librbd::cache::pwl::rwl::replica {


class Reactor {
public:
  Reactor(CephContext *cct);
  ~Reactor();

  // Register an EventHandler of a particular EventType.
  int register_handler(EventHandlerPtr eh, EventType et);

  // Remove an EventHandler of a particular EventType.
  int remove_handler(EventHandlerPtr eh, EventType et);

  // Entry point into the reactive event loop.
  //int handle_events(TimeValue *timeout = 0);
  int handle_events();

  bool empty() { return _event_table.empty(); }

  void shutdown();

private:
  int fd_set_nonblock(int fd);

  int _epoll;
  bool _stop{false};
  CephContext *_cct;
  std::unordered_map<Handle, EventHandle> _event_table;
};

} // namespace ceph::librbd::cache::pwl::rwl::replica
#endif //_REACTOR_H_