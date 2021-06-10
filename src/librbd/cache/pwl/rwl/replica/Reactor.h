#ifndef _REACTOR_H_
#define _REACTOR_H_

#include "EventHandler.h"
#include <unordered_map>

typedef int TimeValue;

class Reactor {
public:
  Reactor();
  ~Reactor();

  // Register an EventHandler of a particular EventType.
  int register_handler(EventHandlerPtr eh, EventType et);

  // Remove an EventHandler of a particular EventType.
  int remove_handler(EventHandlerPtr eh, EventType et);

  // Entry point into the reactive event loop.
  //int handle_events(TimeValue *timeout = 0);
  int handle_events();

  bool empty() { return event_table.empty(); }

  void shutdown();

private:
  int fd_set_nonblock(int fd);

  int _epoll;
  bool stop{false};
  std::unordered_map<Handle, EventHandle> event_table;
};

#endif //_REACTOR_H_