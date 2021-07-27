#ifndef _EVENT_OP_H_
#define _EVENT_OP_H_

#include "EventHandler.h"
#include "Reactor.h"
#include <unistd.h>
#include <inttypes.h>
#include <atomic>
#include <set>
#include "MemoryManager.h"
#include "RpmaOp.h"
#include "Types.h"
#include "include/rados/librados.hpp"

#include <condition_variable>
#include <mutex>

namespace librbd::cache::pwl::rwl::replica {

struct RpmaPeerDeleter {
  void operator() (struct rpma_peer *peer);
};

struct RpmaEpDeleter {
  void operator() (struct rpma_ep *ep);
};

struct RpmaMRDeleter {
    void operator() (struct rpma_mr_local *mr_ptr);
};

using unique_rpma_peer_ptr = std::unique_ptr<struct rpma_peer, RpmaPeerDeleter>;
using unique_rpma_ep_ptr = std::unique_ptr<struct rpma_ep, RpmaEpDeleter>;
using unique_rpma_mr_ptr = std::unique_ptr<struct rpma_mr_local, RpmaMRDeleter>;

class RpmaConn {
  struct rpma_conn *conn{nullptr};
  std::atomic<bool> disconnected{true};
public:
  RpmaConn(struct rpma_conn *conn): conn(conn), disconnected(false) {}
  RpmaConn() : conn(nullptr), disconnected(true) {}
  RpmaConn(const RpmaConn &) = delete;
  RpmaConn& operator=(const RpmaConn &) = delete;
  ~RpmaConn();

  void reset(struct rpma_conn *conn);

  struct rpma_conn *get();

  int disconnect();
};


class EventHandlerInterface : public EventHandler {
public:
  EventHandlerInterface(CephContext *cct, std::weak_ptr<Reactor> reactor_ptr): _reactor_manager(reactor_ptr), _cct(cct) {}
  ~EventHandlerInterface() {}
  virtual const char* name() const = 0;
  virtual int register_self() = 0;
  virtual int remove_self() = 0;
protected:
  std::weak_ptr<Reactor> _reactor_manager;
  CephContext *_cct;
};

// Handles client connection requests.
class AcceptorHandler : public EventHandlerInterface, public std::enable_shared_from_this<AcceptorHandler> {
public:
  AcceptorHandler(CephContext *cct,
                  const std::string& addr,
                  const std::string& port,
                  const std::weak_ptr<Reactor> reactor_manager);

  ~AcceptorHandler();
  AcceptorHandler(const AcceptorHandler &) = delete;
  AcceptorHandler& operator=(const AcceptorHandler &) = delete;

  virtual int register_self() override;
  virtual int remove_self() override;

  // Factory method that accepts a new connection request and
  // creates a RPMA_Handler object to handle connection event
  // using the connection.
  virtual int handle(EventType et) override;

  // Get the I/O  Handle (called by Reactor when
  // Acceptor is registered).
  virtual Handle get_handle(EventType et) const override;
  virtual const char* name() const override { return "AcceptorHandler"; }

private:
  Handle _fd;
  std::string _address;
  std::string _port;
  std::shared_ptr<struct rpma_peer> _peer;
  unique_rpma_ep_ptr _ep;
};


class ConnectionHandler : public EventHandlerInterface {
public:
  ConnectionHandler(CephContext *cct, const std::weak_ptr<Reactor> reactor_manager);
  ~ConnectionHandler();

  // Hook method that handles the connection.
  virtual int handle(EventType et) override;
   // Get the I/O Handle (called by the Reactor when
  // the Handler is registered).
  virtual Handle get_handle(EventType et) const override;

  int handle_completion();
  int handle_connection_event();

  int send(std::function<void()> callback);
  int recv(std::function<void()> callback);

  virtual const char* name() const override { return "ConnectionHandler"; }

  bool connecting() {return connected.load();}
  // wait for the connection to establish
  int wait_established();

protected:
  // Notice: call this function after peer is initialized.
  void init_send_recv_buffer();
  // Notice: call this function after conn is initialized.
  void init_conn_fd();

  std::set<RpmaOp*> callback_table;

  std::shared_ptr<struct rpma_peer> _peer;
  RpmaConn _conn;

  bufferlist send_bl;
  unique_rpma_mr_ptr send_mr;
  bufferlist recv_bl;
  unique_rpma_mr_ptr recv_mr;

  Handle _conn_fd;
  Handle _comp_fd;

private:
  std::atomic<bool> connected{false};
  std::mutex connect_lock;
  std::condition_variable connect_cond_var;
};

class RPMAHandler : public ConnectionHandler, public std::enable_shared_from_this<RPMAHandler>{
public:
  // Initialize the client request
  RPMAHandler(CephContext *cct,
              std::shared_ptr<struct rpma_peer> peer,
              struct rpma_ep *ep,
              const std::weak_ptr<Reactor> reactor_manager);
  ~RPMAHandler();
  virtual int register_self() override;
  virtual int remove_self() override;
  virtual const char* name() const override { return "RPMAHandler"; }
private:
  int register_mr_to_descriptor(RwlReplicaInitRequestReply& init_reply);
  int get_descriptor_for_write();
  void deal_require();
  int close();

  // memory resource
  MemoryManager data_manager;
  unique_rpma_mr_ptr data_mr;
};

class ClientHandler : public ConnectionHandler, public std::enable_shared_from_this<ClientHandler> {
public:
  // Initialize the client request
  ClientHandler(CephContext *cct,
                const std::string& addr,
                const std::string& port,
                const std::weak_ptr<Reactor> reactor_manager);

  ~ClientHandler();
  ClientHandler(const ClientHandler &) = delete;
  ClientHandler& operator=(const ClientHandler &) = delete;
  virtual int register_self() override;
  virtual int remove_self() override;

  int disconnect();

  int write(size_t offset,
            size_t len,
            std::function<void()> callback);
  int flush(size_t offset,
            size_t len,
            std::function<void()> callback);
  int write_atomic(std::function<void()> callback);
  int get_remote_descriptor();
  int prepare_for_send();
  int init_replica(epoch_t cache_id, uint64_t cache_size, std::string pool_name, std::string image_name);
  int close_replica();
  int set_head(void *head_ptr, uint64_t size);
  virtual const char* name() const override { return "ClientHandler"; }
private:
  std::string _address;
  std::string _port;

  void *data_header;
  unique_rpma_mr_ptr data_mr;

  size_t _image_size;
  struct rpma_mr_remote* _image_mr{nullptr};

  std::mutex message_lock;
  std::condition_variable cond_var;
  bool recv_completed;
  bool finished_success;
};

} //namespace ceph::librbd::cache::pwl::rwl::replica
#endif //_EVENT_OP_H_
