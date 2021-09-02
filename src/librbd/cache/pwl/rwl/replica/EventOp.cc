#include "EventHandler.h"
#include "EventOp.h"
#include "MemoryManager.h"
#include "RpmaOp.h"
#include "Types.h"

#include <inttypes.h>
#include "librpma.h"
#include <iostream>
#include <assert.h>
#include <string>
#include <errno.h>
#include <memory>
#include <thread>
#include <chrono>

#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::" << this->name() << ": " << this << " " \
                           << __func__ << ": "

namespace librbd::cache::pwl::rwl::replica {

/*
 * common_peer_via_address -- create a new RPMA peer based on ibv_context
 * received by the provided address
 */
static int common_peer_via_address(const char *addr,
                                   enum rpma_util_ibv_context_type type,
                                   struct rpma_peer **peer_ptr)
{
	struct ibv_context *dev = NULL;

	int ret = rpma_utils_get_ibv_context(addr, type, &dev);
	if (ret)
		return ret;

	/* create a new peer object */
	return rpma_peer_new(dev, peer_ptr);
}

void RpmaPeerDeleter::operator() (struct rpma_peer *peer) {
  rpma_peer_delete(&peer);
}

void RpmaEpDeleter::operator() (struct rpma_ep *ep) {
  rpma_ep_shutdown(&ep);
}

void RpmaMRDeleter::operator() (struct rpma_mr_local *mr_ptr) {
  rpma_mr_dereg(&mr_ptr);
}

RpmaConn::~RpmaConn() {
  if (conn == nullptr) {
    return ;
  }
  if (!disconnected.load()) {
    rpma_conn_disconnect(conn);
  }
  rpma_conn_delete(&conn);
  conn = nullptr;
}

void RpmaConn::reset(struct rpma_conn *conn) {
  this->conn = conn;
  disconnected = false;
}

struct rpma_conn* RpmaConn::get() {
  return conn;
}

int RpmaConn::disconnect() {
  if (!disconnected.load()) {
    disconnected.store(true);
    return rpma_conn_disconnect(conn);
  }
  return 0;
}

AcceptorHandler::AcceptorHandler(CephContext *cct,
                                 const std::string& addr,
                                 const std::string& port,
                                 const std::weak_ptr<Reactor> reactor_manager)
  : EventHandlerInterface(cct, reactor_manager), _address(addr), _port(port)
 {
  int ret = 0;
  struct rpma_peer *peer = nullptr;
  ret = common_peer_via_address(addr.c_str(), RPMA_UTIL_IBV_CONTEXT_LOCAL, &peer);

  if (ret) {
    throw std::runtime_error("lookup an ibv_context via the address and create a new peer using it failed");
  }
  _peer.reset(peer, RpmaPeerDeleter());

  struct rpma_ep *ep = nullptr;
  ret = rpma_ep_listen(peer, addr.c_str(), port.c_str(), &ep);
  if (ret) {
    throw std::runtime_error("listening endpoint at addr:port failed");
  }
  _ep.reset(ep);

  ret = rpma_ep_get_fd(ep, &_fd);
  if (ret) {
    throw std::runtime_error("get the endpoint's event file descriptor failed");
  }
}

int AcceptorHandler::register_self() {
  if (auto reactor = _reactor_manager.lock()) {
    return reactor->register_handler(shared_from_this(), ACCEPT_EVENT);
  }
  return -1;
}

int AcceptorHandler::remove_self() {
  if (auto reactor = _reactor_manager.lock()) {
    return reactor->remove_handler(shared_from_this(), ACCEPT_EVENT);
  }
  return -1;
}

AcceptorHandler::~AcceptorHandler() {
  ldout(_cct, 20) << dendl;
}

Handle AcceptorHandler::get_handle(EventType et) const {
  return _fd;
}

int AcceptorHandler::handle(EventType et) {
  // Can only be called for an ACCEPT event.
  assert(et == ACCEPT_EVENT);

  try {
    std::shared_ptr<RPMAHandler> server_handler = std::make_shared<RPMAHandler>(_cct, _peer, _ep.get(), _reactor_manager);
    server_handler->register_self();
  } catch (std::runtime_error &e) {
    lderr(_cct) << "Runtime error: " << e.what() << dendl;
    return -1;
  }
  return 0;
}

ConnectionHandler::ConnectionHandler(CephContext *cct, const std::weak_ptr<Reactor> reactor_manager)
  : EventHandlerInterface(cct, reactor_manager) {}

// Notice: call this function after peer is initialized.
void ConnectionHandler::init_send_recv_buffer() {
  int ret = 0;
  rpma_mr_local *mr{nullptr};

  recv_bl.append(bufferptr(MSG_SIZE));
  recv_bl.rebuild_page_aligned();
  ret = rpma_mr_reg(_peer.get(), recv_bl.c_str(), MSG_SIZE, RPMA_MR_USAGE_RECV, &mr);
  if (ret) {
    throw std::runtime_error("recv memory region registers failed.");
  }
  recv_mr.reset(mr);

  mr = nullptr;
  send_bl.append(bufferptr(MSG_SIZE));
  send_bl.rebuild_page_aligned();
  ret = rpma_mr_reg(_peer.get(), send_bl.c_str(), MSG_SIZE, RPMA_MR_USAGE_SEND, &mr);
  if (ret) {
    throw std::runtime_error("send memory region registers failed.");
  }
  send_mr.reset(mr);
  return ;
}

// Notice: call this function after conn is initialized.
void ConnectionHandler::init_conn_fd() {
  int ret = 0;
  ret = rpma_conn_get_event_fd(_conn.get(), &_conn_fd);
  if (ret) {
    throw std::runtime_error("get the connection's event fd failed");
  }

  ret = rpma_conn_get_completion_fd(_conn.get(), &_comp_fd);
  if (ret) {
    throw std::runtime_error("get the connection's completion fd failed");
  }
  return ;
}

ConnectionHandler::~ConnectionHandler() {
  ldout(_cct, 10) << "table size: " << callback_table.size() << dendl;
  for (auto &it : callback_table) {
    ldout(_cct, 20) << "pointer: " << it << dendl;
    auto op_func = std::unique_ptr<RpmaOp>{it};
  }
}

Handle ConnectionHandler::get_handle(EventType et) const {
  if (et == CONNECTION_EVENT) {
    return _conn_fd;
  }
  if (et == COMPLETION_EVENT) {
    return _comp_fd;
  }
  return -1;
}

int ConnectionHandler::handle(EventType et) {
  if (et == CONNECTION_EVENT) {
    return handle_connection_event();
  }
  if (et == COMPLETION_EVENT) {
    return handle_completion();
  }
  return -1;
}

int ConnectionHandler::handle_connection_event() {
  ldout(_cct, 10) << dendl;
  int ret = 0;
  // get next connection's event
  enum rpma_conn_event event;
  ret = rpma_conn_next_event(_conn.get(), &event);
  if (ret) {
    if (ret == RPMA_E_NO_EVENT) {
      return 0;
    } else if (ret == RPMA_E_INVAL) {
      lderr(_cct) << "conn or event is NULL" << dendl;
    } else if (ret == RPMA_E_UNKNOWN) {
      lderr(_cct) << "unexpected event" << dendl;
    } else if (ret == RPMA_E_PROVIDER) {
      lderr(_cct) << "rdma_get_cm_event() or rdma_ack_cm_event() failed" << dendl;
    } else if (ret == RPMA_E_NOMEM) {
      lderr(_cct) << "out of memory" << dendl;
    }

    _conn.disconnect();
    return ret;
  }

  /* proceed to the callback specific to the received event */
  if (event == RPMA_CONN_ESTABLISHED) {
    ldout(_cct, 10) << "RPMA_CONN_ESTABLISHED" << dendl;
    {
      std::lock_guard locker(connect_lock);
      connected.store(true);
    }
    connect_cond_var.notify_all();
    return 0;
  } else if (event == RPMA_CONN_CLOSED) {
    ldout(_cct, 10) << "RPMA_CONN_CLOSED" << dendl;
  } else if (event == RPMA_CONN_LOST) {
    ldout(_cct, 10) << "RPMA_CONN_LOST" << dendl;
  } else {
    ldout(_cct, 10) << "RPMA_CONN_UNDEFINED" << dendl;
  }
  {
    std::lock_guard locker(connect_lock);
    connected.store(false);
  }
  connect_cond_var.notify_all();
  ret = remove_self();
  return ret;
}

int ConnectionHandler::handle_completion() {
  ldout(_cct, 10) << dendl;
  int ret = 0;

  /* prepare detected completions for processing */
  ret = rpma_conn_completion_wait(_conn.get());
  if (ret) {
    /* no completion is ready - continue */
    if (ret == RPMA_E_NO_COMPLETION) {
      return 0;
    } else if (ret == RPMA_E_INVAL) {
      lderr(_cct) << "conn is NULL: " << rpma_err_2str(ret) << dendl;
    } else if (ret == RPMA_E_PROVIDER) {
      lderr(_cct) << "ibv_poll_cq(3) failed with a provider error: " << rpma_err_2str(ret) << dendl;
    }

    /* another error occured - disconnect */
    _conn.disconnect();
    return ret;
  }

  /* get next completion */
  struct rpma_completion cmpl;
  ret = rpma_conn_completion_get(_conn.get(), &cmpl);
  if (ret) {
    /* no completion is ready - continue */
    if (ret == RPMA_E_NO_COMPLETION) {
      return 0;
    } else if (ret == RPMA_E_INVAL) {
      lderr(_cct) << "conn or cmpl is NULL: " << rpma_err_2str(ret) << dendl;
    } else if (ret == RPMA_E_PROVIDER) {
      lderr(_cct) << "ibv_poll_cq(3) failed with a provider error: " << rpma_err_2str(ret) << dendl;
    } else if (ret == RPMA_E_UNKNOWN) {
      lderr(_cct) << "ibv_poll_cq(3) failed but no provider error is available: " << rpma_err_2str(ret) << dendl;
    } else {
      // RPMA_E_NOSUPP
      lderr(_cct) << "Not supported opcode: " << rpma_err_2str(ret) << dendl;
    }

    /* another error occured - disconnect */
    _conn.disconnect();
    return ret;
  }

  /* validate received completion */
  if (cmpl.op_status != IBV_WC_SUCCESS) {
    ldout(_cct, 1) << "[op: " << cmpl.op << "] "
                   << "received completion is not as expected "
                   << "("
                   << cmpl.op_status << " != " << IBV_WC_SUCCESS
                   << ")"
                   << dendl;
    return ret;
  }

  if (cmpl.op == RPMA_OP_RECV) {
    ldout(_cct, 10) << "RPMA_OP_RECV" << dendl;
  } else if ( cmpl.op == RPMA_OP_SEND) {
    ldout(_cct, 10) << "RPMA_OP_SEND" << dendl;
  } else if (cmpl.op == RPMA_OP_WRITE) {
    ldout(_cct, 10) << "RPMA_OP_WRITE" << dendl;
  } else if (cmpl.op == RPMA_OP_FLUSH) {
    ldout(_cct, 10) << "RPMA_OP_FLUSH" << dendl;
  } else if (cmpl.op == RPMA_OP_READ) {
    ldout(_cct, 10) << "RPMA_OP_READ" << dendl;
  } else {
    ldout(_cct, 5) << "operation: "
                   << cmpl.op
                   << ". Shouldn't step in this."
                   << dendl;
  }

  if (cmpl.op_context != nullptr) {
    auto op_func = std::unique_ptr<RpmaOp>{static_cast<RpmaOp*>(const_cast<void *>(cmpl.op_context))};
    {
      std::lock_guard locker{callback_lock};
      callback_table.erase(op_func.get());
    }
    op_func->do_callback();
  }
  return ret;
}

int ConnectionHandler::wait_established() {
  ldout(_cct, 20) << dendl;
  using namespace std::chrono_literals;
  std::unique_lock locker(connect_lock);
  connect_cond_var.wait_for(locker, 3s, [this]{return this->connected.load();});
  return connected.load() == true ? 0 : -1;
}

int ConnectionHandler::wait_disconnected() {
  ldout(_cct, 20) << dendl;
  using namespace std::chrono_literals;
  std::unique_lock locker(connect_lock);
  connect_cond_var.wait_for(locker, 3s, [this]{return !this->connected.load();});
  return connected.load() == false ? 0 : -1;
}

int ConnectionHandler::send(std::function<void()> callback) {
  int ret = 0;
  std::unique_ptr<RpmaSend> usend = std::make_unique<RpmaSend>(callback);
  ret = (*usend)(_conn.get(), send_mr.get(), 0, MSG_SIZE,RPMA_F_COMPLETION_ALWAYS, usend.get());
  if (ret == 0) {
    {
      std::lock_guard locker{callback_lock};
      callback_table.insert(usend.get());
    }
    usend.release();
  }
  return ret;
}

int ConnectionHandler::recv(std::function<void()> callback) {
  int ret = 0;
  std::unique_ptr<RpmaRecv> rec = std::make_unique<RpmaRecv>(callback);
  ret = (*rec)(_conn.get(), recv_mr.get(), 0, MSG_SIZE, rec.get());
  if (ret == 0) {
    {
      std::lock_guard locker{callback_lock};
      callback_table.insert(rec.get());
    }
    rec.release();
  }
  return ret;
}

RPMAHandler::RPMAHandler(CephContext *cct,
                         std::shared_ptr<struct rpma_peer> peer,
                         struct rpma_ep *ep,
                         const std::weak_ptr<Reactor> reactor_manager)
  : ConnectionHandler(cct, reactor_manager), data_manager(cct) {
  _peer = peer;
  int ret = 0;

  init_send_recv_buffer();

  struct rpma_conn_req *req = nullptr;
  struct rpma_conn_cfg *cfg_ptr = nullptr;
  ret = rpma_conn_cfg_new(&cfg_ptr);
  if (ret) {
    throw std::runtime_error("new cfg failed");
  }

  //TODO: make those config
  rpma_conn_cfg_set_sq_size(cfg_ptr, 500);
  rpma_conn_cfg_set_rq_size(cfg_ptr, 500);
  rpma_conn_cfg_set_cq_size(cfg_ptr, 500);
  rpma_conn_cfg_set_timeout(cfg_ptr, 1000); //ms

  ret = rpma_ep_next_conn_req(ep, cfg_ptr, &req);
  rpma_conn_cfg_delete(&cfg_ptr);
  
  if (ret) {
    throw std::runtime_error("receive an incoming connection request failed.");
  }

  /* prepare a receive for the client's response */
  std::unique_ptr<RpmaReqRecv> recv = std::make_unique<RpmaReqRecv>([self=this](){
    self->deal_require();
  });
  ret = (*recv)(req, recv_mr.get(), 0, MSG_SIZE, recv.get());
  if (ret == 0) {
    callback_table.insert(recv.get());
    recv.release();
  }
  if (ret) {
    rpma_conn_req_delete(&req);
    throw std::runtime_error("Put an initial receive to be prepared for the first message of the client's ping-pong failed.");
  }

  struct rpma_conn *conn;
  ret = rpma_conn_req_connect(&req, nullptr, &conn);
  if (ret) {
    if (req != nullptr) {
      rpma_conn_req_delete(&req);
    }
    throw std::runtime_error("accept the connection request and obtain the connection object failed.");
  }
  _conn.reset(conn);

  init_conn_fd();
}

int RPMAHandler::register_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    if ((ret = reactor->register_handler(shared_from_this(), CONNECTION_EVENT))) {
      return ret;
    }
    if ((ret = reactor->register_handler(shared_from_this(), COMPLETION_EVENT))) {
      reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
      return ret;
    }
  }
  return ret;
}

int RPMAHandler::remove_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    ret = reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
    ret |= reactor->remove_handler(shared_from_this(), COMPLETION_EVENT);
  }
  return ret;
}

RPMAHandler::~RPMAHandler() {
  ldout(_cct, 20) << dendl;
}

int RPMAHandler::register_mr_to_descriptor(RwlReplicaInitRequestReply& init_reply) {
  int ret = 0;

  int usage = RPMA_MR_USAGE_FLUSH_TYPE_PERSISTENT | RPMA_MR_USAGE_WRITE_DST;

  // register the memory
  rpma_mr_local *mr{nullptr};
  if ((ret = rpma_mr_reg(_peer.get(), data_manager.get_pointer(), data_manager.size(),
                       usage, &mr))) {
    ldout(_cct, 1) << rpma_err_2str(ret) << dendl;
    init_reply.type = RWL_REPLICA_INIT_FAILED;
    return ret;
  }
  data_mr.reset(mr);

  // get size of the memory region's descriptor
  size_t mr_desc_size;
  ret = rpma_mr_get_descriptor_size(mr, &mr_desc_size);

  // resources - memory region
  struct rpma_peer_cfg *pcfg = NULL;

  // create a peer configuration structure
  ret = rpma_peer_cfg_new(&pcfg);

  // configure peer's direct write to pmem support
  ret = rpma_peer_cfg_set_direct_write_to_pmem(pcfg, true);
  if (ret) {
    (void) rpma_peer_cfg_delete(&pcfg);
    ldout(_cct, 1) << "rpma_peer_cfg_set_direct_write_to_pmem failed: " << rpma_err_2str(ret) << dendl;
    init_reply.type = RWL_REPLICA_INIT_FAILED;
    return ret;
  }

  // get size of the peer config descriptor
  size_t pcfg_desc_size;
  ret = rpma_peer_cfg_get_descriptor_size(pcfg, &pcfg_desc_size);

  // calculate data descriptor size for the client write
  init_reply.desc.mr_desc_size = mr_desc_size;
  init_reply.desc.pcfg_desc_size = pcfg_desc_size;
  init_reply.desc.descriptors.resize(mr_desc_size + pcfg_desc_size);

  // get the memory region's descriptor
  rpma_mr_get_descriptor(data_mr.get(), &init_reply.desc.descriptors[0]);

  // Get the peer's configuration descriptor.
  // The pcfg_desc descriptor is saved in the `descriptors`
  // just after the mr_desc descriptor.
  rpma_peer_cfg_get_descriptor(pcfg, &init_reply.desc.descriptors[mr_desc_size]);

  rpma_peer_cfg_delete(&pcfg);

  return 0;
}

int RPMAHandler::get_descriptor_for_write() {
  RwlReplicaInitRequest init;
  auto it = recv_bl.cbegin();
  init.decode(it);
  ldout(_cct, 5) << "\ncache_id: " << init.info.cache_id << "\n"
                  << "cache_size: " << init.info.cache_size << "\n"
                  << "pool_name: " << init.info.pool_name << "\n"
                  << "image_name: " << init.info.image_name << "\n"
                  << dendl;

  if (data_manager.get_pointer() == nullptr) {
    data_manager.init(std::move(init.info));
  }

  RwlReplicaInitRequestReply init_reply(RWL_REPLICA_INIT_SUCCESSED);
  if (!data_manager.is_pmem()) {
    init_reply.type = RWL_REPLICA_INIT_FAILED;
  } else {
    register_mr_to_descriptor(init_reply);
  }

  ldout(_cct, 20) << "succeed to register: "
                  << (init_reply.type == RWL_REPLICA_INIT_SUCCESSED)
                  << dendl;

  bufferlist bl;
  init_reply.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  return 0;
}

int RPMAHandler::close() {
  data_mr.reset();
  RwlReplicaFinishedRequestReply reply(RWL_REPLICA_FINISHED_SUCCCESSED);
  if (data_manager.close_and_remove()) {
    reply.type = RWL_REPLICA_FINISHED_FAILED;
  }
  ldout(_cct, 5) << "succeed to remove cachefile: "
                 << (reply.type != RWL_REPLICA_FINISHED_FAILED)
                 << dendl;

  bufferlist bl;
  reply.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  return 0;
}

void RPMAHandler::deal_require() {
  RwlReplicaRequest request;
  auto it = recv_bl.cbegin();
  request.decode(it);
  switch (request.type) {
    case RWL_REPLICA_INIT_REQUEST:
      get_descriptor_for_write();
      break;
    case RWL_REPLICA_FINISHED_REQUEST:
      close();
      break;
    default:
      ldout(_cct, 1) << "the operation isn't supported now. op: "
                     << request.type
                     << dendl;
  }

  //When it meet the close operation, don't do the recv operation
  /* prepare a receive for the client's response */
  if (request.type != RWL_REPLICA_FINISHED_REQUEST) {
    std::unique_ptr<RpmaRecv> rec = std::make_unique<RpmaRecv>([self=this](){
      self->deal_require();
    });
    int ret = (*rec)(_conn.get(), recv_mr.get(), 0, MSG_SIZE, rec.get());
    if (ret == 0) {
      callback_table.insert(rec.get());
      rec.release();
    }
  }
  /* send the data to the client */
  send(nullptr);
}


ClientHandler::ClientHandler(CephContext *cct,
                             const std::string& addr,
                             const std::string& port,
                             const std::weak_ptr<Reactor> reactor_manager)
    : ConnectionHandler(cct, reactor_manager), _address(addr), _port(port) {
  int ret = 0;
  struct rpma_peer *peer = nullptr;
  ret = common_peer_via_address(addr.c_str(), RPMA_UTIL_IBV_CONTEXT_REMOTE, &peer);
  if (ret) {
    throw std::runtime_error("lookup an ibv_context via the address and create a new peer using it failed");
  }
  _peer.reset(peer, RpmaPeerDeleter());

  init_send_recv_buffer();

  struct rpma_conn_req *req = nullptr;
  struct rpma_conn_cfg *cfg_ptr = nullptr;
  ret = rpma_conn_cfg_new(&cfg_ptr);
  if (ret) {
    throw std::runtime_error("new cfg failed");
  }

  //TODO: make those config
  rpma_conn_cfg_set_sq_size(cfg_ptr, 500);
  rpma_conn_cfg_set_rq_size(cfg_ptr, 500);
  rpma_conn_cfg_set_cq_size(cfg_ptr, 500);
  rpma_conn_cfg_set_timeout(cfg_ptr, 1000); //ms

  ret = rpma_conn_req_new(peer, addr.c_str(), port.c_str(), cfg_ptr, &req);
  rpma_conn_cfg_delete(&cfg_ptr);
  if (ret) {
    throw std::runtime_error("create a new outgoing connection request object failed");
  }

  struct rpma_conn *conn;
  ret = rpma_conn_req_connect(&req, nullptr, &conn);
  if (ret) {
    if (req != nullptr) {
      rpma_conn_req_delete(&req);
    }
    throw std::runtime_error("initiate processing the connection request");
  }
  _conn.reset(conn);

  init_conn_fd();
}

ClientHandler::~ClientHandler() {
  ldout(_cct, 20)  << dendl;
}

int ClientHandler::register_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    if ((ret = reactor->register_handler(shared_from_this(), CONNECTION_EVENT))) {
      return ret;
    }
    if ((ret = reactor->register_handler(shared_from_this(), COMPLETION_EVENT))) {
      reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
      return ret;
    }
  }
  return ret;
}

int ClientHandler::remove_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    ret = reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
    ret |= reactor->remove_handler(shared_from_this(), COMPLETION_EVENT);
  }
  return ret;
}


int ClientHandler::get_remote_descriptor() {
  RwlReplicaInitRequestReply init_reply;
  auto it = recv_bl.cbegin();
  init_reply.decode(it);
  int ret = 0;
  ldout(_cct, 5) << "init request reply: " << init_reply.type << dendl;
  if (init_reply.type == RWL_REPLICA_INIT_SUCCESSED) {
    struct RpmaConfigDescriptor *dst_data = &(init_reply.desc);
    // Create a remote peer configuration structure from the received
    // descriptor and apply it to the current connection
    bool direct_write_to_pmem = false;
    struct rpma_peer_cfg *pcfg = nullptr;
    if (dst_data->pcfg_desc_size) {
      rpma_peer_cfg_from_descriptor(&dst_data->descriptors[dst_data->mr_desc_size], dst_data->pcfg_desc_size, &pcfg);
      rpma_peer_cfg_get_direct_write_to_pmem(pcfg, &direct_write_to_pmem);
      rpma_conn_apply_remote_peer_cfg(_conn.get(), pcfg);
      rpma_peer_cfg_delete(&pcfg);
      // TODO: error handle
    }

    // Create a remote memory registration structure from received descriptor
    if ((ret = rpma_mr_remote_from_descriptor(&dst_data->descriptors[0], dst_data->mr_desc_size, &_image_mr))) {
      ldout(_cct, 1) << rpma_err_2str(ret) << dendl;
    }

    //get the remote memory region size
    size_t size;
    if ((ret = rpma_mr_remote_get_size(_image_mr, &size))) {
      ldout(_cct, 1) << rpma_err_2str(ret) << dendl;
    }

    if (size < _image_size) {
      ldout(_cct, 1) << "Remote memory region size too small "
                     << "for writing the  data of the assumed size ("
                     << size << " < " << _image_size << ")"
                     << dendl;
      return -1;
    }
  }
  return ret;
}

int ClientHandler::init_replica(epoch_t cache_id, uint64_t cache_size, std::string pool_name, std::string image_name) {
  _image_size = cache_size;
  ldout(_cct, 20) << dendl;
  RwlReplicaInitRequest init(RWL_REPLICA_INIT_REQUEST);
  init.info.cache_id = cache_id;
  init.info.cache_size = cache_size;
  init.info.pool_name = pool_name;
  init.info.image_name = image_name;
  bufferlist bl;
  init.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  {
    std::lock_guard locker(message_lock);
    recv_completed = false;
  }
  recv([this] () mutable {
    get_remote_descriptor();
    {
      std::lock_guard locker(message_lock);
      this->recv_completed = true;
    }
    this->cond_var.notify_one();
  });
  send(nullptr);
  using namespace std::chrono_literals;
  std::unique_lock locker(message_lock);
  cond_var.wait_for(locker, 5s,[this]{return this->recv_completed;});

  return (recv_completed == true ? (_image_mr == nullptr ? -28 : 0) : -1);
}

int ClientHandler::close_replica() {
  ldout(_cct, 20) << dendl;
  RwlReplicaFinishedRequest finish(RWL_REPLICA_FINISHED_REQUEST);
  bufferlist bl;
  finish.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  {
    std::lock_guard locker(message_lock);
    recv_completed = false;
    finished_success = false;
  }
  recv([this] () mutable {
    RwlReplicaFinishedRequestReply reply;
    auto it = recv_bl.cbegin();
    reply.decode(it);
    {
      std::lock_guard locker(message_lock);
      this->recv_completed = true;
      if (reply.type == RWL_REPLICA_FINISHED_SUCCCESSED) {
        this->finished_success = true;
      }
    }
    this->cond_var.notify_one();
  });
  send(nullptr);
  using namespace std::chrono_literals;
  std::unique_lock locker(message_lock);
  cond_var.wait_for(locker, 5s,[this]{return this->recv_completed;});

  return ((recv_completed == true && finished_success == true) ? 0 : -1);
}

int ClientHandler::disconnect() {
  return _conn.disconnect();
}

int ClientHandler::set_head(void *head_ptr, uint64_t size) {
  data_header = head_ptr;
  rpma_mr_local *mr{nullptr};
  int ret = rpma_mr_reg(_peer.get(), head_ptr, size, RPMA_MR_USAGE_WRITE_SRC, &mr);
  if (ret) {
    return ret;
  }
  data_mr.reset(mr);
  return 0;
}

int ClientHandler::write(size_t offset,
                         size_t len,
                         std::function<void()> callback) {
  ceph_assert(data_mr);
  ceph_assert(len <= 1024 * 1024 * 1024);
  std::unique_ptr<RpmaWrite> uwrite = std::make_unique<RpmaWrite>(callback);

  int ret = (*uwrite)(_conn.get(), _image_mr, offset, data_mr.get(), offset, len, RPMA_F_COMPLETION_ALWAYS, uwrite.get());
  if (ret == 0) {
    {
      std::lock_guard locker{callback_lock};
      callback_table.insert(uwrite.get());
    }
    uwrite.release();
  }
  return ret;
}

int ClientHandler::flush(size_t offset,
                         size_t len,
                         std::function<void()> callback) {
  ceph_assert(data_mr);
  std::unique_ptr<RpmaFlush> uflush = std::make_unique<RpmaFlush>(callback);
  int ret = (*uflush)(_conn.get(), _image_mr, offset, len, RPMA_FLUSH_TYPE_PERSISTENT, RPMA_F_COMPLETION_ALWAYS, uflush.get());
  if (ret == 0) {
    {
      std::lock_guard locker{callback_lock};
      callback_table.insert(uflush.get());
    }
    uflush.release();
  }
  return ret;
}


} //namespace ceph::librbd::cache::pwl::rwl::replica