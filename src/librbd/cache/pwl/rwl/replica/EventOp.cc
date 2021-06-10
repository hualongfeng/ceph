#include "EventHandler.h"
#include "EventOp.h"
#include "MemoryManager.h"
#include "RpmaOp.h"

#include <inttypes.h>
#include "librpma.h"
#include <iostream>
#include <assert.h>
#include <string>
#include <errno.h>
#include <memory>
#include <thread>
#include <chrono>

#include "log.h"
#include "Types.h"
#include <rados/librados.hpp>

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
  std::cout << "I'm in RpmaPeerDeleter()" << std::endl;
  rpma_peer_delete(&peer);
}

void RpmaEpDeleter::operator() (struct rpma_ep *ep) {
  std::cout << "I'm in RpmaEpDeleter()" << std::endl;
  rpma_ep_shutdown(&ep);
}

void RpmaMRDeleter::operator() (struct rpma_mr_local *mr_ptr) {
  std::cout << "I'm in RpmaMRDeleter()" << std::endl;
  rpma_mr_dereg(&mr_ptr);
}

RpmaConn::~RpmaConn() {
  std::cout << "I'm in RpmaConn::~RpmaConn()" << std::endl;
  if (conn == nullptr) {
    return ;
  }
  if (!disconnected) {
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
  std::cout << "I'm in RpmaConn::disconnect()" << std::endl;
  disconnected = true;
  return rpma_conn_disconnect(conn);
}

AcceptorHandler::AcceptorHandler(const std::string& addr,
                   const std::string& port,
                   const std::weak_ptr<Reactor> reactor_manager)
  : EventHandlerInterface(reactor_manager), _address(addr), _port(port)
 {
  std::cout << "I'm in AcceptorHandler:AcceptorHandler()" << std::endl;
  int ret = 0;
  struct rpma_peer *peer = nullptr;
  ret = common_peer_via_address(addr.c_str(), RPMA_UTIL_IBV_CONTEXT_LOCAL, &peer);
  // ret = server_peer_via_address(addr.c_str(), &peer);
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
  std::cout << "I'm in AcceptorHandler::~AcceptorHandler()" << std::endl;
}

Handle AcceptorHandler::get_handle(EventType et) const {
  return _fd;
}

int AcceptorHandler::handle(EventType et) {
  // Can only be called for an ACCEPT event.
  assert(et == ACCEPT_EVENT);

  try {
    std::shared_ptr<RPMAHandler> client_handler = std::make_shared<RPMAHandler>(_peer, _ep.get(), _reactor_manager);
    client_handler->register_self();
  } catch (std::runtime_error &e) {
    std::cout << "Runtime error: " << e.what() << std::endl;
    return -1;
  }
  return 0;
}

ConnectionHandler::ConnectionHandler(const std::weak_ptr<Reactor> reactor_manager)
  : EventHandlerInterface(reactor_manager) {
  std::cout << "I'm in ConnectionHandler::ConnectionHandler()" << std::endl;
}

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
  std::cout << "I'm in ConnectionHandler::~ConnectionHandler()" << std::endl;
  std::cout << "table size: " << callback_table.size() << std::endl;
  for (auto &it : callback_table) {
    std::cout << "pointer: " << it << std::endl;
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
  std::cout << "I'm in ConnectionHandler::handle_connection_event()" << std::endl;
  int ret = 0;
  // get next connection's event
  enum rpma_conn_event event;
  ret = rpma_conn_next_event(_conn.get(), &event);
  if (ret) {
    if (ret == RPMA_E_NO_EVENT) {
      return 0;
    } else if (ret == RPMA_E_INVAL) {
      LOG("conn or event is NULL");
    } else if (ret == RPMA_E_UNKNOWN) {
      LOG("unexpected event");
    } else if (ret == RPMA_E_PROVIDER) {
      LOG("rdma_get_cm_event() or rdma_ack_cm_event() failed");
    } else if (ret == RPMA_E_NOMEM) {
      LOG("out of memory");
    }

    // rpma_conn_disconnect(_conn.get());
    _conn.disconnect();
    return ret;
  }

  /* proceed to the callback specific to the received event */
  if (event == RPMA_CONN_ESTABLISHED) {
    LOG("RPMA_CONN_ESTABLISHED");
    connected = true;
    return 0;
  } else if (event == RPMA_CONN_CLOSED) {
    LOG("RPMA_CONN_CLOSED");
  } else if (event == RPMA_CONN_LOST) {
    LOG("RPMA_CONN_LOST");
  } else {
    LOG("RPMA_CONN_UNDEFINED");
  }
  connected = false;
  ret = remove_self();
  return ret;
}

int ConnectionHandler::handle_completion() {
  std::cout << "I'm in ConnectionHandler::handle_completion()" << std::endl;
  int ret = 0;

  /* prepare detected completions for processing */
  ret = rpma_conn_completion_wait(_conn.get());
  if (ret) {
    /* no completion is ready - continue */
    if (ret == RPMA_E_NO_COMPLETION) {
      return 0;
    } else if (ret == RPMA_E_INVAL) {
      LOG("conn is NULL: %s", rpma_err_2str(ret));
    } else if (ret == RPMA_E_PROVIDER) {
      LOG("ibv_poll_cq(3) failed with a provider error: %s", rpma_err_2str(ret));
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
      LOG("conn or cmpl is NULL: %s", rpma_err_2str(ret));
    } else if (ret == RPMA_E_PROVIDER) {
      LOG("ibv_poll_cq(3) failed with a provider error: %s", rpma_err_2str(ret));
    } else if (ret == RPMA_E_UNKNOWN) {
      LOG("ibv_poll_cq(3) failed but no provider error is available: %s", rpma_err_2str(ret));
    } else {
      // RPMA_E_NOSUPP
      LOG("Not supported opcode: %s", rpma_err_2str(ret));
    }

    /* another error occured - disconnect */
    _conn.disconnect();
    return ret;
  }

  /* validate received completion */
  if (cmpl.op_status != IBV_WC_SUCCESS) {
    (void) LOG("[op: %d] received completion is not as expected (%d != %d)\n",
               cmpl.op,
               cmpl.op_status,
               IBV_WC_SUCCESS);

    return ret;
  }

  if (cmpl.op == RPMA_OP_RECV) {
    LOG("RPMA_OP_RECV");
  } else if ( cmpl.op == RPMA_OP_SEND) {
    LOG("RPMA_OP_SEND");
  } else if (cmpl.op == RPMA_OP_WRITE) {
    LOG("RPMA_OP_WRITE");
  } else if (cmpl.op == RPMA_OP_FLUSH) {
    LOG("RPMA_OP_FLUSH");
  } else if (cmpl.op == RPMA_OP_READ) {
    LOG("RPMA_OP_READ");
  } else {
    LOG("operation: %d\n. Shouldn't step in this", cmpl.op);
  }

  if (cmpl.op_context != nullptr) {
    auto op_func = std::unique_ptr<RpmaOp>{static_cast<RpmaOp*>(const_cast<void *>(cmpl.op_context))};
    callback_table.erase(op_func.get());
    op_func->do_callback();
  } else {
    LOG("op_context is nullptr");
  }
  return ret;
}

int ConnectionHandler::send(std::function<void()> callback) {
  int ret = 0;
  std::unique_ptr<RpmaSend> usend = std::make_unique<RpmaSend>(callback);
  ret = (*usend)(_conn.get(), send_mr.get(), 0, MSG_SIZE,RPMA_F_COMPLETION_ALWAYS, usend.get());
  if (ret == 0) {
    callback_table.insert(usend.get());
    usend.release();
  }
  return ret;
}

int ConnectionHandler::recv(std::function<void()> callback) {
  int ret = 0;
  std::unique_ptr<RpmaRecv> rec = std::make_unique<RpmaRecv>(callback);
  ret = (*rec)(_conn.get(), recv_mr.get(), 0, MSG_SIZE, rec.get());
  if (ret == 0) {
    callback_table.insert(rec.get());
    rec.release();
  }
  return ret;
}

RPMAHandler::RPMAHandler(std::shared_ptr<struct rpma_peer> peer,
                         struct rpma_ep *ep,
                         const std::weak_ptr<Reactor> reactor_manager)
  : ConnectionHandler(reactor_manager) {
  std::cout << "I'm in RPMAHandler::RPMAHandler()" << std::endl;
  _peer = peer;
  int ret = 0;

  init_send_recv_buffer();

  struct rpma_conn_req *req = nullptr;
  ret = rpma_ep_next_conn_req(ep, nullptr, &req);
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
  std::cout << "I'm in ~RPMAHandler()" << std::endl;
}

int RPMAHandler::register_mr_to_descriptor(enum rpma_op op) {
  int ret = 0;

  int usage = 0;
  switch (op) {
    case RPMA_OP_FLUSH:
      usage |= (data_manager.is_pmem() ? RPMA_MR_USAGE_FLUSH_TYPE_PERSISTENT : RPMA_MR_USAGE_FLUSH_TYPE_VISIBILITY);
      // don't have break
    case RPMA_OP_WRITE:
      usage |= RPMA_MR_USAGE_WRITE_DST;
      break;
    case RPMA_OP_READ:
      usage |= RPMA_MR_USAGE_READ_SRC;
//  break;
//  don't resolve read operation
    default:
      LOG("Warn: Don't step in this\n");
      break;
  }

  /* register the memory */
  rpma_mr_local *mr{nullptr};
  if ((ret = rpma_mr_reg(_peer.get(), data_manager.get_pointer(), data_manager.size(),
                       usage, &mr))) {
    LOG("%s", rpma_err_2str(ret));
    return ret;
  }
  data_mr.reset(mr);

  /* get size of the memory region's descriptor */
  size_t mr_desc_size;
  ret = rpma_mr_get_descriptor_size(mr, &mr_desc_size);

  /* calculate data for the client write */
  RwlReplicaInitRequestReply init_reply(RWL_REPLICA_INIT_SUCCESSED);
  init_reply.desc.mr_desc_size = mr_desc_size;
  init_reply.desc.descriptors.resize(mr_desc_size);

  /* get the memory region's descriptor */
  rpma_mr_get_descriptor(data_mr.get(), &init_reply.desc.descriptors[0]);

  if (op == RPMA_OP_FLUSH) {
    /* resources - memory region */
    struct rpma_peer_cfg *pcfg = NULL;

    /* create a peer configuration structure */
    ret = rpma_peer_cfg_new(&pcfg);

    if (data_manager.is_pmem()) {
      /* configure peer's direct write to pmem support */
      ret = rpma_peer_cfg_set_direct_write_to_pmem(pcfg, true);
      if (ret) {
        (void) rpma_peer_cfg_delete(&pcfg);
        LOG("rpma_peer_cfg_set_direct_write_to_pmem failed");
        return ret;
      }
    }

    /* get size of the peer config descriptor */
    size_t pcfg_desc_size;
    ret = rpma_peer_cfg_get_descriptor_size(pcfg, &pcfg_desc_size);
    init_reply.desc.pcfg_desc_size = pcfg_desc_size;
    init_reply.desc.descriptors.resize(mr_desc_size + pcfg_desc_size);

    /*
    * Get the peer's configuration descriptor.
    * The pcfg_desc descriptor is saved in the `descriptors[]` array
    * just after the mr_desc descriptor.
    */
    rpma_peer_cfg_get_descriptor(pcfg, &init_reply.desc.descriptors[mr_desc_size]);

    rpma_peer_cfg_delete(&pcfg);
  }
  bufferlist bl;
  init_reply.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  return 0;
}

int RPMAHandler::get_descriptor_for_write() {
  RwlReplicaInitRequest init;
  auto it = recv_bl.cbegin();
  init.decode(it);
  LOG("Alloc memory size: %" PRIu64 "\n", init.info.cache_size);

  std::string path("rbd-pwl." + init.info.pool_name + "." + init.info.image_name + ".pool." + std::to_string(init.info.cache_id));
  if (data_manager.get_pointer() == nullptr) {
    data_manager.init(init.info.cache_size, path);
  }
  return register_mr_to_descriptor(RPMA_OP_FLUSH);
}

int RPMAHandler::close() {
  data_mr.reset();
  RwlReplicaFinishedRequestReply reply(RWL_REPLICA_FINISHED_SUCCCESSED);
  if (data_manager.close_and_remove()) {
    reply.type = RWL_REPLICA_FINISHED_FAILED;
  }
  LOG("%d", reply.type);
  bufferlist bl;
  reply.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  send(nullptr);
  return 0;
}

int RPMAHandler::get_descriptor() {
  RwlReplicaRequest request;
  auto it = recv_bl.cbegin();
  request.decode(it);
  switch (request.type) {
    case RWL_REPLICA_INIT_REQUEST:
      return get_descriptor_for_write();
    case RWL_REPLICA_FINISHED_REQUEST:
      return close();
    default:
      LOG("the op:%d isn't supported now", request.type);
  }
  return -1;
}

void RPMAHandler::deal_require() {
  get_descriptor();

  /* prepare a receive for the client's response */
  std::unique_ptr<RpmaRecv> rec = std::make_unique<RpmaRecv>([self=this](){
    self->deal_require();
  });
  int ret = (*rec)(_conn.get(), recv_mr.get(), 0, MSG_SIZE, rec.get());
  if (ret == 0) {
    callback_table.insert(rec.get());
    rec.release();
  }

  /* send the common_data to the client */
  rpma_send(_conn.get(), send_mr.get(), 0, MSG_SIZE,RPMA_F_COMPLETION_ALWAYS, nullptr);
}


ClientHandler::ClientHandler(const std::string& addr,
                             const std::string& port,
                             const std::weak_ptr<Reactor> reactor_manager)
    : ConnectionHandler(reactor_manager), _address(addr), _port(port) {
  std::cout << "I'm in ClientHandler::ClientHandler()" << std::endl;
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
  rpma_conn_cfg_set_sq_size(cfg_ptr, 200);
  rpma_conn_cfg_set_rq_size(cfg_ptr, 200);
  rpma_conn_cfg_set_cq_size(cfg_ptr, 200);

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
  std::cout << "I'm in ClientHandler::~ClientHandler()" << std::endl;
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
  LOG("init_reply.type: %d", init_reply.type);
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
      LOG("%s", rpma_err_2str(ret));
    }

    //get the remote memory region size
    size_t size;
    if ((ret = rpma_mr_remote_get_size(_image_mr, &size))) {
      LOG("%s", rpma_err_2str(ret));
    }

    if (size < _image_size) {
      LOG("%s:%d: Remote memory region size too small for writing the"
      " data of the assumed size (%zu < %ld)",
      __FILE__, __LINE__, size, _image_size);
      return -1;
    }
    /* determine the flush type */
    if (direct_write_to_pmem) {
      LOG("RPMA_FLUSH_TYPE_PERSISTENT is supported");
      _flush_type = RPMA_FLUSH_TYPE_PERSISTENT;
    } else {
      LOG("RPMA_FLUSH_TYPE_PERSISTENT is NOT supported");
      _flush_type = RPMA_FLUSH_TYPE_VISIBILITY;
    }
  }
  return ret;
}

int ClientHandler::init_replica(epoch_t cache_id, uint64_t cache_size, std::string pool_name, std::string image_name) {
  _image_size = cache_size;
  LOG("");
  RwlReplicaInitRequest init(RWL_REPLICA_INIT_REQUEST);
  init.info.cache_id = cache_id;
  init.info.cache_size = cache_size;
  init.info.pool_name = pool_name;
  init.info.image_name = image_name;
  bufferlist bl;
  init.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  auto start = clock::now();
  std::atomic<bool> completed = false;
  recv([this, &completed] () mutable {
    get_remote_descriptor();
    completed = true;
  });
  send(nullptr);
  double cost_time;
  while(completed == false && cost_time < 10e6) { // cost_time < 1s
    std::this_thread::yield();
    auto end = clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    cost_time = duration.count();
  }
  LOG("End: %lf", cost_time);
  return (completed == true ? 0 : -1);
}

int ClientHandler::close_replica() {
  LOG("");
  RwlReplicaFinishedRequest finish(RWL_REPLICA_FINISHED_REQUEST);
  bufferlist bl;
  finish.encode(bl);
  assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());

  auto start = clock::now();
  std::atomic<bool> completed = false;
  std::atomic<bool> successed = false;
  recv([this, &completed, &successed] () mutable {
    RwlReplicaFinishedRequestReply reply;
    auto it = recv_bl.cbegin();
    reply.decode(it);
    if (reply.type == RWL_REPLICA_FINISHED_SUCCCESSED) {
      successed = true;
    } else {
      successed = false;
    }
    completed = true;
  });
  send(nullptr);
  double cost_time;
  while(completed == false && cost_time < 10e6) { // cost_time < 1s
    std::this_thread::yield();
    auto end = clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    cost_time = duration.count();
  }
  LOG("End: %lf", cost_time);
  return ((completed == true && successed == true) ? 0 : -1);
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
  assert(data_mr);
  assert(len <= 1024 * 1024 * 1024);
  std::unique_ptr<RpmaWrite> uwrite = std::make_unique<RpmaWrite>(callback);

  int ret = (*uwrite)(_conn.get(), _image_mr, offset, data_mr.get(), offset, len, RPMA_F_COMPLETION_ALWAYS, uwrite.get());
  if (ret == 0) {
    callback_table.insert(uwrite.get());
    uwrite.release();
  }
  return ret;
}

int ClientHandler::flush(size_t offset,
                         size_t len,
                         std::function<void()> callback) {
  assert(data_mr);
  std::unique_ptr<RpmaFlush> uflush = std::make_unique<RpmaFlush>(callback);
  int ret = (*uflush)(_conn.get(), _image_mr, offset, len, _flush_type, RPMA_F_COMPLETION_ALWAYS, uflush.get());
  if (ret == 0) {
    callback_table.insert(uflush.get());
    uflush.release();
  }
  return ret;
}
