#include <thread>

#include "ReplicaClient.h"
#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"
#include "Types.h"
#include "RpmaOp.h"

#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd.h"
#include "librbd/Types.h"

#include "common/errno.h"
#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::ReplicaClient: " << this << " " \
                           << __func__ << ": "

namespace librbd::cache::pwl::rwl::replica {

using namespace librbd::cls_client;

int ReplicaClient::flush(size_t offset, size_t len) {
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler) continue;
    std::atomic<bool> completed{false};
    r = daemon.client_handler->flush(offset, len, [&completed]() mutable {
      completed = true;
    });
    if (r != 0) {
      ldout(_cct, 1) << "rpma_flush error: " << r << dendl;
      return r;
    }
    while(completed == false);
  }
  return 0;  
}

int ReplicaClient::write(size_t offset, size_t len) {
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler) continue;
    std::atomic<bool> completed{false};
    r = daemon.client_handler->write(offset, len, [&completed]() mutable {
      completed = true;
    });
    if (r != 0) {
      ldout(_cct, 1) << "rpma_write error: " << r << dendl;
      return r;
    }
    while(completed == false);
  }
  return 0;
}

int ReplicaClient::cache_free() {
  int r = 0;
  cls::rbd::RwlCacheFree free{_cache_id, _need_free_daemons};
  r = rwlcache_free(&io_ctx, free);
  if (r < 0) {
      ldout(_cct, 1) << "rwlcache_free: " << r << cpp_strerror(r) << dendl;
  }
  return r;
}

int ReplicaClient::replica_close() {
  ldout(_cct, 20) << dendl;
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler) continue;
    if (_need_free_daemons.count(daemon.id) == 0) continue;
    r = daemon.client_handler->close_replica();
    if (r < 0) {
      return r;
    }
    _need_free_daemons.erase(daemon.id);
  }
  return 0;
}

int ReplicaClient::replica_init(std::string pool_name, std::string image_name) {
  ldout(_cct, 10) << "pool_name: " << pool_name << "\n"
                  << "image_name: " << image_name << "\n" 
                  << dendl;
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler) continue;
    r = daemon.client_handler->init_replica(_cache_id, _size, pool_name, image_name);
    if (r < 0) {
      return r;
    }
    _need_free_daemons.insert(daemon.id);
  }
  return 0;
}

int ReplicaClient::set_head(void *head_ptr, uint64_t size) {
  ldout(_cct, 10) << "head_ptr: " << head_ptr << "\n"
                  << "size: " << size << "\n"
                  << dendl;
  ceph_assert(size <= _size);
  for (auto &daemon : _daemons) {
    if (daemon.client_handler) {
      int r = daemon.client_handler->set_head(head_ptr, size);
      if (r < 0) {
        return r;
      }
    }
  }
  return 0;
}

int ReplicaClient::init_rados() {
  int r = 0;
  r = rados.init_with_context(_cct);
  if (r < 0) {
    return r;
  }

  r = rados.connect();
  if (r < 0) {
    ldout(_cct, 1) << "failed to connect to cluster: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

int ReplicaClient::init_ioctx() {
  int r = 0;

  std::string poolname = _cct->_conf.get_val<std::string>("rbd_persistent_replicated_cache_cls_pool");
  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0) {
    ldout(_cct, 1) << "failed to access pool "<< cpp_strerror(r) << dendl;
  }
  return r;
}

void ReplicaClient::disconnect() {
  ldout(_cct, 20) << dendl;
  for (auto &daemon : _daemons) {
    if (daemon.client_handler) {
      daemon.client_handler->disconnect();
      daemon.client_handler.reset();
    }
  }
}

int ReplicaClient::cache_request() {
  ldout(_cct, 20) << dendl;
  _id = rados.get_instance_id();

  int r = 0;
  cls::rbd::RwlCacheRequest req = {_id, _size, _copies};
  r = rwlcache_request(&io_ctx, req, _cache_id);
  if (r < 0) {
    ldout(_cct, 1) << "rwlcache_request: " << r << cpp_strerror(r) << dendl;
    return r;
  }

  cls::rbd::RwlCacheInfo info;
  r = rwlcache_get_cacheinfo(&io_ctx, _cache_id, info);
  if (r < 0) {
    ldout(_cct, 1) << "rwlcache_get_cacheinfo: " << r << cpp_strerror(r) << dendl;
    return r;
  }

  ceph_assert(info.cache_id == _cache_id);

  for (auto &daemon : info.daemons) {
    ldout(_cct, 10) << "daemons infomation: \n"
                    << "id: " << daemon.id << "\n"
                    << "rdma_address: " << daemon.rdma_address << "\n"
                    << "rdma_port: " << daemon.rdma_port << "\n"
                    << dendl;
    _daemons.push_back(DaemonInfo{daemon.id, daemon.rdma_address, std::to_string(daemon.rdma_port), nullptr});
  }

  //connect to replica
  for (auto &daemon : _daemons) {
    try {
      daemon.client_handler = std::make_shared<ClientHandler>(_cct, daemon.rdma_ip, daemon.rdma_port, _reactor);

      if ((r = daemon.client_handler->register_self())) {
        return r;
      }
    } catch (std::runtime_error &e) {
      ldout(_cct, 1) << "Runtione error: " << e.what() << dendl;
      disconnect();
      return -1;
    }
  }

  std::thread([this]{
    while(this->_reactor) {
      this->_reactor->handle_events();
    }
  }).detach();

  for (auto &daemon : _daemons) {
    // wait_established() function need to change
    // so that it will finish in some times even though
    // it will not be success
    // it should a value to indicate whether it is success
    daemon.client_handler->wait_established();
  }

  // if the connection failed, what do we do after?
  // Or it will be blocked on waiting established.
  // TODO: there should have success and failed
  cls::rbd::RwlCacheRequestAck ack{_cache_id, 0};
  r = rwlcache_request_ack(&io_ctx, ack);
  if (r < 0) {
    ldout(_cct, 1) << "rwlcache_request_ack: " << r << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

}