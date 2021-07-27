#include <thread>
#include <chrono>

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

ReplicaClient::ReplicaClient(CephContext *cct, uint64_t size, uint32_t copies, std::string pool_name, std::string image_name)
    : _size(size), _copies(copies), _pool_name(std::move(pool_name)), _image_name(std::move(image_name)),
      _cct(cct), _reactor(std::make_shared<Reactor>(cct)) {}

ReplicaClient::~ReplicaClient() {
  ldout(_cct, 20) << dendl;
  shutdown();
}

void ReplicaClient::shutdown() {
  _reactor->shutdown();
  rados.shutdown();
}

int ReplicaClient::flush(size_t offset, size_t len) {
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler) continue;
    {
      std::lock_guard locker(flush_lock);
      one_flush_finish = false;
    }
    r = daemon.client_handler->flush(offset, len, [this]() mutable {
      {
        std::lock_guard locker(this->flush_lock);
        this->one_flush_finish = true;
      }
      this->flush_var.notify_one();
    });
    if (r != 0) {
      ldout(_cct, 1) << "rpma_flush error: " << r << dendl;
      return r;
    }
    std::unique_lock locker(flush_lock);
    using namespace std::chrono_literals;
    flush_var.wait_for(locker, 30s,[this]{return this->one_flush_finish;});
    if (one_flush_finish != true) {
      return -1;
    }
  }
  ldout(_cct, 1) << "_write_nums: " << _write_nums.load() << dendl;
  return 0;  
}

int ReplicaClient::write(size_t offset, size_t len) {
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler) continue;
    _write_nums.fetch_add(1);
    r = daemon.client_handler->write(offset, len, [this]() mutable {
      ldout(this->_cct, 20) << "write success" << dendl;
      this->_write_nums.fetch_sub(1);
    });
    if (r != 0) {
      ldout(_cct, 1) << "rpma_write error: " << r << dendl;
      return r;
    }
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
    if (!daemon.client_handler || !daemon.client_handler->connecting()) continue;
    if (_need_free_daemons.count(daemon.id) == 0) continue;  // part alloc failed, so need to free that successful part
    r = daemon.client_handler->close_replica();
    if (r == 0) {
      _need_free_daemons.erase(daemon.id);  // only close successfully to erase
    }
  }
  return 0;
}

int ReplicaClient::replica_init() {
  ldout(_cct, 10) << "pool_name: " << _pool_name << "\n"
                  << "image_name: " << _image_name << "\n"
                  << dendl;
  int r = 0;
  for (auto &daemon : _daemons) {
    if (!daemon.client_handler) continue;
    _need_free_daemons.insert(daemon.id); //maybe it create cachefile in replica daemon, but reply failed
    r = daemon.client_handler->init_replica(_cache_id, _size, _pool_name, _image_name);
    if (r < 0) {
      replica_close();
      return r;
    }
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
      daemon.client_handler->wait_disconnected();
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
  ldout(_cct, 20) << "cache_id: " << _cache_id << dendl;

  cls::rbd::RwlCacheInfo info;
  r = rwlcache_get_cacheinfo(&io_ctx, _cache_id, info);
  if (r < 0) {
    ldout(_cct, 1) << "rwlcache_get_cacheinfo: " << r << cpp_strerror(r) << dendl;
    ceph_assert(r == 0);
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

      r = daemon.client_handler->register_self();
      ceph_assert(r == 0);
    } catch (std::runtime_error &e) {
      ldout(_cct, 1) << "Runtione error: " << e.what() << dendl;
      disconnect();
      r = -1; // indicate this connection error
      goto request_ack;
    }
  }

  std::thread([this]{
    this->_reactor->handle_events();
  }).detach();

  for (auto &daemon : _daemons) {
    // wait_established() function need to change
    // so that it will finish in some times even though
    // it will not be success
    // it should a value to indicate whether it is success
    //TODO:
    r = daemon.client_handler->wait_established();
    if (r < 0) {
      // if there has connection failed, all should be stopped.
      disconnect();
      break;
    }
  }

  if (r == 0) {
    r = replica_init();
  }

request_ack:
  // if the connection failed, what do we do after?
  // Or it will be blocked on waiting established.
  // TODO: there should have success and failed
  cls::rbd::RwlCacheRequestAck ack{_cache_id, r, _need_free_daemons};
  int ret = rwlcache_request_ack(&io_ctx, ack);
  if (ret < 0) {
    ldout(_cct, 1) << "rwlcache_request_ack: " << ret << cpp_strerror(ret) << dendl;
  }

  return r;
}

}