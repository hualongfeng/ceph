#include "TimerPing.h"

#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd.h"
#include "librbd/Types.h"

#include "common/errno.h"
#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::TimePing: " << this << " " \
                           << __func__ << ": "

namespace librbd::cache::pwl::rwl::replica {

using namespace librbd::cls_client;

DaemonPing::DaemonPing(CephContext *cct, librados::Rados &rados, librados::IoCtx &io_ctx)
  : _cct(cct), _rados(rados), _io_ctx(io_ctx),
  _mutex(ceph::make_mutex("daemon ping")),
  _ping_timer(cct, _mutex) {
}
DaemonPing::~DaemonPing() {
  _mutex.lock();
  _ping_timer.cancel_all_events();
  _ping_timer.shutdown();
  _mutex.unlock();
}

void DaemonPing::init(std::shared_ptr<Reactor> reactor) {
  _ping_timer.init();
  _reactor = reactor;
}

int DaemonPing::timer_ping() {
  _mutex.lock();
  _ping_timer.add_event_after(100, new C_Ping(shared_from_this()));
  _mutex.unlock();
  return 0;
}

int DaemonPing::free_caches() {
  update_cacheinfos();
  for (auto &id : need_free_caches) {
    if (infos.count(id) == 0) {
      freed_caches.insert(id);
      continue;
    }
    std::string cachefile_name("rbd-pwl." + infos[id].pool_name + "." + infos[id].image_name + ".pool." + std::to_string(infos[id].cache_id));
    std::string path = _cct->_conf->rwl_replica_path;
    fs::remove(path + "/" + cachefile_name);
    freed_caches.insert(id);
    infos.erase(id);
  }
  need_free_caches.clear();
  return 0;
}

int DaemonPing::single_ping() {
  ldout(_cct, 5) << dendl;
  cls::rbd::RwlCacheDaemonPing ping{_rados.get_instance_id(), freed_caches};
  bool has_need_free_cache{false};
  int r = rwlcache_daemonping(&_io_ctx, ping, has_need_free_cache);
  if (r < 0) {
    return r;
  }
  freed_caches.clear();

  if (has_need_free_cache) {
    cls::rbd::RwlCacheDaemonNeedFreeCaches caches;
    r = rwlcache_get_needfree_caches(&_io_ctx, _rados.get_instance_id(), caches);
    if (r < 0) {
      return r;
    }
    need_free_caches.swap(caches.need_free_caches);
  }
  return 0;
}

int DaemonPing::get_cache_info_from_filename(std::string filename, struct RwlCacheInfo& info) {
  info.cache_size = fs::file_size(filename);
  std::string::size_type found = filename.rfind('/');
  if (found != std::string::npos) {
    filename = filename.substr(found + 1);
  }
  if (filename.compare(0, 7, "rbd-pwl") != 0) {
    return -1;
  }

  // find pool_name
  found = filename.find('.');
  if (found == std::string::npos) {
    return -1;
  }
  info.pool_name = filename.substr(found + 1, filename.find('.', found + 1) - found - 1);

  // find image_name
  found = filename.find('.', found + 1);
  if (found == std::string::npos) {
    return -1;
  }
  info.image_name = filename.substr(found + 1, filename.find('.', found + 1) - found - 1);

  // find id
  found = filename.rfind('.');
  if (found == std::string::npos) {
    return -1;
  }
  info.cache_id = std::stoi(filename.substr(found + 1), nullptr, 10);
                                                                                                     
  return 0;
}

void DaemonPing::update_cacheinfos() {
  std::string path = _cct->_conf->rwl_replica_path;
  RwlCacheInfo info;
  //for (auto& p : fs::recursive_directory_iterator(".")) {           
  for (auto& p : fs::directory_iterator(path)) {           
    if (fs::is_regular_file(p.status())
        && !get_cache_info_from_filename(p.path(), info)
        && infos.count(info.cache_id) == 0) {
      infos.emplace(info.cache_id, std::move(info));
    }
  }
}

DaemonPing::C_Ping::C_Ping(std::shared_ptr<DaemonPing> dp) : dp(dp) {}
DaemonPing::C_Ping::~C_Ping() {}
void DaemonPing::C_Ping::finish(int r) {
  int ret = dp->single_ping();
  if (ret < 0) {
    if (dp->_reactor) {
      ldout(dp->_cct, 5) << "Error: shutdown in DaemonPing" << dendl;
      dp->_reactor->shutdown();
    }
    return;
  }
  
  // dp->_mutex.lock();
  dp->_ping_timer.add_event_after(100, new C_Ping(dp));
  // dp->_mutex.unlock();

  dp->free_caches();
}

}