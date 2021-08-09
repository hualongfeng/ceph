#include <inttypes.h>
#include <iostream>
#include <vector>

#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"
#include "TimerPing.h"

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"

#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd.h"
#include "librbd/Types.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::Main: "

using namespace librbd::cache::pwl::rwl::replica;
using namespace librbd::cls_client;

void usage() {
  std::cout << "usage: ceph-rwl-replica-server [options...]\n";
  std::cout << "options:\n";
  std::cout << "  -m monaddress[:port]                 connect to specified monitor\n";
  std::cout << "  --keyring=<path>                     path to keyring for local"
            << " cluster\n";
  std::cout << "  --log-file=<logfile>                 file to log debug output\n";
  std::cout << "  --debug-rwl-replica=<log-level>/<memory-level>"
            << " set debug level\n";
  generic_server_usage();
}

std::shared_ptr<Reactor> reactor;

static void handle_signal(int signum) {
  if (reactor) {
    reactor->shutdown();
  }
  return ;
}


int main(int argc, const char* argv[]) {

  std::vector<const char*> args;
  env_to_vec(args);
  argv_to_vec(argc, argv, args);

  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  int flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
  // Prevent global_inti() from dropping permissions until frontends can bind
  // privileged ports
  flags |= CINIT_FLAG_DEFER_DROP_PRIVILEGES;

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON,
                         CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }

  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  /* configure logging thresholds to see more details */
  rpma_log_set_threshold(RPMA_LOG_THRESHOLD, RPMA_LOG_LEVEL_INFO);
  rpma_log_set_threshold(RPMA_LOG_THRESHOLD_AUX, RPMA_LOG_LEVEL_INFO);

  std::string replica_addr = g_conf().get_val<std::string>("rwl_replica_addr");
  auto pos         = replica_addr.find(":");
  std::string ip   = replica_addr.substr(0, pos);
  std::string port = replica_addr.substr(pos + 1);

  int r = 0;
  librados::Rados rados;
  librados::IoCtx io_ctx;
  std::string poolname = g_conf().get_val<std::string>("rbd_persistent_replicated_cache_cls_pool");
  cls::rbd::RwlCacheDaemonInfo d_info;
  std::shared_ptr<DaemonPing> dp = std::make_shared<DaemonPing>(g_ceph_context, rados, io_ctx);

  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    goto cleanup;
  }

  r = rados.connect();
  if (r < 0) {
    std::cerr << "rwl-replica: failed to connect to cluster: " << cpp_strerror(r) << std::endl;
    goto cleanup;
  }

  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0) {
    std::cerr << "rwl-replica: failed to access pool " << poolname << ": "
              << cpp_strerror(r) << std::endl;
    goto cleanup;
  }

  ldout(g_ceph_context, 20) << "addr: " << g_ceph_context->_conf->rwl_replica_addr << dendl;
  ldout(g_ceph_context, 20) << "size: " << g_ceph_context->_conf->rwl_replica_size << dendl;
  ldout(g_ceph_context, 20) << "path: " << g_ceph_context->_conf->rwl_replica_path << dendl;

  d_info.id = rados.get_instance_id();
  d_info.rdma_address = ip;
  d_info.rdma_port = stoi(port, nullptr, 10);
  d_info.total_size = g_ceph_context->_conf->rwl_replica_size;
  ldout(g_ceph_context, 20) << "Start:\n"
                            << "id: " << d_info.id << "\n"
                            << "rdma_address: " << d_info.rdma_address << "\n"
                            << "rdma_port: " << d_info.rdma_port << "\n"
                            << "total_size: " << d_info.total_size << "\n"
                            << dendl;

  r = rwlcache_daemoninfo(&io_ctx, d_info);
  std::cout << "rwlcache_daemoninfo: " << r << std::endl;


  try {
    reactor = std::make_shared<Reactor>(g_ceph_context);
    std::shared_ptr<AcceptorHandler> rpma_acceptor = std::make_shared<AcceptorHandler>(g_ceph_context, ip, port, reactor);
    if ((r = rpma_acceptor->register_self())) {
      goto cleanup;
    }
    dp->init(reactor);
    dp->timer_ping();
    reactor->handle_events();
  } catch (std::runtime_error &e) {
    std::cout << __FILE__ << ":" << __LINE__ << " Runtime error: " << e.what() << std::endl;
  }

 cleanup:
  reactor.reset();
  rados.shutdown();
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  return r != 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
