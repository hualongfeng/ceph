#include <inttypes.h>
#include <iostream>
#include <string>
#include <libpmem.h>

#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"
#include "Types.h"
#include "RpmaOp.h"
#include "ReplicaClient.h"

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"

using namespace librbd::cache::pwl::rwl::replica;

void usage() {
  std::cout << "usage: ceph-rwl-replica-client [options...]\n";
  std::cout << "options:\n";
  std::cout << "  -m monaddress[:port]                 connect to specified monitor\n";
  std::cout << "  --keyring=<path>                     path to keyring for local"
            << " cluster\n";
  std::cout << "  --log-file=<logfile>                 file to log debug output\n";
  std::cout << "  --debug-rwl-replica=<log-level>/<memory-level>"
            << " set debug level\n";
  generic_server_usage();
}

// std::shared_ptr<Reactor> reactor;

static void handle_signal(int signum) {
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

  int r = 0;

  librados::Rados rados;
  r = rados.init_with_context(g_ceph_context);
  r |= rados.connect();
  if (r < 0) {
    std::cout << "failed to connect to cluster: " << cpp_strerror(r) << std::endl;
    exit(1);
  }
  librados::IoCtx io_ctx;
  r = rados.ioctx_create("rbd", io_ctx);
  if (r < 0) {
    std::cout << "failed to access pool "<< cpp_strerror(r) << std::endl;
    exit(1);
  }
  std::cout << rados.get_instance_id() << std::endl;

  auto &copies = g_ceph_context->_conf->rwl_replica_copies;
  // std::cout << "before copies: " << copies << std::endl;
  // copies = 4;
  // auto &copies1 = g_ceph_context->_conf->rwl_replica_copies;
  // std::cout << "after copies: " << copies1 << std::endl;
  // return 0;
  ReplicaClient replica_client(g_ceph_context, REQUIRE_SIZE, copies, "RBD", "test", io_ctx);

  r = replica_client.init_ioctx();
  if (r < 0) {
    std::cerr << "rwl-replica: failed to access pool: "
              << cpp_strerror(r) << std::endl;
    return -r;
  }
  r = replica_client.cache_request();
  if (r != 0) {
    std::cout << "cache_request: " << r << cpp_strerror(r) << std::endl;
    return -r;
  }

  std::cout << "---------------data prepare----------------------" << std::endl;

  // data prepare
  bufferlist bl;
  size_t mr_size = 1024 * 1024 * 128; //max size, can't extend 1G byte
  bl.append(bufferptr(mr_size));
  bl.rebuild_page_aligned();
  void* mr_ptr = bl.c_str();;
  if (mr_ptr == NULL) return -1;
  char data[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-\n";
  size_t data_size = strlen(data);
  for(size_t i = 0; i < mr_size; i+=data_size) {
    //memcpy((char*)mr_ptr + i, data, data_size);
    pmem_memcpy_nodrain((char*)mr_ptr + i, data, data_size);
  }
  r == 0 && std::cout << "---------------set_head--------------------------" << std::endl;
  r == 0 && replica_client.set_head(mr_ptr, mr_size);

  r == 0 && std::cout << "---------------write-----------------------------" << std::endl;
  int cnt = 0;
  size_t write_size = 4096;
  for (size_t i = 0; i < mr_size; i+=write_size) {
    cnt++;
    replica_client.write(i, i+write_size);
    // std::cout << (r = replica_client.write(i, i+data_size)) << std::endl;
    if(cnt % 20 == 0) {
      replica_client.flush(i, i+write_size);
      // std::cout << (r = replica_client.flush(0, mr_size)) << std::endl;
    }
  }
  // r == 0 && std::cout << "---------------flush-----------------------------" << std::endl;
  r == 0 && std::cout << "---------------close_replica---------------------" << std::endl;
  //r == 0 && replica_client.replica_close();
  r == 0 && std::cout << "---------------disconnect------------------------" << std::endl;
  replica_client.disconnect();
  std::cout << "---------------cachefree-------------------------" << std::endl;
  std::cout << replica_client.cache_free() << std::endl;
  std::cout << "---------------cleanup---------------------------" << std::endl;

  // replica_client.shutdown();

  // std::cout << "---------------cleanup---------------------------" << std::endl;
//  cleanup:
  // reactor.reset();
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  return r != 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
