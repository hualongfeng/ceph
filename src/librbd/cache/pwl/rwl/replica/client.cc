#include <inttypes.h>
#include <iostream>
#include <string>

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

  int r = 0;
  ReplicaClient replica_client(g_ceph_context, REQUIRE_SIZE, 3);
  r = replica_client.init_rados();
  if (r < 0) {
    std::cerr << "rwl-replica: failed to connect to cluster: " << cpp_strerror(r) << std::endl;
    return -r;
  }
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
  r = replica_client.replica_init("RBD", "test");
  if (r != 0) {
    std::cout << "replica init failed: " << r << std::endl;
  }

  std::cout << "---------------data prepare----------------------" << std::endl;

  // data prepare
  bufferlist bl;
  size_t mr_size = 1024 * 1024 * 1024; //max size, can't extend 1G byte
  bl.append(bufferptr(mr_size));
  bl.rebuild_page_aligned();
  void* mr_ptr = bl.c_str();;
  if (mr_ptr == NULL) return -1;
  char data[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-\n";
  size_t data_size = strlen(data);
  for(size_t i = 0; i < mr_size; i+=data_size) {
    memcpy((char*)mr_ptr + i, data, data_size);
  }
  std::cout << "---------------set_head--------------------------" << std::endl;
  replica_client.set_head(mr_ptr, mr_size);

  std::cout << "---------------write-----------------------------" << std::endl;
  replica_client.write(0, mr_size);
  std::cout << "---------------flush-----------------------------" << std::endl;
  replica_client.flush(0, mr_size);
  std::cout << "---------------close_replica---------------------" << std::endl;
  // replica_client.replica_close();
  std::cout << "---------------disconnect------------------------" << std::endl;
  replica_client.disconnect();
  std::cout << "---------------cachefree-------------------------" << std::endl;
  replica_client.disconnect();
  std::cout << "---------------cleanup---------------------------" << std::endl;

//  cleanup:
  reactor.reset();
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  return r != 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
