#include <inttypes.h>
#include <iostream>
#include <string>
#include <thread>
#include <fstream>

#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"
#include "Types.h"
#include "RpmaOp.h"

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"

using namespace ceph::librbd::cache::pwl::rwl::replica;

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

  std::string basename("temporary");
  // std::shared_ptr<Reactor> reactor;
  std::shared_ptr<ClientHandler> rpma_client;

  int ret = 0;

  try {
    reactor = std::make_shared<Reactor>(g_ceph_context);
    rpma_client = std::make_shared<ClientHandler>(g_ceph_context, ip, port, reactor);
    if ((ret = rpma_client->register_self())) {
      std::cout << "ret :" << ret << std::endl;
      return ret;
    }
  } catch (std::runtime_error &e) {
    std::cout << __FILE__ << ":" << __LINE__ << " Runtime error: " << e.what() << std::endl;
  }

  std::thread th1([]{
      while (reactor) {
        reactor->handle_events();
        if (reactor->empty()) {
          std::cout << "My event_table is empty!!!" << std::endl;
          break;
        }
      }
  });

  rpma_client->wait_established();

  std::cout << "init_replica: " << rpma_client->init_replica(1, REQUIRE_SIZE, "RBD", "test") << std::endl;
  std::cout << "-------------------------------------" << std::endl;

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
  std::cout << "-------------------------------------" << std::endl;

  rpma_client->set_head(mr_ptr, mr_size);
  std::atomic<bool> completed{false};
  rpma_client->write(0, mr_size, [&completed]{
    completed = true;
  });
  while(completed == false);

  std::cout << "-------------------------------------" << std::endl;
  std::string line;
  std::ifstream myfile("/mnt/pmem/rbd-pwl.RBD.test.pool.1");
  if (myfile.is_open()) {
    for (int i = 0; i < 10; i++) {
      getline(myfile, line);
      std::cout << line << std::endl;
    }
    myfile.close();
  }
  std::cout << std::endl;

  completed = false;
  std::cout << "-------------------------------------" << std::endl;
  rpma_client->flush(0, mr_size, [&completed]{
    completed = true;
  });
  while(completed == false);

  myfile.open("/mnt/pmem/rbd-pwl.RBD.test.pool.1");
  if (myfile.is_open()) {
    for (int i = 0; i < 10; i++) {
      getline(myfile, line);
      std::cout << line << std::endl;
    }
    myfile.close();
  }
  std::cout << std::endl;


  std::cout << "-------------------------------------" << std::endl;

  // std::cout << "close_replica: " << rpma_client->close_replica() << std::endl;
  std::cout << "-------------------------------------" << std::endl;

  std::cout << "close: "  << rpma_client->disconnect() << std::endl;
  std::cout << "-------------------------------------" << std::endl;


  th1.join();

//  cleanup:
  reactor.reset();
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  return ret != 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
