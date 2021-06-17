#include <inttypes.h>
#include <iostream>
#include <string>
#include <thread>
#include <fstream>
#include <vector>

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

#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd.h"
#include "librbd/Types.h"

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


  int r = 0;
  librados::Rados rados;
  librados::IoCtx io_ctx;
  std::string poolname = g_conf().get_val<std::string>("rbd_persistent_replicated_cache_cls_pool");
  cls::rbd::RwlCacheDaemonInfo d_info;

  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    return -r;
  }

  r = rados.connect();
  if (r < 0) {
    std::cerr << "rwl-replica: failed to connect to cluster: " << cpp_strerror(r) << std::endl;
    return -r;
  }

  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0) {
    std::cerr << "rwl-replica: failed to access pool " << poolname << ": "
              << cpp_strerror(r) << std::endl;
    return -r;
  }

  cls::rbd::RwlCacheRequest req = {
    rados.get_instance_id(),
    10UL * 1024 * 1024 * 1024,
    3
  };
  epoch_t cache_id;

  r = rwlcache_request(&io_ctx, req, cache_id);
  if (r != 0) {
    std::cout << "rwlcache_request: " << r << cpp_strerror(r) << std::endl;
    return -r;
  }

  cls::rbd::RwlCacheInfo info;
  r = rwlcache_get_cacheinfo(&io_ctx, cache_id, info);
  if (r != 0) {
    std::cout << "rwlcache_get_cacheinfo: " << r << cpp_strerror(r) << std::endl;
    return -r;
  }

  assert(info.cache_id == cache_id);

  for (auto &daemon : info.daemons) {
    std::cout << "id: " << daemon.id << ", "
              << "rdma_address: " << daemon.rdma_address << ", "
              << "rdma_port: " << daemon.rdma_port
              << std::endl;
  }


  std::shared_ptr<ClientHandler> rpma_client;
  using ClientHandlerPtr = std::shared_ptr<ClientHandler>;
  std::vector<ClientHandlerPtr> clients;

  int ret = 0;

  try {
    reactor = std::make_shared<Reactor>(g_ceph_context);
    for (auto &daemon : info.daemons) {
      std::string ip = daemon.rdma_address;
      std::string port = std::to_string(daemon.rdma_port);
      rpma_client = std::make_shared<ClientHandler>(g_ceph_context, ip, port, reactor);
      if ((ret = rpma_client->register_self())) {
        std::cout << "ret :" << ret << std::endl;
        return ret;
      }
      clients.push_back(rpma_client);
    }
  } catch (std::runtime_error &e) {
    std::cout << __FILE__ << ":" << __LINE__ << " Runtime error: " << e.what() << std::endl;
  }

  cls::rbd::RwlCacheRequestAck ack = {cache_id, 0};
  r = rwlcache_request_ack(&io_ctx, ack);
  if (r != 0) {
    std::cout << "rwlcache_request_ack: " << r << cpp_strerror(r) << std::endl;
    return -r;
  }

  std::thread th1([]{
      while (reactor) {
        reactor->handle_events();
      }
      std::cout << "stoped to handle events" << std::endl;
  });
  //  th1.join();
  th1.detach();

  for (auto &client : clients) {
    client->wait_established();
  }
  // rpma_client->wait_established();

  for (auto &client : clients) {
    // std::cout << "init_replica: " << rpma_client->init_replica(1, REQUIRE_SIZE, "RBD", "test") << std::endl;
    std::cout << "init_replica: " << client->init_replica(1, REQUIRE_SIZE, "RBD", "test") << std::endl;
  }
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
  std::cout << "---------------set_head----------------------" << std::endl;

  for (auto &client : clients) {
    // rpma_client->set_head(mr_ptr, mr_size);
    client->set_head(mr_ptr, mr_size);
  }


  std::cout << "----------------write---------------------" << std::endl;

  for (auto &client : clients) {
    std::atomic<bool> completed{false};
    // rpma_client->write(0, mr_size, [&completed]{
    client->write(0, mr_size, [&completed]{
      completed = true;
    });
    while(completed == false);
  }

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

  std::cout << "---------------flush----------------------" << std::endl;
  for (auto &client : clients) {
    std::atomic<bool> completed{false};
    // rpma_client->flush(0, mr_size, [&completed]{
    client->flush(0, mr_size, [&completed]{
      completed = true;
    });
    while(completed == false);
  }

  myfile.open("/mnt/pmem/rbd-pwl.RBD.test.pool.1");
  if (myfile.is_open()) {
    for (int i = 0; i < 10; i++) {
      getline(myfile, line);
      std::cout << line << std::endl;
    }
    myfile.close();
  }
  std::cout << std::endl;


  std::cout << "----------------close_replica---------------------" << std::endl;

  for (auto &client : clients) {
  // std::cout << "close_replica: " << rpma_client->close_replica() << std::endl;
    std::cout << "close_replica: " << client->close_replica() << std::endl;
  }
  std::cout << "------------------disconnect-------------------" << std::endl;

  for (auto &client : clients) {
    // std::cout << "close: "  << rpma_client->disconnect() << std::endl;
    std::cout << "close: "  << client->disconnect() << std::endl;
  }
  std::cout << "-------------cachefree------------------------" << std::endl;


  cls::rbd::RwlCacheFree free = {cache_id};
  r = rwlcache_free(&io_ctx, free);
  if (r != 0) {
    std::cout << "rwlcache_free: " << r << cpp_strerror(r) << std::endl;
    return -r;
  }

  std::cout << "-------------cleanup------------------------" << std::endl;

//  cleanup:
  reactor.reset();
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  return ret != 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
