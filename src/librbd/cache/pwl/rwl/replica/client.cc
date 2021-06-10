#include <inttypes.h>
#include "librpma.h"
#include <iostream>
#include <assert.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <libpmem.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <stddef.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <memory>
#include <unordered_map>
#include <thread>
#include <fstream>
#include "log.h"

#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"
#include "Types.h"


int main(int argc, char* argv[]) {

  /* configure logging thresholds to see more details */
  rpma_log_set_threshold(RPMA_LOG_THRESHOLD, RPMA_LOG_LEVEL_INFO);
  rpma_log_set_threshold(RPMA_LOG_THRESHOLD_AUX, RPMA_LOG_LEVEL_INFO);

  char *addr = argv[1];
  char *port = argv[2];
  std::string basename("temporary");
  std::shared_ptr<Reactor> reactor;
  std::shared_ptr<ClientHandler> rpma_client;

  int ret = 0;

  try {
    reactor = std::make_shared<Reactor>();
    rpma_client = std::make_shared<ClientHandler>(addr, port, reactor);
    if ((ret = rpma_client->register_self())) {
      std::cout << "ret :" << ret << std::endl;
      return ret;
    }
  } catch (std::runtime_error &e) {
    std::cout << __FILE__ << ":" << __LINE__ << " Runtime error: " << e.what() << std::endl;
  }

  std::thread th1([reactor]{
      while (true) {
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

  // rpma_client->prepare_for_send();
  // rpma_client->send(nullptr);
  // std::atomic<bool> completed = false;
  // rpma_client->recv([&rpma_client, &completed] () mutable {
  //   rpma_client->get_remote_descriptor();
  //   completed = true;
  // });

  // while (completed == false);

  // // NOT REACHED
  // while (true);
  th1.join();
  return 0;
}
