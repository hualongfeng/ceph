#include <inttypes.h>
#include <iostream>
#include "log.h"

#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"


int main(int argc, char* argv[]) {

  /* configure logging thresholds to see more details */
  rpma_log_set_threshold(RPMA_LOG_THRESHOLD, RPMA_LOG_LEVEL_INFO);
  rpma_log_set_threshold(RPMA_LOG_THRESHOLD_AUX, RPMA_LOG_LEVEL_INFO);

  char *addr = argv[1];
  char *port = argv[2];

  try {
    std::shared_ptr<Reactor> reactor = std::make_shared<Reactor>();
    std::shared_ptr<AcceptorHandler> rpma_acceptor = std::make_shared<AcceptorHandler>(addr, port, reactor);
    int ret = 0;
    if ((ret = rpma_acceptor->register_self())) {
      return ret;
    }
    while(true)
      reactor->handle_events();
  } catch (std::runtime_error &e) {
    std::cout << __FILE__ << ":" << __LINE__ << " Runtime error: " << e.what() << std::endl;
  }
  // NOT REACHED
  return 0;
}
