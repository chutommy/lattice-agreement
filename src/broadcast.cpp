#include "broadcast.hpp"

constexpr uint32_t kInFlight = 16; // own messages broadcast and not yet delivered

int main(int argc, char **argv) {
  Options o = options(argc, argv, "usage: broadcast [--loss P] ID HOST:PORT...");
  Net net(o);
  PerfectLinks pl(net);
  FifoBroadcast b(pl);
  Output out;
  b.on_deliver = [&](unsigned origin, std::string_view msg) { out.lines(origin, msg); };
  Loop::run(net, out, kBatch, [&] { return b.in_flight() < kInFlight; },
      [&](std::string_view batch) { b.broadcast(batch); });
}
