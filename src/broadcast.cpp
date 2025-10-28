#include "broadcast.hpp"
#include "fused.hpp"

constexpr uint32_t kInFlight = 16; // own messages broadcast and not yet delivered

template <class B> void serve(Net &net, B &b) {
  Output out;
  b.on_deliver = [&](unsigned origin, std::string_view msg) { out.lines(origin, msg); };
  Loop::run(net, out, kBatch, [&] { return b.in_flight() < kInFlight; },
      [&](std::string_view batch) { b.broadcast(batch); });
}

int main(int argc, char **argv) {
  Options o = options(argc, argv, "usage: broadcast [--fused] [--loss P] ID HOST:PORT...");
  Net net(o);
  if (o.fused) {
    FusedBroadcast b(net);
    serve(net, b);
  } else {
    PerfectLinks pl(net);
    FifoBroadcast b(pl);
    serve(net, b);
  }
}
