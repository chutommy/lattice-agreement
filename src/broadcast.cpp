#include "broadcast.hpp"

int main(int argc, char **argv) {
  Options o = options(argc, argv, "usage: broadcast [--loss P] ID HOST:PORT...");
  Net net(o);
  PerfectLinks pl(net);
  UniformReliableBroadcast b(pl);
  Output out;
  b.on_deliver = [&](unsigned origin, std::string_view msg) { out.lines(origin, msg); };
  Loop::run(net, out, kBatch, [&](std::string_view batch) { b.broadcast(batch); });
}
