#include "links.hpp"

int main(int argc, char **argv) {
  Options o = options(argc, argv, "usage: links [--loss P] ID HOST:PORT...");
  Net net(o);
  PerfectLinks pl(net);
  Output out;
  pl.on_deliver = [&](unsigned from, std::string_view msg) {
    out.lines(from, msg);
  };

  // batch-lines go to each destination as one msg
  auto send = [&](std::string_view batch) {
    std::vector<Bytes> to(net.n);
    for_each_line(batch, [&](std::string_view line) {
      size_t space = line.find(' ');
      unsigned long id = std::strtoul(Bytes(line.substr(0, space)).c_str(), nullptr, 10);
      if (space == std::string_view::npos || id < 1 || id > net.n) {
        std::fprintf(stderr, "lines are `ID text`, ID from 1 to %u\n", net.n);
        return;
      }
      to[id - 1] += line.substr(space + 1);
      to[id - 1] += '\n';
    });
    for (unsigned q = 0; q < net.n; q++)
      if (!to[q].empty())
        pl.send(q, std::move(to[q]));
  };

  Loop::run(net, out, kBatch, send);
}
