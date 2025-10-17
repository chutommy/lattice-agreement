#include "links.hpp"

int main(int argc, char **argv) {
  Options o = options(argc, argv, "usage: links ID HOST:PORT...");
  Net net(o);
  PerfectLinks pl(net);
  Output out;
  pl.on_deliver = [&](unsigned from, std::string_view msg) {
    out.line(std::to_string(from + 1) + " " + Bytes(msg));
  };

  // a line `ID text` sends `text` to process `ID`
  auto send = [&](std::string_view line) {
    size_t space = line.find(' ');
    unsigned long id = std::strtoul(Bytes(line.substr(0, space)).c_str(), nullptr, 10);
    if (space == std::string_view::npos || id < 1 || id > net.n) {
      std::fprintf(stderr, "lines are `ID text`, ID from 1 to %u\n", net.n);
      return;
    }
    pl.send(static_cast<unsigned>(id - 1), Bytes(line.substr(space + 1)));
  };

  Loop::run(net, out, send);
}
