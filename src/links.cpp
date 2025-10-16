// Fair-loss links: a line `ID text` sends `text` to process `ID` in one datagram.

#include "net.hpp"

int main(int argc, char **argv) {
  Options o = options(argc, argv, "usage: links ID HOST:PORT...");
  Net net(o);
  Output out;
  net.on_receive = [&](std::string_view d) {
    Reader r{d};
    uint32_t from = r.u32();
    if (!r.ok || from >= net.n)
      return;
    out.line(std::to_string(from + 1) + " " + Bytes(r.rest));
  };

  auto send = [&](std::string_view line) {
    size_t space = line.find(' ');
    unsigned long id = std::strtoul(Bytes(line.substr(0, space)).c_str(), nullptr, 10);
    if (space == std::string_view::npos || id < 1 || id > net.n) {
      std::fprintf(stderr, "lines are `ID text`, ID from 1 to %u\n", net.n);
      return;
    }
    Bytes d;
    put32(d, net.self);
    d += line.substr(space + 1);
    net.send(static_cast<unsigned>(id - 1), d);
  };

  Loop::run(net, out, send);
}
