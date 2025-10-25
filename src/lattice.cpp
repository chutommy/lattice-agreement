// Lattice agreement. Each input line is a proposal, a set of words; the i-th lines of
// all processes form one instance. Each decision is printed as a line, in order.

#include "lattice.hpp"

constexpr uint32_t kInFlight = 16; // own proposals not yet decided

int main(int argc, char **argv) {
  Options o = options(argc, argv, "usage: lattice [--loss P] ID HOST:PORT...");
  Net net(o);
  PerfectLinks pl(net);
  LatticeAgreement la(pl);
  Output out;
  la.on_decide = [&](const Set &decision) {
    Bytes line;
    for (const std::string &v : decision)
      line += (line.empty() ? "" : " ") + v;
    out.line(line);
  };

  auto propose = [&](std::string_view line) {
    Set value;
    size_t start = 0;
    while ((start = line.find_first_not_of(" \n", start)) != std::string_view::npos) {
      size_t end = std::min(line.find_first_of(" \n", start), line.size());
      value.emplace_back(line.substr(start, end - start));
      start = end;
    }
    std::sort(value.begin(), value.end());
    value.erase(std::unique(value.begin(), value.end()), value.end());
    la.propose(std::move(value));
  };

  Loop::run(net, out, 1, [&] { return la.in_flight() < kInFlight; }, propose);
}
