#pragma once

// PERFECT LINKS
// a message sent to a correct process is delivered there exactly once.
// https://chutommy.com/dist-algorithms#perfect-links
//
// Messages are numbered per destination and retransmitted until acknowledged.

#include <map>
#include <unordered_set>
#include <vector>

#include "net.hpp"

class PerfectLinks {
public:
  static constexpr Clock::duration kTimeout = 50ms;

  Net &net;
  std::function<void(unsigned from, std::string_view msg)> on_deliver;

  explicit PerfectLinks(Net &net) : net(net), out(net.n), delivered(net.n) {
    net.on_receive = [this](std::string_view d) { receive(d); };
    net.every(10ms, [this] { retransmit_late(); });
  }

  void send(unsigned to, Bytes msg) {
    Link &l = out[to];
    uint32_t seq = l.next++;
    Message &m = l.unacked[seq];
    m.data = std::move(msg);
    transmit(to, seq, m);
  }

private:
  enum : uint8_t { DATA, ACK };

  struct Message {
    Bytes data;
    Clock::time_point at{}; // last transmission
  };
  struct Link {
    uint32_t next = 0;
    std::map<uint32_t, Message> unacked;
  };

  std::vector<Link> out;
  std::vector<std::unordered_set<uint32_t>> delivered;

  static Bytes header(uint8_t kind, unsigned from, uint32_t seq) {
    Bytes d;
    put8(d, kind);
    put32(d, from);
    put32(d, seq);
    return d;
  }

  void transmit(unsigned to, uint32_t seq, Message &m) {
    m.at = Clock::now();
    net.send(to, header(DATA, net.self, seq) + m.data);
  }

  void retransmit_late() {
    Clock::time_point now = Clock::now();
    for (unsigned to = 0; to < net.n; to++)
      for (auto &[seq, m] : out[to].unacked)
        if (now - m.at >= kTimeout)
          transmit(to, seq, m);
  }

  void receive(std::string_view d) {
    Reader r{d};
    uint8_t kind = r.u8();
    uint32_t from = r.u32(), seq = r.u32();
    if (!r.ok || from >= net.n)
      return;
    if (kind == ACK) {
      out[from].unacked.erase(seq);
      return;
    }
    net.send(from, header(ACK, net.self, seq));
    if (delivered[from].insert(seq).second)
      on_deliver(from, r.rest);
  }
};
