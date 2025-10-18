#pragma once

// PERFECT LINKS
// a message sent to a correct process is delivered there exactly once.
// https://chutommy.com/dist-algorithms#perfect-links
//
// Messages are numbered per destination, acknowledged one by one and
// retransmitted after a timeout. At most kWindow are unacknowledged.

#include <algorithm>
#include <cassert>
#include <deque>
#include <vector>

#include "net.hpp"

// circular window of flags starting at base + offset, everything else checked.
class SlidingWindow {
public:
  explicit SlidingWindow(unsigned size) : flags(size, false), size(size) {}

  bool was_checked(unsigned i) const {
    return i < base + offset || (i < base + offset + size && flags[i % size]);
  }
  bool can_check(unsigned i) const {
    return i >= base + offset && i < base + offset + size && !flags[i % size];
  }
  void check(unsigned i) {
    assert(can_check(i));
    flags[i % size] = true;
    while (flags[offset]) {
      flags[offset] = false;
      if (++offset == size) {
        offset = 0;
        base += size;
      }
    }
  }

private:
  std::vector<bool> flags;
  unsigned size;
  unsigned base = 0; // a multiple of size
  unsigned offset = 0;
};

class PerfectLinks {
public:
  static constexpr uint32_t kWindow = 64;
  static constexpr Clock::duration kTimeout = 50ms;

  Net &net;
  std::function<void(unsigned from, std::string_view msg)> on_deliver;

  explicit PerfectLinks(Net &net)
      : net(net), out(net.n), in(net.n, SlidingWindow(kWindow)) {
    net.on_receive = [this](std::string_view d) { receive(d); };
    net.every(10ms, [this] { retransmit_late(); });
  }

  void send(unsigned to, Bytes msg) {
    out[to].queue.push_back({std::move(msg)});
    transmit_window(to);
  }

private:
  enum : uint8_t { DATA, ACK };

  struct Message {
    Bytes data;
    Clock::time_point at{}; // last transmission
    bool acked = false;
  };
  // queue[i] is message base + i; messages below `sent` have been transmitted.
  struct Link {
    uint32_t base = 0, sent = 0;
    std::deque<Message> queue;
  };

  std::vector<Link> out;
  std::vector<SlidingWindow> in;

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

  void transmit_window(unsigned to) {
    Link &l = out[to];
    while (l.sent < l.base + kWindow && l.sent - l.base < l.queue.size()) {
      transmit(to, l.sent, l.queue[l.sent - l.base]);
      l.sent++;
    }
  }

  void retransmit_late() {
    Clock::time_point now = Clock::now();
    for (unsigned to = 0; to < net.n; to++) {
      Link &l = out[to];
      for (uint32_t seq = l.base; seq < l.sent; seq++) {
        Message &m = l.queue[seq - l.base];
        if (!m.acked && now - m.at >= kTimeout)
          transmit(to, seq, m);
      }
    }
  }

  void receive(std::string_view d) {
    Reader r{d};
    uint8_t kind = r.u8();
    uint32_t from = r.u32(), seq = r.u32();
    if (!r.ok || from >= net.n)
      return;
    if (kind == ACK) {
      acknowledged(from, seq);
      return;
    }
    SlidingWindow &w = in[from];
    bool fresh = w.can_check(seq);
    if (fresh || w.was_checked(seq))
      net.send(from, header(ACK, net.self, seq));
    if (fresh) {
      w.check(seq);
      on_deliver(from, r.rest);
    }
  }

  void acknowledged(unsigned to, uint32_t seq) {
    Link &l = out[to];
    if (seq < l.base || seq >= l.sent || l.queue[seq - l.base].acked)
      return;
    Message &acked = l.queue[seq - l.base];
    acked.acked = true;
    acked.data.clear();
    while (!l.queue.empty() && l.queue.front().acked) {
      l.queue.pop_front();
      l.base++;
    }
    transmit_window(to);
  }
};
