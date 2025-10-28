#pragma once

// FIFO uniform reliable broadcast in a single layer over UDP.
// https://chutommy.com/dist-algorithms#fifo-uniform-broadcast
//
// Every copy of a message carries the set of processes known to have it: a
// receiver adds itself, merges the set with its own and answers. A message is
// delivered once more than half have it, and retransmitted to those missing
// until everyone has it.

#include <deque>
#include <optional>
#include <vector>

#include "net.hpp"

class AckSet {
public:
  explicit AckSet(unsigned n) : bits((n + 7) / 8, '\0') {}
  AckSet(unsigned n, std::string_view raw) : bits(raw.substr(0, (n + 7) / 8)) {}

  bool has(unsigned i) const { return static_cast<uint8_t>(bits[i / 8]) >> (i % 8) & 1; }
  void add(unsigned i) { bits[i / 8] = static_cast<char>(bits[i / 8] | 1 << (i % 8)); }
  void merge(const AckSet &other) {
    for (size_t i = 0; i < bits.size(); i++)
      bits[i] = static_cast<char>(bits[i] | other.bits[i]);
  }
  unsigned count() const {
    unsigned c = 0;
    for (char b : bits)
      c += static_cast<unsigned>(__builtin_popcount(static_cast<uint8_t>(b)));
    return c;
  }
  const Bytes &raw() const { return bits; }

private:
  Bytes bits;
};

class FusedBroadcast {
public:
  static constexpr Clock::duration kRetransmit = 20ms;

  std::function<void(unsigned origin, std::string_view msg)> on_deliver;

  explicit FusedBroadcast(Net &net) : net(net), n(net.n), self(net.self), origins(n) {
    net.on_receive = [this](std::string_view d) { receive(d); };
    net.every(kRetransmit, [this] { retransmit(); });
  }

  void broadcast(std::string_view msg) {
    Copy c{2, self, next_seq++, AckSet(n), Bytes(msg)};
    c.acks.add(self);
    pending++;
    store_and_deliver(c, false);
    send_missing(c);
  }

  // Own messages broadcast and not yet delivered here.
  uint32_t in_flight() const { return pending; }

private:
  // ttl 2: sent by origin, to relay
  // ttl 1: stored or relayed
  // ttl 0: an answer
  struct Copy {
    int ttl;
    uint32_t origin, seq;
    AckSet acks;
    Bytes msg;
  };
  // copies[i] is message i, below `next` delivered.
  struct Origin {
    uint32_t next = 0;
    std::deque<std::optional<Copy>> copies;
  };

  Net &net;
  unsigned n, self;
  uint32_t next_seq = 0, pending = 0;
  std::vector<Origin> origins;

  Bytes encode(const Copy &c) const {
    Bytes d;
    put32(d, self);
    put8(d, static_cast<uint8_t>(std::max(c.ttl, 0)));
    put32(d, c.origin);
    put32(d, c.seq);
    return d + c.acks.raw() + c.msg;
  }

  void send_missing(const Copy &c) {
    Bytes d = encode(c);
    for (unsigned q = 0; q < n; q++)
      if (!c.acks.has(q))
        net.send(q, d);
  }

  void retransmit() {
    for (const Origin &o : origins)
      for (const auto &c : o.copies)
        if (c)
          send_missing(*c);
  }

  void receive(std::string_view d) {
    Reader r{d};
    uint32_t from = r.u32();
    int ttl = r.u8() - 1;
    uint32_t origin = r.u32(), seq = r.u32();
    std::string_view acks = r.take((n + 7) / 8);
    if (!r.ok || from >= n || origin >= n)
      return;
    Copy c{ttl, origin, seq, AckSet(n, acks), Bytes(r.rest)};
    if (c.acks.has(self)) { // the sender knows this process has it: only learn who else does
      store_and_deliver(c, false);
      return;
    }
    c.acks.add(self);
    store_and_deliver(c, true);
    net.send(from, encode(c)); // answer at once instead of at the next retransmission
    if (c.ttl > 0)
      send_missing(c);
  }

  // Stores the copy or merges its set into the stored one (with `sync`, also back into `c`),
  // then delivers what a majority has.
  void store_and_deliver(Copy &c, bool sync) {
    Origin &o = origins[c.origin];
    size_t i = c.seq;
    if (i >= o.copies.size())
      o.copies.resize(i + 1);
    std::optional<Copy> &stored = o.copies[i];
    if (stored) {
      stored->acks.merge(c.acks);
    } else {
      stored = c;
      stored->ttl = 1;
    }
    if (sync)
      c.acks.merge(stored->acks);

    while (o.next < o.copies.size()) {
      const std::optional<Copy> &next = o.copies[o.next];
      if (!next || next->acks.count() * 2 <= n)
        break;
      o.next++;
      on_deliver(next->origin, next->msg);
      if (next->origin == self) { // tell everyone who has it
        pending--;
        Bytes d = encode(*next);
        for (unsigned q = 0; q < n; q++)
          if (q != self)
            net.send(q, d);
      }
    }
  }
};
