#pragma once

// FIFO uniform reliable broadcast
// https://chutommy.com/dist-algorithms#fifo-uniform-broadcast

#include <map>
#include <unordered_map>
#include <vector>

#include "links.hpp"

// Best-effort broadcast: send to every process, this one included.
// https://chutommy.com/dist-algorithms#best-effort-broadcast
//
class BestEffortBroadcast {
public:
  PerfectLinks &pl;
  std::function<void(unsigned from, std::string_view msg)> on_deliver;

  explicit BestEffortBroadcast(PerfectLinks &pl) : pl(pl) {
    pl.on_deliver = [this](unsigned from, std::string_view msg) {
      on_deliver(from, msg);
    };
  }
  void broadcast(const Bytes &msg) {
    for (unsigned q = 0; q < pl.net.n; q++)
      pl.send(q, msg);
  }
};

// Uniform reliable broadcast: if any process delivers a message, every correct
// process does, given a correct majority. A process relays a message the first
// time it sees it and delivers it once more than half have relayed it: one of
// them is correct.
// https://chutommy.com/dist-algorithms#fifo-uniform-broadcast
//
class UniformReliableBroadcast {
public:
  std::function<void(unsigned origin, std::string_view msg)> on_deliver;

  explicit UniformReliableBroadcast(PerfectLinks &pl)
      : beb(pl), n(pl.net.n), self(pl.net.self), entries(n) {
    beb.on_deliver = [this](unsigned, std::string_view msg) { receive(msg); };
  }

  void broadcast(std::string_view msg) {
    uint32_t seq = next_seq++;
    entries[self][seq].relayed = true; // this broadcast is its relay
    Bytes d;
    put32(d, self);
    put32(d, seq);
    d += msg;
    beb.broadcast(d);
  }

private:
  struct Entry {
    unsigned copies = 0; // one from each process that relayed it
    bool relayed = false, delivered = false;
  };

  BestEffortBroadcast beb;
  unsigned n, self;
  uint32_t next_seq = 0;
  std::vector<std::unordered_map<uint32_t, Entry>> entries; // per origin

  void receive(std::string_view d) {
    Reader r{d};
    uint32_t origin = r.u32(), seq = r.u32();
    if (!r.ok || origin >= n)
      return;
    Entry &e = entries[origin][seq];
    if (!e.relayed) {
      e.relayed = true;
      beb.broadcast(Bytes(d));
    }
    if (++e.copies * 2 > n && !e.delivered) {
      e.delivered = true;
      on_deliver(origin, r.rest);
    }
  }
};

// FIFO order: messages from an origin are delivered in the order it broadcast.
class FifoBroadcast {
public:
  std::function<void(unsigned origin, std::string_view msg)> on_deliver;

  explicit FifoBroadcast(PerfectLinks &pl)
      : urb(pl), self(pl.net.self), origins(pl.net.n) {
    urb.on_deliver = [this](unsigned origin, std::string_view msg) {
      receive(origin, msg);
    };
  }

  void broadcast(std::string_view msg) {
    Bytes d;
    put32(d, next_seq++);
    d += msg;
    urb.broadcast(d);
  }

  // own messages broadcast, not yet delivered here
  uint32_t in_flight() const { return next_seq - origins[self].next; }

private:
  struct Origin {
    uint32_t next = 0;
    std::map<uint32_t, Bytes> early;
  };

  UniformReliableBroadcast urb;
  unsigned self;
  uint32_t next_seq = 0;
  std::vector<Origin> origins;

  void receive(unsigned origin, std::string_view d) {
    Reader r{d};
    uint32_t seq = r.u32();
    if (!r.ok)
      return;
    Origin &o = origins[origin];
    if (seq != o.next) {
      o.early.emplace(seq, r.rest);
      return;
    }
    on_deliver(origin, r.rest);
    o.next++;
    for (auto it = o.early.begin(); it != o.early.end() && it->first == o.next;
         it = o.early.erase(it)) {
      on_deliver(origin, it->second);
      o.next++;
    }
  }
};
