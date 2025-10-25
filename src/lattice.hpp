#pragma once

// Lattice agreement over perfect links, one instance per proposal.
// https://chutommy.com/dist-algorithms#lattice-agreement
//
// An acceptor acknowledges a proposal that contains what it has accepted, and
// otherwise refuses, sending back the union. Once more than half have answered,
// the proposer decides if all acknowledged, and otherwise proposes again with
// everything it was sent.

#include <algorithm>
#include <deque>
#include <iterator>
#include <unordered_map>
#include <vector>

#include "links.hpp"

using Set = std::vector<std::string>; // sorted, without duplicates

inline Set merge(const Set &a, const Set &b) {
  Set u;
  u.reserve(a.size() + b.size());
  std::set_union(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(u));
  return u;
}

class LatticeAgreement {
public:
  std::function<void(const Set &decision)> on_decide; // in instance order

  explicit LatticeAgreement(PerfectLinks &pl) : pl(pl), n(pl.net.n) {
    pl.on_deliver = [this](unsigned from, std::string_view msg) {
      receive(from, msg);
    };
  }

  void propose(Set value) {
    uint32_t instance = next_instance++;
    proposals.push_back({std::move(value)});
    send_proposal(instance, proposals.back());
  }

  // own proposals not yet decided
  uint32_t in_flight() const { return next_instance - next_decision; }

private:
  enum : uint8_t { PROPOSE, ACK, NACK };

  struct Proposal {
    Set value;
    uint32_t number = 0;
    unsigned acks = 0, nacks = 0;
    bool decided = false;
  };

  PerfectLinks &pl;
  unsigned n;
  uint32_t next_instance = 0, next_decision = 0;
  std::deque<Proposal> proposals;             // proposals[i] is instance next_decision + i
  std::unordered_map<uint32_t, Set> accepted; // what this process accepted, per instance

  static Bytes message(uint8_t kind, uint32_t instance, uint32_t number, const Set *value) {
    Bytes d;
    put8(d, kind);
    put32(d, instance);
    put32(d, number);
    if (value) {
      put32(d, static_cast<uint32_t>(value->size()));
      for (const std::string &v : *value) {
        put32(d, static_cast<uint32_t>(v.size()));
        d += v;
      }
    }
    return d;
  }

  static Set read_set(Reader &r) {
    Set s(r.u32());
    for (std::string &v : s)
      if (r.ok)
        v = r.take(r.u32());
    return s;
  }

  void send_proposal(uint32_t instance, const Proposal &p) {
    Bytes d = message(PROPOSE, instance, p.number, &p.value);
    for (unsigned q = 0; q < n; q++)
      pl.send(q, d);
  }

  void receive(unsigned from, std::string_view d) {
    Reader r{d};
    uint8_t kind = r.u8();
    uint32_t instance = r.u32(), number = r.u32();
    Set value = kind == ACK ? Set() : read_set(r);
    if (!r.ok)
      return;

    if (kind == PROPOSE) {
      Set &a = accepted[instance];
      if (std::includes(value.begin(), value.end(), a.begin(), a.end())) {
        a = std::move(value);
        pl.send(from, message(ACK, instance, number, nullptr));
      } else {
        a = merge(a, value);
        pl.send(from, message(NACK, instance, number, &a));
      }
      return;
    }

    if (instance < next_decision || instance >= next_instance)
      return;
    Proposal &p = proposals[instance - next_decision];
    if (p.decided || number != p.number)
      return;
    if (kind == ACK) {
      p.acks++;
    } else {
      p.nacks++;
      p.value = merge(p.value, value);
    }
    if ((p.acks + p.nacks) * 2 <= n)
      return;
    if (p.nacks > 0) {
      p.number++;
      p.acks = p.nacks = 0;
      send_proposal(instance, p);
      return;
    }
    p.decided = true;
    while (!proposals.empty() && proposals.front().decided) {
      on_decide(proposals.front().value);
      proposals.pop_front();
      next_decision++;
    }
  }
};
