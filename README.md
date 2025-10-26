# Lattice Agreement

Perfect links, FIFO uniform reliable broadcast and lattice agreement over UDP, written in C++17.

| Program | Abstraction | Implementation |
|---|---|---|
| `links` | [Perfect links][pl] | Sequence numbers and per-message ACKs. Fast retransmit and an adaptive timeout, as in TCP. |
| `broadcast` | [FIFO uniform reliable broadcast][furb] | Three layers over perfect links: [best-effort][beb], [majority-ack][urb], [sequence numbers][fifo]. |
| `lattice` | [Lattice agreement][la] | [Proposer-acceptor][pa] over perfect links. One instance per input line. |

## Usage

    make
    bin/broadcast 1 localhost:9001 localhost:9002 localhost:9003

Start one process per address. Each reads lines from standard input and prints what it delivers.

- `links`: a line `ID text` sends `text` to process `ID`.
- `broadcast`: every line is broadcast.
- `lattice`: every line is a proposal, a set of words.

`--loss 0.1` drops a tenth of the datagrams a process sends.

`bench/run.py` runs a group on one machine under loss, pauses and crashes. It checks every output against the properties of the abstraction.

    bench/run.py broadcast -n 5 -m 100000 --loss 0.1 --pause --crash 2

[pl]: https://chutommy.com/dist-algorithms#perfect-links
[furb]: https://chutommy.com/dist-algorithms#fifo-uniform-broadcast
[beb]: https://chutommy.com/dist-algorithms#best-effort-broadcast/basic-broadcast
[urb]: https://chutommy.com/dist-algorithms#uniform-reliable-broadcast/majority-ack-uniform-reliable-broadcast
[fifo]: https://chutommy.com/dist-algorithms#fifo-uniform-broadcast/broadcast-with-sequence-number
[la]: https://chutommy.com/dist-algorithms#lattice-agreement
[pa]: https://chutommy.com/dist-algorithms#lattice-agreement/proposer-acceptor-lattice-agreement
