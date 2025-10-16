#pragma once

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using Bytes = std::string;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

inline void put8(Bytes &b, uint8_t v) { b.push_back(static_cast<char>(v)); }

inline void put32(Bytes &b, uint32_t v) {
  for (int shift = 0; shift < 32; shift += 8)
    b.push_back(static_cast<char>(v >> shift));
}

struct Reader {
  std::string_view rest;
  bool ok = true;

  std::string_view take(size_t n) {
    if (rest.size() < n) {
      ok = false;
      n = rest.size();
    }
    std::string_view v = rest.substr(0, n);
    rest.remove_prefix(n);
    return v;
  }
  uint8_t u8() {
    std::string_view v = take(1);
    return ok ? static_cast<uint8_t>(v[0]) : 0;
  }
  uint32_t u32() {
    std::string_view v = take(4);
    uint32_t x = 0;
    for (size_t i = 0; ok && i < 4; i++)
      x |= static_cast<uint32_t>(static_cast<uint8_t>(v[i])) << (8 * i);
    return x;
  }
};

struct Options {
  unsigned self = 0; // 0-based
  std::vector<sockaddr_in> peers;
};

[[noreturn]] inline void die(const char *what) {
  std::fprintf(stderr, "%s\n", what);
  std::exit(2);
}

inline sockaddr_in address(const std::string &hostport) {
  size_t colon = hostport.rfind(':');
  if (colon == std::string::npos)
    die("addresses are HOST:PORT");
  std::string host = hostport.substr(0, colon);
  int port = std::atoi(hostport.c_str() + colon + 1);
  addrinfo hints{}, *res = nullptr;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  if (port <= 0 || port > 65535 ||
      getaddrinfo(host.empty() ? "127.0.0.1" : host.c_str(), nullptr, &hints, &res) != 0)
    die(("cannot resolve " + hostport).c_str());
  sockaddr_in a = *reinterpret_cast<sockaddr_in *>(res->ai_addr);
  a.sin_port = htons(static_cast<uint16_t>(port));
  freeaddrinfo(res);
  return a;
}

inline Options options(int argc, char **argv, const char *usage) {
  Options o;
  std::vector<std::string> rest;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a.size() > 1 && a[0] == '-')
      die(usage);
    else
      rest.push_back(a);
  }
  if (rest.size() < 2)
    die(usage);
  unsigned long id = std::strtoul(rest[0].c_str(), nullptr, 10);
  if (id < 1 || id >= rest.size())
    die(usage);
  o.self = static_cast<unsigned>(id - 1);
  for (size_t i = 1; i < rest.size(); i++)
    o.peers.push_back(address(rest[i]));
  return o;
}

class Net {
public:
  const unsigned self; // index of this process, 0-based
  const unsigned n;    // number of processes
  std::function<void(std::string_view datagram)> on_receive;

  explicit Net(const Options &o)
      : self(o.self), n(static_cast<unsigned>(o.peers.size())), peers(o.peers) {
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0 || bind(fd, reinterpret_cast<const sockaddr *>(&peers[self]),
                       sizeof(sockaddr_in)) < 0) {
      std::perror("bind");
      std::exit(1);
    }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
  }
  ~Net() { close(fd); }
  Net(const Net &) = delete;
  Net &operator=(const Net &) = delete;

  void send(unsigned to, std::string_view datagram) {
    sendto(fd, datagram.data(), datagram.size(), 0,
           reinterpret_cast<const sockaddr *>(&peers[to]), sizeof(sockaddr_in));
  }

private:
  int fd;
  std::vector<sockaddr_in> peers;

  void receive() {
    static char buf[1 << 16];
    for (int i = 0; i < 1024; i++) {
      ssize_t len = recv(fd, buf, sizeof buf, 0);
      if (len < 0)
        break;
      on_receive(std::string_view(buf, static_cast<size_t>(len)));
    }
  }

  friend class Loop;
};

class Input {
public:
  bool eof() const { return closed; }
  bool has_line() const { return std::memchr(buf.data() + start, '\n', buf.size() - start); }

  void fill() {
    char chunk[1 << 16];
    ssize_t len = read(STDIN_FILENO, chunk, sizeof chunk);
    if (len < 0 && errno == EINTR)
      return;
    if (len <= 0) {
      closed = true;
      if (start < buf.size() && buf.back() != '\n')
        buf += '\n';
      return;
    }
    if (start > (1 << 16)) {
      buf.erase(0, start);
      start = 0;
    }
    buf.append(chunk, static_cast<size_t>(len));
  }

  // the next line, without the newline
  std::string_view line() {
    const char *nl = static_cast<const char *>(std::memchr(buf.data() + start, '\n', buf.size() - start));
    size_t end = static_cast<size_t>(nl - buf.data());
    std::string_view l(buf.data() + start, end - start);
    start = end + 1;
    return l;
  }

private:
  Bytes buf;
  size_t start = 0;
  bool closed = false;
};

class Output {
public:
  void line(std::string_view s) {
    buf += s;
    buf += '\n';
  }
  void flush() {
    size_t done = 0;
    while (done < buf.size()) {
      ssize_t len = write(STDOUT_FILENO, buf.data() + done, buf.size() - done);
      if (len < 0 && errno == EINTR)
        continue;
      if (len < 0)
        break;
      done += static_cast<size_t>(len);
    }
    buf.clear();
  }

private:
  Bytes buf;
};

inline volatile std::sig_atomic_t stopping = 0;

class Loop {
public:
  // run until SIGINT or SIGTERM.
  // `consume` gets the input line by line.
  static void run(Net &net, Output &out,
                  const std::function<void(std::string_view)> &consume) {
    struct sigaction sa{};
    sa.sa_handler = [](int) { stopping = 1; };
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    Input in;
    while (!stopping) {
      bool want = !in.eof() && !in.has_line();
      fd_set fds;
      FD_ZERO(&fds);
      FD_SET(net.fd, &fds);
      if (want)
        FD_SET(STDIN_FILENO, &fds);
      int r = select(net.fd + 1, &fds, nullptr, nullptr, nullptr);
      if (r < 0 && errno != EINTR) {
        std::perror("select");
        break;
      }
      if (r > 0 && FD_ISSET(net.fd, &fds))
        net.receive();
      if (r > 0 && want && FD_ISSET(STDIN_FILENO, &fds))
        in.fill();
      while (in.has_line())
        consume(in.line());
      out.flush();
    }
    out.flush();
  }
};
