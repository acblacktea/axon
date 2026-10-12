// Market data over shared memory: one writer (the MDS), any number of readers
// (strategies on the same host), no locks, and a writer that never waits.
//
// The other transport, ZMQ, stays: it is how a strategy on another machine
// gets the same feed. This one is for the same machine, where a ZMQ hop -- a
// JSON encode, a socket, a JSON decode -- costs tens of microseconds and this
// costs a memcpy.
//
// THE STRUCTURE is a broadcast ring, after Agrona's BroadcastTransmitter /
// BroadcastReceiver (the same idea as LMAX's Disruptor, minus consumer
// back-pressure). It is NOT a queue: nothing is dequeued, every reader sees
// every record, and the writer does not know the readers exist.
//
//   * The writer appends variable-length records to a ring of `capacity`
//     bytes. Before writing it advances `tail_intent` -- "bytes up to here may
//     be overwritten" -- and after, `tail` -- "records up to here are whole".
//   * Each reader keeps its own cursor. It copies a record out, then re-reads
//     `tail_intent`: if the writer has since claimed the bytes it was copying,
//     the copy may be torn, so it is thrown away. That is the whole protocol --
//     a seqlock whose version number is the ring position.
//   * A reader that falls a full ring behind has been LAPPED. It jumps to the
//     present and counts what it missed. A slow strategy loses data; it never
//     slows the feed down for anyone else.
//
// That last property is the design choice. Every record is a complete picture
// -- a top-N book snapshot, a ticker, a kline -- so a reader that skips ahead
// loses history, never correctness.
//
// RECORDS are fixed binary layouts, no JSON: a strategy reads prices straight
// out of the buffer. Positions are absolute 64-bit byte offsets that never
// wrap, so "how far behind am I" is a subtraction.
//
// THIS HEADER is self-contained -- C++20, POSIX, nothing else -- so a strategy
// includes it without taking on the MDS's dependencies.
//
// Single writer per segment: the MDS publishes from one thread.

#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

namespace axon::market_data::shm {

// ---------------------------------------------------------------------------
// The record format
// ---------------------------------------------------------------------------

inline constexpr std::uint64_t kMagic = 0x4158'4D44'5348'4D31ULL;  // "AXMDSHM1"
inline constexpr std::uint32_t kVersion = 1;
// Every record starts on this boundary, so a header always fits in front of
// the end of the ring -- the padding record that fills the gap needs 16 bytes.
inline constexpr std::size_t kRecordAlign = 16;
// The largest record a writer accepts and a reader buffers: a 400-level book
// is ~13 KB.
inline constexpr std::size_t kMaxRecordBytes = 64 * 1024;
inline constexpr std::size_t kDefaultCapacity = 64ULL << 20;  // 64 MiB

enum class RecordType : std::uint16_t {
  kPadding = 0,  // fills the end of the ring; readers skip it
  kDepth = 1,
  kTicker = 2,
  kKline = 3,
};

enum class EventKind : std::uint8_t { kSnapshot = 0, kUpdate = 1 };

struct RecordHeader {
  std::uint32_t stride;    // bytes from this record to the next, header included
  RecordType type;
  std::uint16_t reserved;
  std::uint64_t sequence;  // 1, 2, 3... per writer session: a gap is a loss
};
static_assert(sizeof(RecordHeader) == 16);

// Common to every market data record.
struct EventHeader {
  char exchange[24];  // NUL-padded, e.g. "binance_usdt_futures"
  char symbol[40];    // unified symbol, e.g. "BTC_USDT_PERP"
  std::int64_t exchange_ts_ms;  // the exchange's stamp
  std::int64_t local_ts_ms;     // when the MDS received it
  // steady_clock (CLOCK_MONOTONIC) when the record was written. The clock is
  // system-wide, so a reader on the same host gets the one-way latency of
  // this hop as now - publish_ns.
  std::int64_t publish_ns;
  std::int64_t book_sequence;  // depth: the exchange's update id; otherwise 0
  std::uint16_t bid_count;     // depth only
  std::uint16_t ask_count;
  EventKind kind;
  std::uint8_t reserved[3];
};
static_assert(sizeof(EventHeader) == 104);

// A depth record's body: bid_count bids (best first), then ask_count asks.
struct Level {
  double price;
  double quantity;
};

struct Ticker {
  double bid_price;
  double bid_quantity;
  double ask_price;
  double ask_quantity;
};

struct Kline {
  char interval[8];  // "1m"
  std::int64_t open_time;
  std::int64_t close_time;
  double open;
  double high;
  double low;
  double close;
  double volume;
  double quote_volume;
  std::int64_t num_trades;
  std::uint8_t is_closed;
  std::uint8_t reserved[7];
};

// ---------------------------------------------------------------------------
// The segment
// ---------------------------------------------------------------------------

// Cache-line separated, so the counters the writer updates on every record do
// not share a line with the ones readers only read at attach time.
struct SegmentHeader {
  std::uint64_t magic;
  std::uint32_t version;
  std::uint32_t reserved;
  std::uint64_t capacity;  // power of two
  // Bumped each time a writer attaches. A reader that sees it change knows
  // the writer restarted and its sequence numbers start over.
  std::atomic<std::uint64_t> epoch;
  // Where the current writer's first record is: a reader that notices the
  // restart late reads on from here rather than skipping what it missed.
  std::atomic<std::uint64_t> epoch_start;
  alignas(64) std::atomic<std::uint64_t> tail_intent;
  alignas(64) std::atomic<std::uint64_t> tail;
  alignas(64) std::atomic<std::int64_t> heartbeat_ns;
  std::atomic<std::int32_t> writer_pid;
};
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(sizeof(SegmentHeader) % 64 == 0);

inline std::int64_t steady_ns() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

namespace detail {

// "axon_mds" -> "/axon_mds", the form shm_open wants.
inline std::string object_name(std::string_view name) {
  std::string out;
  if (name.empty() || name.front() != '/') out += '/';
  out += name;
  return out;
}

inline constexpr std::size_t align_up(std::size_t n, std::size_t a) noexcept {
  return (n + a - 1) & ~(a - 1);
}

[[noreturn]] inline void fail(const std::string& what) {
  throw std::system_error(errno, std::generic_category(), what);
}

// An mmap'd shm object, unmapped and closed on destruction.
class Mapping {
 public:
  Mapping() = default;
  Mapping(void* p, std::size_t size) : p_(p), size_(size) {}
  ~Mapping() { reset(); }
  Mapping(Mapping&& o) noexcept : p_(o.p_), size_(o.size_) { o.p_ = nullptr; }
  Mapping& operator=(Mapping&& o) noexcept {
    if (this != &o) {
      reset();
      p_ = o.p_;
      size_ = o.size_;
      o.p_ = nullptr;
    }
    return *this;
  }
  void reset() noexcept {
    if (p_ != nullptr) ::munmap(p_, size_);
    p_ = nullptr;
  }
  std::byte* data() const noexcept { return static_cast<std::byte*>(p_); }

 private:
  void* p_ = nullptr;
  std::size_t size_ = 0;
};

}  // namespace detail

// Removes a segment's name. Readers already attached keep their mapping.
inline void unlink(std::string_view name) { ::shm_unlink(detail::object_name(name).c_str()); }

// ---------------------------------------------------------------------------
// The writer
// ---------------------------------------------------------------------------

class Writer {
 public:
  // Attaches to the named segment, creating it if needed. A segment of the
  // right shape is reused in place -- readers already attached keep reading
  // across a writer restart, and see the epoch change. One of another shape
  // (a different capacity or version) is replaced; readers of the old one
  // see its heartbeat stop and must reattach.
  explicit Writer(std::string_view name, std::size_t capacity = kDefaultCapacity)
      : name_(detail::object_name(name)), capacity_(capacity), mask_(capacity - 1) {
    if (capacity < 4096 || (capacity & (capacity - 1)) != 0) {
      throw std::invalid_argument("shm capacity must be a power of two of at least 4096");
    }
    const std::size_t size = sizeof(SegmentHeader) + capacity;

    int fd = ::shm_open(name_.c_str(), O_RDWR, 0644);
    bool reuse = false;
    if (fd >= 0) {
      struct stat st {};
      if (::fstat(fd, &st) == 0 && static_cast<std::size_t>(st.st_size) == size) {
        void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (p != MAP_FAILED) {
          const auto* h = static_cast<const SegmentHeader*>(p);
          reuse = h->magic == kMagic && h->version == kVersion && h->capacity == capacity;
          map_ = detail::Mapping(p, size);
        }
      }
      if (!reuse) {
        // Never resized in place: a reader still mapped to a shrinking
        // object would fault. A fresh object under the same name instead.
        map_.reset();
        ::close(fd);
        ::shm_unlink(name_.c_str());
        fd = -1;
      }
    }
    if (fd < 0) {
      fd = ::shm_open(name_.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
      if (fd < 0) detail::fail("shm_open " + name_);
      if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
        ::close(fd);
        detail::fail("ftruncate " + name_);
      }
      // Populated up front: a page fault on the first write to each page is
      // exactly the tail latency this transport exists to avoid.
      void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, 0);
      if (p == MAP_FAILED) {
        ::close(fd);
        detail::fail("mmap " + name_);
      }
      map_ = detail::Mapping(p, size);
      auto* h = header();
      h->magic = kMagic;
      h->version = kVersion;
      h->capacity = capacity;
      h->epoch.store(0, std::memory_order_relaxed);
      h->epoch_start.store(0, std::memory_order_relaxed);
      h->tail_intent.store(0, std::memory_order_relaxed);
      h->tail.store(0, std::memory_order_relaxed);
    }
    ::close(fd);

    auto* h = header();
    h->writer_pid.store(static_cast<std::int32_t>(::getpid()), std::memory_order_relaxed);
    position_ = h->tail.load(std::memory_order_relaxed);
    const std::uint64_t intent = h->tail_intent.load(std::memory_order_relaxed);
    if (intent > position_) {
      // A writer that died mid-record left half of one behind. Cover it with
      // padding before the tail moves past it, so no reader can take what is
      // there for a record.
      for (std::uint64_t at = position_; at < intent;) {
        const std::size_t index = static_cast<std::size_t>(at & mask_);
        const std::size_t len =
            static_cast<std::size_t>(std::min<std::uint64_t>(intent - at, capacity_ - index));
        const RecordHeader filler{static_cast<std::uint32_t>(len), RecordType::kPadding, 0, 0};
        std::memcpy(data() + index, &filler, sizeof filler);
        at += len;
      }
      position_ = intent;
    }
    h->tail_intent.store(position_, std::memory_order_relaxed);
    h->tail.store(position_, std::memory_order_release);
    // Positions carry on from the last writer, so readers already attached
    // keep reading the same ring; only the sequence numbers start over.
    h->epoch_start.store(position_, std::memory_order_relaxed);
    h->epoch.fetch_add(1, std::memory_order_release);
    heartbeat();
  }

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  // Appends one record made of `parts`, concatenated. False if it is larger
  // than kMaxRecordBytes. Never blocks.
  bool publish(RecordType type, std::initializer_list<std::span<const std::byte>> parts) noexcept {
    std::size_t payload = 0;
    for (const auto& p : parts) payload += p.size();
    const std::size_t stride = detail::align_up(sizeof(RecordHeader) + payload, kRecordAlign);
    if (stride > kMaxRecordBytes || stride > capacity_ / 2) {
      return false;
    }

    auto* h = header();
    std::uint64_t pos = position_;
    std::size_t index = static_cast<std::size_t>(pos & mask_);
    const bool wraps = index + stride > capacity_;
    const std::size_t pad = wraps ? capacity_ - index : 0;

    // Claim first: from here a reader inside these bytes discards its copy.
    h->tail_intent.store(pos + pad + stride, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);

    if (wraps) {
      const RecordHeader filler{static_cast<std::uint32_t>(pad), RecordType::kPadding, 0, 0};
      std::memcpy(data() + index, &filler, sizeof filler);
      pos += pad;
      index = 0;
    }
    const RecordHeader rh{static_cast<std::uint32_t>(stride), type, 0, ++sequence_};
    std::byte* out = data() + index;
    std::memcpy(out, &rh, sizeof rh);
    out += sizeof rh;
    for (const auto& p : parts) {
      std::memcpy(out, p.data(), p.size());
      out += p.size();
    }

    position_ = pos + stride;
    h->tail.store(position_, std::memory_order_release);
    ++published_;
    return true;
  }

  // Lets readers tell a quiet feed from a dead writer. Call at least every
  // second or so; publish() does not, to stay off the clock on every record.
  void heartbeat() noexcept {
    header()->heartbeat_ns.store(steady_ns(), std::memory_order_relaxed);
  }

  std::uint64_t published() const noexcept { return published_; }
  std::size_t capacity() const noexcept { return capacity_; }
  const std::string& name() const noexcept { return name_; }

 private:
  SegmentHeader* header() const noexcept { return reinterpret_cast<SegmentHeader*>(map_.data()); }
  std::byte* data() const noexcept { return map_.data() + sizeof(SegmentHeader); }

  std::string name_;
  std::size_t capacity_;
  std::uint64_t mask_;
  detail::Mapping map_;
  std::uint64_t position_ = 0;
  std::uint64_t sequence_ = 0;
  std::uint64_t published_ = 0;
};

// ---------------------------------------------------------------------------
// The reader
// ---------------------------------------------------------------------------

// One record, valid until the next poll() delivers another.
class RecordView {
 public:
  RecordType type() const noexcept { return type_; }
  std::uint64_t sequence() const noexcept { return sequence_; }
  const EventHeader& event() const noexcept { return event_; }
  std::string_view exchange() const noexcept {
    return {event_.exchange, ::strnlen(event_.exchange, sizeof event_.exchange)};
  }
  std::string_view symbol() const noexcept {
    return {event_.symbol, ::strnlen(event_.symbol, sizeof event_.symbol)};
  }
  EventKind kind() const noexcept { return event_.kind; }

  // Depth only: best first.
  std::span<const Level> bids() const noexcept { return {levels_, event_.bid_count}; }
  std::span<const Level> asks() const noexcept {
    return {levels_ + event_.bid_count, event_.ask_count};
  }
  // Ticker / kline only.
  const Ticker& ticker() const noexcept { return *reinterpret_cast<const Ticker*>(body_); }
  const Kline& kline() const noexcept { return *reinterpret_cast<const Kline*>(body_); }

 private:
  friend class Reader;
  RecordType type_ = RecordType::kPadding;
  std::uint64_t sequence_ = 0;
  EventHeader event_{};
  const std::byte* body_ = nullptr;
  const Level* levels_ = nullptr;
};

class Reader {
 public:
  struct Stats {
    std::uint64_t records = 0;     // delivered
    std::uint64_t lost = 0;        // skipped by the writer's sequence numbers
    std::uint64_t lapped = 0;      // times this reader fell a whole ring behind
    std::uint64_t restarts = 0;    // writer restarts seen
    std::uint64_t malformed = 0;   // records whose own counts did not fit them
  };

  // Attaches read-only and starts at the present: records already in the ring
  // are history. Throws if the segment does not exist or is not this format.
  explicit Reader(std::string_view name)
      : name_(detail::object_name(name)), scratch_(new std::byte[kMaxRecordBytes]) {
    const int fd = ::shm_open(name_.c_str(), O_RDONLY, 0);
    if (fd < 0) detail::fail("shm_open " + name_ + " (is the market data service running?)");
    struct stat st {};
    if (::fstat(fd, &st) != 0 || static_cast<std::size_t>(st.st_size) < sizeof(SegmentHeader)) {
      ::close(fd);
      throw std::runtime_error(name_ + " is not a market data segment");
    }
    const auto size = static_cast<std::size_t>(st.st_size);
    void* p = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    ::close(fd);
    if (p == MAP_FAILED) detail::fail("mmap " + name_);
    map_ = detail::Mapping(p, size);

    const auto* h = header();
    if (h->magic != kMagic || h->version != kVersion ||
        sizeof(SegmentHeader) + h->capacity != size) {
      throw std::runtime_error(name_ + ": unsupported segment format (writer version " +
                               std::to_string(h->version) + ", reader " +
                               std::to_string(kVersion) + ")");
    }
    capacity_ = h->capacity;
    mask_ = capacity_ - 1;
    epoch_ = h->epoch.load(std::memory_order_acquire);
    cursor_ = h->tail.load(std::memory_order_acquire);
  }

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  // Calls `fn(const RecordView&)` for each record written since the last call,
  // up to `max_records`. Returns how many were delivered. Never blocks.
  template <typename Fn>
  std::size_t poll(Fn&& fn, std::size_t max_records = 256) {
    const auto* h = header();
    const std::uint64_t epoch = h->epoch.load(std::memory_order_acquire);
    if (epoch != epoch_) {
      // A new writer. Its sequence numbers start again from 1, and its first
      // records begin at epoch_start -- read from there, unless this reader
      // already has (it may have read on before noticing).
      epoch_ = epoch;
      ++stats_.restarts;
      expected_sequence_ = 0;
      cursor_ = std::max(cursor_, h->epoch_start.load(std::memory_order_relaxed));
    }

    std::size_t delivered = 0;
    std::uint64_t tail = h->tail.load(std::memory_order_acquire);
    while (cursor_ != tail && delivered < max_records) {
      if (tail - cursor_ > capacity_) {
        lap(tail);
        continue;
      }
      const std::size_t index = static_cast<std::size_t>(cursor_ & mask_);
      RecordHeader rh;
      std::memcpy(&rh, data() + index, sizeof rh);
      if (!still_valid(cursor_)) {
        lap(h->tail.load(std::memory_order_acquire));
        tail = cursor_;
        continue;
      }
      if (rh.stride < sizeof(RecordHeader) || rh.stride % kRecordAlign != 0 ||
          rh.stride > capacity_ - index) {
        // Only possible for bytes being rewritten under us -- which the check
        // above rules out -- or a foreign writer. Either way, resync.
        lap(h->tail.load(std::memory_order_acquire));
        tail = cursor_;
        continue;
      }
      if (rh.type == RecordType::kPadding) {
        cursor_ += rh.stride;
        continue;
      }
      const std::size_t body = rh.stride - sizeof(RecordHeader);
      if (body > kMaxRecordBytes) {
        lap(h->tail.load(std::memory_order_acquire));
        tail = cursor_;
        continue;
      }
      std::memcpy(scratch_.get(), data() + index + sizeof(RecordHeader), body);
      if (!still_valid(cursor_)) {
        lap(h->tail.load(std::memory_order_acquire));
        tail = cursor_;
        continue;
      }
      cursor_ += rh.stride;

      if (expected_sequence_ != 0 && rh.sequence > expected_sequence_) {
        stats_.lost += rh.sequence - expected_sequence_;
      }
      expected_sequence_ = rh.sequence + 1;

      if (!make_view(rh, body)) {
        ++stats_.malformed;
        continue;
      }
      ++stats_.records;
      ++delivered;
      fn(static_cast<const RecordView&>(view_));
    }
    return delivered;
  }

  // Whether the writer has signalled within `max_age`. A reader whose writer
  // is gone keeps returning nothing from poll(); this is how it can tell.
  bool writer_alive(std::chrono::nanoseconds max_age = std::chrono::seconds(5)) const noexcept {
    return steady_ns() - header()->heartbeat_ns.load(std::memory_order_relaxed) <=
           max_age.count();
  }

  // Bytes the writer is ahead of this reader. Near capacity() means about to
  // be lapped.
  std::uint64_t backlog() const noexcept {
    return header()->tail.load(std::memory_order_acquire) - cursor_;
  }
  std::size_t capacity() const noexcept { return capacity_; }
  const Stats& stats() const noexcept { return stats_; }
  const std::string& name() const noexcept { return name_; }

 private:
  const SegmentHeader* header() const noexcept {
    return reinterpret_cast<const SegmentHeader*>(map_.data());
  }
  const std::byte* data() const noexcept { return map_.data() + sizeof(SegmentHeader); }

  // True if the bytes at `position` cannot have been touched since they were
  // copied: the writer has not claimed past one ring ahead of them.
  bool still_valid(std::uint64_t position) const noexcept {
    std::atomic_thread_fence(std::memory_order_acquire);
    return header()->tail_intent.load(std::memory_order_relaxed) - position <= capacity_;
  }

  // Fell behind: jump to the present. The sequence gap on the next record
  // counts what was missed.
  void lap(std::uint64_t tail) noexcept {
    ++stats_.lapped;
    cursor_ = tail;
  }

  bool make_view(const RecordHeader& rh, std::size_t body) noexcept {
    if (body < sizeof(EventHeader)) return false;
    view_.type_ = rh.type;
    view_.sequence_ = rh.sequence;
    std::memcpy(&view_.event_, scratch_.get(), sizeof(EventHeader));
    view_.body_ = scratch_.get() + sizeof(EventHeader);
    view_.levels_ = reinterpret_cast<const Level*>(view_.body_);
    const std::size_t rest = body - sizeof(EventHeader);
    switch (rh.type) {
      case RecordType::kDepth:
        return (std::size_t{view_.event_.bid_count} + view_.event_.ask_count) * sizeof(Level) <=
               rest;
      case RecordType::kTicker:
        return sizeof(Ticker) <= rest;
      case RecordType::kKline:
        return sizeof(Kline) <= rest;
      case RecordType::kPadding:
        break;
    }
    return false;
  }

  std::string name_;
  detail::Mapping map_;
  std::uint64_t capacity_ = 0;
  std::uint64_t mask_ = 0;
  std::uint64_t epoch_ = 0;
  std::uint64_t cursor_ = 0;
  std::uint64_t expected_sequence_ = 0;
  std::unique_ptr<std::byte[]> scratch_;
  RecordView view_;
  Stats stats_;
};

}  // namespace axon::market_data::shm
