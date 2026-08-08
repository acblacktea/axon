// Contiguous byte buffer for socket I/O.
//
// One allocation at construction, none afterwards. The read side hands out a
// pointer straight into the buffer so a WebSocket frame can be parsed in place
// and a JSON payload scanned without ever being copied.
//
// Layout:
//
//     [ consumed | readable | writable ]
//       ^          ^          ^
//       0          read_      write_
//
// `consumed` is dead space left behind by consume(). It is reclaimed by
// compact(), which memmoves the readable region back to offset 0.
//
// WHY COMPACTION AND NOT A RING: a ring buffer never memmoves, but it can
// split a message across the wrap point, and then the parser needs either a
// scatter-gather API or a copy into a staging buffer. For a stream where a
// single logical message must be contiguous to be parsed in place, the
// occasional memmove is cheaper than making every parse handle a discontinuity.
// The memmove only happens when the writable tail runs out, and only moves the
// bytes of a partially-received message -- typically a few hundred.
//
// Not thread safe. One buffer per connection, owned by that connection's
// thread.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

#include "calais/core/platform.h"

namespace calais::net {

class ByteBuffer {
 public:
  ByteBuffer() = default;

  explicit ByteBuffer(std::size_t capacity)
      : data_(static_cast<std::byte*>(::operator new(capacity, kAlign))),
        capacity_(capacity) {}

  ~ByteBuffer() {
    if (data_ != nullptr) {
      ::operator delete(data_, kAlign);
    }
  }

  ByteBuffer(const ByteBuffer&) = delete;
  ByteBuffer& operator=(const ByteBuffer&) = delete;

  ByteBuffer(ByteBuffer&& o) noexcept { *this = std::move(o); }

  ByteBuffer& operator=(ByteBuffer&& o) noexcept {
    if (this != &o) {
      if (data_ != nullptr) {
        ::operator delete(data_, kAlign);
      }
      data_ = o.data_;
      capacity_ = o.capacity_;
      read_ = o.read_;
      write_ = o.write_;
      compactions_ = o.compactions_;
      o.data_ = nullptr;
      o.capacity_ = 0;
      o.read_ = 0;
      o.write_ = 0;
      o.compactions_ = 0;
    }
    return *this;
  }

  // ---- read side --------------------------------------------------------

  const std::byte* readable() const noexcept { return data_ + read_; }
  std::size_t readable_size() const noexcept { return write_ - read_; }
  bool empty() const noexcept { return read_ == write_; }

  // Marks `n` bytes as consumed. When the buffer drains completely the
  // cursors reset to zero, which is the common case and costs nothing.
  void consume(std::size_t n) noexcept {
    read_ += n;
    if (read_ == write_) {
      read_ = 0;
      write_ = 0;
    }
  }

  // ---- write side -------------------------------------------------------

  std::byte* writable() noexcept { return data_ + write_; }
  std::size_t writable_size() const noexcept { return capacity_ - write_; }

  void commit(std::size_t n) noexcept { write_ += n; }

  // Moves the unread bytes to the front, reclaiming space already consumed.
  // Returns the number of bytes now writable.
  std::size_t compact() noexcept {
    if (read_ == 0) {
      return writable_size();
    }
    const std::size_t live = write_ - read_;
    if (live > 0) {
      std::memmove(data_, data_ + read_, live);
      ++compactions_;
    }
    read_ = 0;
    write_ = live;
    return writable_size();
  }

  // Compacts only if the writable tail has fallen below `needed`. Returns
  // false when even a full compaction cannot free that much -- which means the
  // buffer is genuinely too small for the message in flight, and the caller
  // must fail the connection rather than silently truncate.
  bool ensure_writable(std::size_t needed) noexcept {
    if (writable_size() >= needed) {
      return true;
    }
    compact();
    return writable_size() >= needed;
  }

  // ---- state ------------------------------------------------------------

  std::size_t capacity() const noexcept { return capacity_; }
  bool valid() const noexcept { return data_ != nullptr; }

  // How many times bytes have been memmoved. A number that climbs steadily in
  // production means the buffer is undersized relative to the message rate --
  // every compaction is a copy that a bigger buffer would have avoided.
  std::uint64_t compactions() const noexcept { return compactions_; }

  void clear() noexcept {
    read_ = 0;
    write_ = 0;
  }

 private:
  // Over-align to a cache line so the read cursor and the socket's DMA target
  // never share a line with anything else.
  static constexpr std::align_val_t kAlign{core::kCacheLineSize};

  std::byte* data_ = nullptr;
  std::size_t capacity_ = 0;
  std::size_t read_ = 0;
  std::size_t write_ = 0;
  std::uint64_t compactions_ = 0;
};

}  // namespace calais::net
