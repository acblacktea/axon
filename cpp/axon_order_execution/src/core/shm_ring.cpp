#include "axon/core/shm_ring.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <new>
#include <utility>

namespace axon::core {
namespace {

std::size_t align_up(std::size_t v, std::size_t a) noexcept {
  return (v + a - 1) / a * a;
}

[[noreturn]] void throw_errno(const char* what, const std::string& path) {
  throw ShmRingError(std::string(what) + " '" + path +
                     "': " + std::strerror(errno));
}

struct SlotHeader {
  std::uint32_t length;
  std::uint32_t reserved0;
  std::uint64_t reserved1;
};
static_assert(sizeof(SlotHeader) == kShmSlotHeaderSize);

}  // namespace

ShmRing::~ShmRing() { close(); }

ShmRing::ShmRing(ShmRing&& other) noexcept { *this = std::move(other); }

ShmRing& ShmRing::operator=(ShmRing&& other) noexcept {
  if (this != &other) {
    close();
    header_ = std::exchange(other.header_, nullptr);
    data_ = std::exchange(other.data_, nullptr);
    mapping_ = std::exchange(other.mapping_, nullptr);
    mapping_size_ = std::exchange(other.mapping_size_, 0);
    fd_ = std::exchange(other.fd_, -1);
    path_ = std::move(other.path_);
    unlink_on_destroy_ = std::exchange(other.unlink_on_destroy_, false);
    cached_head_ = other.cached_head_;
    cached_tail_ = other.cached_tail_;
    mask_ = std::exchange(other.mask_, 0);
    stride_ = std::exchange(other.stride_, 0);
  }
  return *this;
}

void ShmRing::close() noexcept {
  if (mapping_ != nullptr) {
    ::munmap(mapping_, mapping_size_);
    mapping_ = nullptr;
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  if (unlink_on_destroy_ && !path_.empty()) {
    ::unlink(path_.c_str());
    unlink_on_destroy_ = false;
  }
  header_ = nullptr;
  data_ = nullptr;
  mapping_size_ = 0;
}

ShmRing ShmRing::create(const std::string& path, std::uint32_t max_payload,
                        std::uint32_t slot_count) {
  if (slot_count < 2 || (slot_count & (slot_count - 1)) != 0) {
    throw ShmRingError("slot_count must be a power of two >= 2");
  }
  if (max_payload == 0) {
    throw ShmRingError("max_payload must be non-zero");
  }

  const std::size_t stride =
      align_up(kShmSlotHeaderSize + max_payload, kCacheLineSize);
  const std::size_t data_offset = align_up(sizeof(ShmRingHeader), kCacheLineSize);
  const std::size_t total = data_offset + stride * slot_count;

  // O_TRUNC so a stale ring from a crashed run cannot be resurrected with old
  // cursors -- restarting must always give a clean ring.
  const int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0600);
  if (fd < 0) {
    throw_errno("shm ring: open", path);
  }
  if (::ftruncate(fd, static_cast<off_t>(total)) != 0) {
    ::close(fd);
    throw_errno("shm ring: ftruncate", path);
  }

  void* map = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    ::close(fd);
    throw_errno("shm ring: mmap", path);
  }

  auto* header = new (map) ShmRingHeader{};
  header->magic = kShmRingMagic;
  header->version = kShmRingVersion;
  header->slot_stride = static_cast<std::uint32_t>(stride);
  header->slot_count = slot_count;
  header->max_payload = max_payload;
  header->reserved = 0;
  header->data_offset = data_offset;
  header->total_bytes = total;
  header->tail.store(0, std::memory_order_relaxed);
  header->head.store(0, std::memory_order_relaxed);
  header->push_failures.store(0, std::memory_order_relaxed);

  ShmRing ring;
  ring.header_ = header;
  ring.mapping_ = map;
  ring.mapping_size_ = total;
  ring.fd_ = fd;
  ring.path_ = path;
  ring.data_ = static_cast<std::byte*>(map) + data_offset;
  ring.mask_ = slot_count - 1;
  ring.stride_ = static_cast<std::uint32_t>(stride);
  return ring;
}

ShmRing ShmRing::open(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) {
    throw_errno("shm ring: open", path);
  }

  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    ::close(fd);
    throw_errno("shm ring: fstat", path);
  }
  if (static_cast<std::size_t>(st.st_size) < sizeof(ShmRingHeader)) {
    ::close(fd);
    throw ShmRingError("shm ring: '" + path + "' is truncated");
  }

  const std::size_t total = static_cast<std::size_t>(st.st_size);
  void* map = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    ::close(fd);
    throw_errno("shm ring: mmap", path);
  }

  auto* header = static_cast<ShmRingHeader*>(map);
  if (header->magic != kShmRingMagic) {
    ::munmap(map, total);
    ::close(fd);
    throw ShmRingError("shm ring: '" + path + "' has a bad magic number");
  }
  if (header->version != kShmRingVersion) {
    ::munmap(map, total);
    ::close(fd);
    throw ShmRingError("shm ring: '" + path + "' version mismatch (file " +
                       std::to_string(header->version) + ", expected " +
                       std::to_string(kShmRingVersion) + ")");
  }
  if (header->total_bytes != total) {
    ::munmap(map, total);
    ::close(fd);
    throw ShmRingError("shm ring: '" + path + "' size mismatch");
  }

  ShmRing ring;
  ring.header_ = header;
  ring.mapping_ = map;
  ring.mapping_size_ = total;
  ring.fd_ = fd;
  ring.path_ = path;
  ring.data_ = static_cast<std::byte*>(map) + header->data_offset;
  ring.mask_ = header->slot_count - 1;
  ring.stride_ = header->slot_stride;
  ring.cached_head_ = header->head.load(std::memory_order_acquire);
  ring.cached_tail_ = header->tail.load(std::memory_order_acquire);
  return ring;
}

std::byte* ShmRing::slot_at(std::uint64_t seq) const noexcept {
  return data_ + (seq & mask_) * stride_;
}

void* ShmRing::try_acquire_write() noexcept {
  const std::uint64_t tail = header_->tail.load(std::memory_order_relaxed);

  if (AXON_UNLIKELY(tail - cached_head_ >= header_->slot_count)) {
    cached_head_ = header_->head.load(std::memory_order_acquire);
    if (tail - cached_head_ >= header_->slot_count) {
      header_->push_failures.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }
  }
  return slot_at(tail) + kShmSlotHeaderSize;
}

void ShmRing::commit_write(std::uint32_t len) noexcept {
  const std::uint64_t tail = header_->tail.load(std::memory_order_relaxed);
  auto* slot = reinterpret_cast<SlotHeader*>(slot_at(tail));
  slot->length = len;
  slot->reserved0 = 0;
  slot->reserved1 = 0;
  // Release pairs with the consumer's acquire on `tail`: the payload and the
  // length must both be visible before the cursor advances.
  header_->tail.store(tail + 1, std::memory_order_release);
}

bool ShmRing::try_push(const void* data, std::uint32_t len) noexcept {
  if (AXON_UNLIKELY(len > header_->max_payload)) {
    header_->push_failures.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  void* dst = try_acquire_write();
  if (AXON_UNLIKELY(dst == nullptr)) {
    return false;
  }
  std::memcpy(dst, data, len);
  commit_write(len);
  return true;
}

const void* ShmRing::try_acquire_read(std::uint32_t& out_len) noexcept {
  const std::uint64_t head = header_->head.load(std::memory_order_relaxed);

  if (AXON_UNLIKELY(head == cached_tail_)) {
    cached_tail_ = header_->tail.load(std::memory_order_acquire);
    if (head == cached_tail_) {
      return nullptr;
    }
  }

  std::byte* slot = slot_at(head);
  out_len = reinterpret_cast<const SlotHeader*>(slot)->length;
  return slot + kShmSlotHeaderSize;
}

void ShmRing::commit_read() noexcept {
  const std::uint64_t head = header_->head.load(std::memory_order_relaxed);
  header_->head.store(head + 1, std::memory_order_release);
}

bool ShmRing::try_pop(void* out, std::uint32_t cap,
                      std::uint32_t& out_len) noexcept {
  std::uint32_t len = 0;
  const void* src = try_acquire_read(len);
  if (src == nullptr) {
    return false;
  }
  if (AXON_UNLIKELY(len > cap)) {
    // Leave the message in the ring rather than dropping it. A caller that
    // sized its buffer wrongly should stall and be noticed, not lose an order.
    out_len = len;
    return false;
  }
  std::memcpy(out, src, len);
  out_len = len;
  commit_read();
  return true;
}

std::uint32_t ShmRing::max_payload() const noexcept {
  return header_ != nullptr ? header_->max_payload : 0;
}

std::uint32_t ShmRing::slot_count() const noexcept {
  return header_ != nullptr ? header_->slot_count : 0;
}

std::uint64_t ShmRing::push_failures() const noexcept {
  return header_ != nullptr
             ? header_->push_failures.load(std::memory_order_relaxed)
             : 0;
}

std::size_t ShmRing::size() const noexcept {
  if (header_ == nullptr) {
    return 0;
  }
  const std::uint64_t tail = header_->tail.load(std::memory_order_acquire);
  const std::uint64_t head = header_->head.load(std::memory_order_acquire);
  return static_cast<std::size_t>(tail - head);
}

}  // namespace axon::core
