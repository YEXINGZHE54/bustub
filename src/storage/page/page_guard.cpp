#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(std::exchange(that.bpm_, nullptr)),
      page_(std::exchange(that.page_, nullptr)),
      is_dirty_(std::exchange(that.is_dirty_, false)) {}

void BasicPageGuard::Drop() {
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (page_ != nullptr) {
    Drop();
  }
  bpm_ = std::exchange(that.bpm_, nullptr);
  page_ = std::exchange(that.page_, nullptr);
  is_dirty_ = std::exchange(that.is_dirty_, false);
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (page_ != nullptr) {
    Drop();
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)){};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() { guard_.Drop(); }

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)){};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() { guard_.Drop(); }

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
