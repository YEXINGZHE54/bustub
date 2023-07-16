/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : bpm_(nullptr), guard_(nullptr, nullptr), pos_(0){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard &&guard, int32_t pos, BufferPoolManager *bpm)
    : bpm_(bpm), guard_(std::move(guard)), pos_(pos) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return (guard_.PageId() == itr.guard_.PageId() && pos_ == itr.pos_);
}

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return !(guard_.PageId() == itr.guard_.PageId() && pos_ == itr.pos_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return guard_.PageId() == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto leaf = reinterpret_cast<const B_PLUS_TREE_LEAF_PAGE_TYPE *>(guard_.GetData());
  return leaf->GetMapping(pos_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ++pos_;
  auto leaf = reinterpret_cast<const B_PLUS_TREE_LEAF_PAGE_TYPE *>(guard_.GetData());
  if (pos_ < leaf->GetSize()) {
    return *this;
  }
  pos_ = 0;
  if (leaf->GetNextPageId() == INVALID_PAGE_ID) {
    guard_.Drop();
    return *this;  // reached end
  }
  // try next page
  auto guard_ = bpm_->FetchPageRead(leaf->GetNextPageId());
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
