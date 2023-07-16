#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header = reinterpret_cast<const BPlusTreeHeaderPage *>(guard.GetData());
  return header->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header = reinterpret_cast<const BPlusTreeHeaderPage *>(guard.GetData());
  auto root_id = header->root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return false;
  }
  auto parent = bpm_->FetchPageRead(root_id);
  guard.Drop();

  // parent has alread hold a guard for some page, in loop starting
  const BPlusTreePage *page = nullptr;
  const InternalPage *internal = nullptr;
  auto idx = -1;
  while (true) {
    page = reinterpret_cast<const BPlusTreePage *>(parent.GetData());
    if (page->IsLeafPage()) {
      auto leaf = reinterpret_cast<const LeafPage *>(parent.GetData());
      idx = LeafKeyIndex(leaf, key);
      if (idx != -1) {
        result->emplace_back(leaf->ValueAt(idx));
        return true;
      }
      return false;
    }
    // internal page
    internal = reinterpret_cast<const InternalPage *>(parent.GetData());
    idx = InternalKeyIndex(internal, key);
    auto tmp = bpm_->FetchPageRead(internal->ValueAt(idx));
    parent = std::move(tmp);
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;

  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header = reinterpret_cast<BPlusTreeHeaderPage *>(ctx.header_page_->GetDataMut());
  if (header->root_page_id_ == INVALID_PAGE_ID) {  // empty tree
    auto guard = bpm_->NewPageGuarded(&(header->root_page_id_));
    auto leaf = reinterpret_cast<LeafPage *>(guard.GetDataMut());
    // init for new page
    leaf->Init(leaf_max_size_);
    // insert kv into new page
    leaf->IncreaseSize(1);
    leaf->SetKeyAt(0, key);
    leaf->SetValueAt(0, value);
    return true;
  }
  ctx.root_page_id_ = header->root_page_id_;
  auto root = bpm_->FetchPageWrite(ctx.root_page_id_);

  // in every loop start, root holds page write latch already
  BPlusTreePage *page = nullptr;
  InternalPage *internal = nullptr;
  LeafPage *leaf = nullptr;
  char *ptr = nullptr;
  auto idx = -1;
  auto res = 0;
  page_id_t pid, pid2, splited_pid;
  auto leafPos = -1;
  while (true) {
    ptr = root.GetDataMut();
    splited_pid = root.PageId();  // actually, it will be updated to leaf node pageid after loop, not too bad
    page = reinterpret_cast<BPlusTreePage *>(ptr);
    if (IsSafeModify(page, true)) {
      // release all in write set
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;
      }
      while (!ctx.write_set_.empty()) {
        ctx.write_set_.pop_front();
      }
    }
    ctx.write_set_.emplace_back(std::move(root));

    // internal page
    if (!page->IsLeafPage()) {
      internal = reinterpret_cast<InternalPage *>(ptr);
      idx = InternalKeyIndex(internal, key);
      root = bpm_->FetchPageWrite(internal->ValueAt(idx));  // jump to next level page
      continue;
    }

    // leaf page
    leaf = reinterpret_cast<LeafPage *>(ptr);
    for (int i = 0; i < leaf->GetSize(); i++) {
      res = comparator_(key, leaf->KeyAt(i));
      if (res == 0) {  // duplicate key
        return false;
      }
      if (res > 0) {
        continue;
      }
      // we have found target page and pos
      leafPos = i;
      break;
    }
    // after search leaf, we should break loop
    // special case: leaf page was found but it should be in right-most pos?
    if (leafPos == -1) {
      leafPos = leaf->GetSize();
    }
    break;
  }

  // key < key[i], this is where we should insert value
  if (IsSafeModify(leaf, true)) {  // no need to split first
    InsertIntoLeaf(leaf, leafPos, key, value);
    return true;
  }
  // we should split node
  // only when root node is leaf node, we has to record split_pid for it
  pid = SplitLeaf(leaf);
  auto guard = bpm_->FetchPageWrite(pid);
  auto leaf2 = reinterpret_cast<LeafPage *>(guard.GetDataMut());
  // has to decide which page inset, left or right?
  LeafPage *target = leaf;
  if (leafPos >= leaf->GetSize()) {
    target = leaf2;
    leafPos = leafPos - leaf->GetSize();
  }
  InsertIntoLeaf(target, leafPos, key, value);

  // modify internal now, prepare loop
  auto splitKey = leaf2->KeyAt(0);
  ctx.write_set_.pop_back();
  guard.Drop();
  // loop condition: splitKey and pid set
  while (!ctx.write_set_.empty()) {
    root = std::move(ctx.write_set_.back());
    internal = reinterpret_cast<InternalPage *>(root.GetDataMut());
    idx = InternalKeyIndex(internal, splitKey) + 1;
    if (IsSafeModify(internal, true)) {  // no need to split first
      InsertIntoInternal(internal, idx, splitKey, pid);
      break;
    }
    // we should split internal
    splited_pid = root.PageId();
    pid2 = SplitInternal(internal);
    guard = bpm_->FetchPageWrite(pid2);
    auto internal2 = reinterpret_cast<InternalPage *>(guard.GetDataMut());
    // has to decide which page inset, left or right?
    InternalPage *target = internal;
    if (idx >= internal->GetSize()) {
      target = internal2;
      idx = idx - internal->GetSize();
    }
    InsertIntoInternal(target, idx, splitKey, pid);

    // prepare loop
    splitKey = internal2->KeyAt(0);
    pid = pid2;
    ctx.write_set_.pop_back();
    guard.Drop();
  }
  // special case: what if we need to split root node?
  if (ctx.IsRootPage(splited_pid)) {
    auto guard = bpm_->NewPageGuarded(&(header->root_page_id_));
    auto internal = reinterpret_cast<InternalPage *>(guard.GetDataMut());
    // init for new page
    internal->Init(internal_max_size_);
    // insert kv into new page
    internal->IncreaseSize(2);
    internal->SetValueAt(0, splited_pid);
    internal->SetKeyAt(1, splitKey);
    internal->SetValueAt(1, pid);
    guard.Drop();
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header = reinterpret_cast<const BPlusTreeHeaderPage *>(guard.GetData());
  auto root_id = header->root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return End();  // return end()
  }
  auto parent = bpm_->FetchPageRead(root_id);
  guard.Drop();

  // parent has alread hold a guard for some page, in loop starting
  const BPlusTreePage *page = nullptr;
  const InternalPage *internal = nullptr;
  while (true) {
    page = reinterpret_cast<const BPlusTreePage *>(parent.GetData());
    if (page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(std::move(parent), 0, bpm_);
    }
    // internal page
    internal = reinterpret_cast<const InternalPage *>(parent.GetData());
    // directly jump to left most page
    auto tmp = bpm_->FetchPageRead(internal->ValueAt(0));
    parent = std::move(tmp);
  }
  // you should not reach here!
  return End();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header = reinterpret_cast<const BPlusTreeHeaderPage *>(guard.GetData());
  auto root_id = header->root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    return End();
  }
  auto parent = bpm_->FetchPageRead(root_id);
  guard.Drop();

  // parent has alread hold a guard for some page, in loop starting
  const BPlusTreePage *page = nullptr;
  const InternalPage *internal = nullptr;
  auto idx = -1;
  while (true) {
    page = reinterpret_cast<const BPlusTreePage *>(parent.GetData());
    if (page->IsLeafPage()) {
      auto leaf = reinterpret_cast<const LeafPage *>(parent.GetData());
      idx = LeafKeyIndex(leaf, key);
      if (idx != -1) {
        return INDEXITERATOR_TYPE(std::move(parent), idx, bpm_);
      }
      // not found, return end()
      return End();
    }
    // internal page
    internal = reinterpret_cast<const InternalPage *>(parent.GetData());
    idx = InternalKeyIndex(internal, key);
    auto tmp = bpm_->FetchPageRead(internal->ValueAt(idx));
    parent = std::move(tmp);
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header = reinterpret_cast<const BPlusTreeHeaderPage *>(guard.GetData());
  return header->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

/**
 *
 * @param key The key to search for
 * @return i, where k[i] <= key < k[i+1]
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalKeyIndex(const InternalPage *page, const KeyType &key) const -> int {
  int i = 1;
  for (; i < page->GetSize(); i++) {
    KeyType k = page->KeyAt(i);
    if (comparator_(key, k) < 0) {
      return i - 1;
    }
  }
  // larger than any key
  if (i == page->GetSize()) {
    return page->GetSize() - 1;
  }
  return 0;
}

/**
 *
 * @param key The key to search for
 * @return i, where k[i] == key
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafKeyIndex(const LeafPage *leaf, const KeyType &key) const -> int {
  for (int i = 0; i < leaf->GetSize(); i++) {
    KeyType k = leaf->KeyAt(i);
    if (comparator_(key, k) == 0) {
      return i;
    }
  }
  return -1;
}

/**
 * @param page page modified
 * @param isInsert is insert or delete
 * @return true if safe to unlock parent
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafeModify(const BPlusTreePage *page, bool isInsert) const -> bool {
  if (isInsert) {
    return page->GetSize() + 1 <= page->GetMaxSize();
  }
  return page->GetSize() >= page->GetMinSize() + 1;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MoveLeafChild(LeafPage *fromPage, int fromPos, LeafPage *ToPage, int toPos, int size) {
  for (; size > 0; --size) {  // move kv
    ToPage->SetKeyAt(toPos, fromPage->KeyAt(fromPos));
    ToPage->SetValueAt(toPos, fromPage->ValueAt(fromPos));
    --fromPos;
    --toPos;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MoveInternalChild(InternalPage *fromPage, int fromPos, InternalPage *ToPage, int toPos, int size) {
  for (; size > 0; --size) {  // move kv
    ToPage->SetKeyAt(toPos, fromPage->KeyAt(fromPos));
    ToPage->SetValueAt(toPos, fromPage->ValueAt(fromPos));
    --fromPos;
    --toPos;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf) -> page_id_t {
  page_id_t pid;
  auto newSize = leaf->GetMinSize();
  auto remainSize = leaf->GetSize() - newSize;
  auto guard = bpm_->NewPageGuarded(&pid);
  auto leaf2 = reinterpret_cast<LeafPage *>(guard.GetDataMut());
  // init for new page
  leaf2->Init(leaf_max_size_);
  leaf2->SetSize(newSize);
  // copy child
  MoveLeafChild(leaf, leaf->GetSize() - 1, leaf2, leaf2->GetSize() - 1, newSize);
  leaf->SetSize(remainSize);
  return pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal) -> page_id_t {
  page_id_t pid;
  auto newSize = internal->GetMinSize();
  auto remainSize = internal->GetSize() - newSize;
  auto guard = bpm_->NewPageGuarded(&pid);
  auto internal2 = reinterpret_cast<InternalPage *>(guard.GetDataMut());
  // init for new page
  internal2->Init(internal_max_size_);
  internal2->SetSize(newSize);
  // copy child
  MoveInternalChild(internal, internal->GetSize() - 1, internal2, internal2->GetSize() - 1, newSize);
  internal->SetSize(remainSize);
  return pid;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoLeaf(LeafPage *leaf, int i, const KeyType &key, const ValueType &value) {
  leaf->IncreaseSize(1);
  MoveLeafChild(leaf, leaf->GetSize() - 1 - 1, leaf, leaf->GetSize() - 1, leaf->GetSize() - 1 - i);
  leaf->SetKeyAt(i, key);
  leaf->SetValueAt(i, value);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternal(InternalPage *internal, int i, const KeyType &key, const page_id_t value) {
  internal->IncreaseSize(1);
  MoveInternalChild(internal, internal->GetSize() - 1 - 1, internal, internal->GetSize() - 1,
                    internal->GetSize() - 1 - i);
  internal->SetKeyAt(i, key);
  internal->SetValueAt(i, value);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
