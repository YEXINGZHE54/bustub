#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> node(root_);
  std::map<char, std::shared_ptr<const TrieNode>>::const_iterator iter;
  for (auto c : key) {
    iter = node->children_.find(c);
    if (iter != node->children_.cend()) {
      node = iter->second;
    } else {
      node = nullptr;
    }
    if (node == nullptr) {
      return nullptr;
    }
  }
  if (!node->is_value_node_) {
    return nullptr;
  }
  auto ptr = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (ptr == nullptr) {
    return nullptr;
  }
  return ptr->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  auto val = std::make_shared<T>(std::move(value));
  // special case: key is empty, we directly store value to root!
  if (key.empty()) {
    std::shared_ptr<TrieNodeWithValue<T>> root(nullptr);
    if (root_ == nullptr) {
      root = std::make_shared<TrieNodeWithValue<T>>(val);
    } else {
      root = std::make_shared<TrieNodeWithValue<T>>(root_->children_, val);
    }
    return Trie(root);
  }
  // key is not empty now, we can ensure to update children on some leaf
  std::shared_ptr<TrieNode> root(nullptr);
  if (root_ != nullptr) {
    root = root_->Clone();
  } else {
    root = std::make_shared<TrieNode>();
  }
  std::shared_ptr<TrieNode> node(root);
  std::shared_ptr<const TrieNode> next(nullptr);
  std::shared_ptr<TrieNode> ptr(nullptr);
  std::shared_ptr<TrieNodeWithValue<T>> leaf(nullptr);
  std::map<char, std::shared_ptr<const TrieNode>>::const_iterator iter;
  char last_c = ' ';
  bool first = true;
  for (auto c : key) {
    if (!first) {
      if (next != nullptr) {
        ptr = next->Clone();
      } else {
        ptr = std::make_shared<TrieNode>();
      }
      node->children_[last_c] = ptr;
      node = ptr;
    }
    first = false;
    iter = node->children_.find(c);
    if (iter != node->children_.cend()) {
      next = iter->second;
    } else {
      next = nullptr;
    }
    last_c = c;
  }
  // now, we reach end of key, next is our target
  if (next != nullptr) {
    leaf = std::make_shared<TrieNodeWithValue<T>>(next->children_, val);
  } else {
    leaf = std::make_shared<TrieNodeWithValue<T>>(val);
  }
  node->children_[last_c] = leaf;
  return Trie(root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  // Remove is same as Put, except update
  if (root_ == nullptr) {
    return Trie(root_);
  }
  // special case: key is empty
  if (key.empty()) {
    if (root_->children_.empty()) {
      return Trie(nullptr);  // root is deleted leaf now
    }
    auto root = std::make_shared<TrieNode>(root_->children_);
    return Trie(root);
  }

  // now key is not empty, we can ensure to update children in stack
  std::shared_ptr<TrieNode> root = root_->Clone();
  std::shared_ptr<TrieNode> node(root);
  std::shared_ptr<const TrieNode> next(nullptr);
  std::map<char, std::shared_ptr<const TrieNode>>::const_iterator iter;
  std::stack<std::shared_ptr<TrieNode>> stack;
  for (auto c : key) {
    stack.push(node);
    iter = node->children_.find(c);
    if (iter != node->children_.cend()) {
      next = iter->second;
    } else {
      next = nullptr;
    }
    if (next == nullptr) {
      return Trie(root_);
    }
    node = next->Clone();
  }
  // now, we reach end of key, node is our target and it should has value
  if (!node->is_value_node_) {
    return Trie(root_);
  }
  // now node has no value
  if (node->children_.empty()) {
    // if node has no children(means it's deleted leaf now)
    // we should go back to check if parent is also deleted leaf(no value and children size = 1)
    while (!stack.empty()) {
      node = stack.top();
      if (node->is_value_node_ || node->children_.size() > 1) {
        break;
      }
      stack.pop();
    }
  } else {
    // node is not leaf, convert it to no-value node
    // push it to stack, to chain it in new Trie
    stack.push(std::make_shared<TrieNode>(node->children_));
  }
  // only one node, deleted
  if (stack.empty()) {
    return Trie(nullptr);
  }
  std::stack<std::shared_ptr<TrieNode>> reversed;
  while (!stack.empty()) {
    reversed.push(stack.top());
    stack.pop();
  }
  // we always asume: reversed is not empty
  for (auto c : key) {
    node = reversed.top();
    reversed.pop();
    // so we has to check reversed empty, after pop
    if (reversed.empty()) {
      node->children_.erase(c);
      break;
    }
    node->children_[c] = reversed.top();
  }
  return Trie(root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
