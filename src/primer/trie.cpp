#include "primer/trie.h"
#include <cstddef>
#include <cstdio>
#include <map>
#include <memory>
#include <string_view>
#include <utility>

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }

  auto tmp_node = root_;
  for (auto ch : key) {
    if (tmp_node->children_.find(ch) != tmp_node->children_.end()) {
      // 按照key依次遍历Trie
      tmp_node = tmp_node->children_.at(ch);
    } else {
      // 若中途找不到子节点, 则查找失败, 直接返回nullptr
      return nullptr;
    }
  }

  auto result_node = dynamic_cast<const TrieNodeWithValue<T> *>(tmp_node.get());
  if (result_node == nullptr) {
    // 节点没有存储value, 返回nullptr
    return nullptr;
  }  // 返回value的指针
  return result_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> new_root;
  if (key.empty()) {
    // key为空
    if (root_ != nullptr) {
      // 已有根节点, 复制并插入值
      new_root = std::dynamic_pointer_cast<TrieNode>(
          std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value))));
    } else {
      // 原本为空树, 新建节点
      new_root = std::dynamic_pointer_cast<TrieNode>(
          std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value))));
    }
    return Trie(new_root);
  }

  if (root_ != nullptr) {
    // 已有根节点
    new_root = std::shared_ptr<TrieNode>(root_->Clone());
  } else {
    // 原本为空树
    new_root = std::make_unique<TrieNode>();
  }

  auto ptr = new_root;
  std::shared_ptr<TrieNode> node;
  for (size_t idx = 0; idx < key.length() - 1; idx++) {
    // 按照key依次遍历Trie
    auto ch = key[idx];
    // 中间节点
    if (auto it = ptr->children_.find(ch); it != ptr->children_.end()) {
      node = std::shared_ptr<TrieNode>(it->second->Clone());
    } else {
      node = std::make_unique<TrieNode>();
    }

    ptr->children_[ch] = node;
    ptr = node;
  }

  // 最后一个节点
  if (auto it = ptr->children_.find(key.back()); it != ptr->children_.end()) {
    // 插入位置在已有节点
    node = std::dynamic_pointer_cast<TrieNode>(
        std::make_shared<TrieNodeWithValue<T>>(it->second->children_, std::make_shared<T>(std::move(value))));
  } else {
    // 插入位置为新节点
    node = std::dynamic_pointer_cast<TrieNode>(
        std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value))));
  }

  ptr->children_[key.back()] = node;
  
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie(root_);
  }
  return Trie(Dfs(key, 0, root_));
}

auto Trie::Dfs(std::string_view key, size_t idx, const std::shared_ptr<const TrieNode> &ptr) const
    -> std::shared_ptr<const TrieNode> {
  std::shared_ptr<TrieNode> node;
  if (idx == key.length()) {
    // 递归出口, 查找到最后一个节点
    if (!ptr->children_.empty()) {
      // 若节点为非叶节点, 则拷贝, 删除存储的value
      node = std::make_shared<TrieNode>(ptr->children_);
    } else {
      // 叶节点则直接删除, 返回nullptr
      node = nullptr;
    }
    return node;
  }

  auto it = ptr->children_.find(key[idx]);
  if (it == ptr->children_.end()) {
    // 查询不到key的字符对应的节点, 则查找失败, 直接返回root_
    return root_;
  }

  auto child_node = Dfs(key, idx + 1, it->second);
  // 返回为root_, 代表查找失败, 直接返回
  if (child_node == root_) {
    return root_;
  }

  if (ptr->is_value_node_ || ptr->children_.size() > 1 || child_node != nullptr) {
    // 若节点存储了value, 或节点的子节点＞1, 或返回的子树不为nullptr, 则拷贝节点
    node = ptr->Clone();
    if (child_node != nullptr) {
      // 若子树不为nullptr, 则将子树连接到新节点
      node->children_[key[idx]] = child_node;
    } else {
      // 子树为nullptr, 则从子节点中删除
      node->children_.erase(key[idx]);
    }
  } else {
    node = nullptr;
  }

  return node;
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
