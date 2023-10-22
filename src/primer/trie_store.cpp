#include "primer/trie_store.h"
#include <mutex>
#include <optional>

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  // throw NotImplementedException("TrieStore::Get is not implemented.");
  std::unique_lock<std::mutex> lk(this->root_lock_);
  auto trie = this->root_;
  lk.unlock();

  if (auto val_ptr = trie.Get<T>(key); val_ptr != nullptr) {
    return std::make_optional<ValueGuard<T>>(trie, *val_ptr);
  }
  return std::nullopt;
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Put is not implemented.");
  // 先获取write_lock_, 再获取root_lock_, 防止并发插入时, 后来的线程获得旧的Trie
  std::lock_guard<std::mutex> lk(this->write_lock_);

  // 获取到trie后立刻释放root_lock_, 让读取线程可以读旧的trie
  std::unique_lock<std::mutex> ul(this->root_lock_);
  auto trie = root_;
  ul.unlock();

  auto new_trie = trie.Put(key, std::move(value));

  ul.lock();
  this->root_ = new_trie;
  ul.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Remove is not implemented.");
  // 先获取write_lock_, 再获取root_lock_, 防止并发删除时, 后来的线程获得旧的Trie
  std::lock_guard<std::mutex> lk(this->write_lock_);

  // 获取到trie后立刻释放root_lock_, 让读取线程可以读旧的trie
  std::unique_lock<std::mutex> ul(this->root_lock_);
  auto trie = this->root_;
  ul.unlock();

  auto new_trie = trie.Remove(key);

  ul.lock();
  this->root_ = new_trie;
  ul.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
