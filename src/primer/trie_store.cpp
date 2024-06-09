#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  //	用于避免持有锁的时间过长，从而减少锁竞争，提高并发性能。
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  /*
    std::shared_ptr<const TrieNode> root_copy;
      auto trie = Trie();
    // 使用互斥锁来保护根节点的读取
      std::lock_guard<std::mutex> lock(root_lock_);
    // 获取Trie的根节点
      trie = root_;
    // 在Trie中查找键
      const T *value = trie.Get<T>(key);  // 假设TrieNode有Get方法
    // 根据查找结果返回
      if (value) {
      return ValueGuard<T>(trie, *value);
    }
      return std::nullopt;
  */

  // 获取根节点锁，从旧根节点读取数据
  std::lock_guard<std::mutex> root_lock(root_lock_);

  // 注意这里需要显式指定Get函数的模板T
  const T *value = root_.Get<T>(key);

  if (value == nullptr) return std::nullopt;
  return ValueGuard<T>(root_, *value);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.

  // 获取写锁，修改的时候不能有其他线程写
  std::lock_guard<std::mutex> write_lock(write_lock_);
  Trie trie = root_.Put<T>(key, std::move(value));

  // 获取根节点锁
  std::lock_guard<std::mutex> root_lock(root_lock_);
  root_ = trie;
  return;
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.

  // 获取写锁，修改的时候不能有其他线程写
  std::lock_guard<std::mutex> write_lock(write_lock_);
  Trie trie = root_.Remove(key);

  // 获取根节点锁
  std::lock_guard<std::mutex> root_lock(root_lock_);
  root_ = trie;
  return;
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
