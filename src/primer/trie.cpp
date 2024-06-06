#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
	// https://blog.csdn.net/qq_40878302/article/details/136005292
	/*
	dynamic_cast 是 C++ 中用于在运行时进行类型安全的类型转换操作符。它主要用于将基类的指针或引用转换为派生类的指针或引用。
	dynamic_cast 只能用于包含多态类型（即具有虚函数）的类，并且会在运行时检查转换是否成功。
	*/

	std::shared_ptr<const TrieNode> node = root_;

	for(char ch : key)
	{
		if(node == nullptr || node->children_.find(ch) == node->children_.end())
			return nullptr;
		node = node->children_.at(ch);
	}

	TrieNodeWithValue<T> const *value_node = dynamic_cast<const TrieNodeWithValue<T>*>(node.get());
	if(value_node != nullptr)
		return value_node->value_.get();
	return nullptr;

//	throw NotImplementedException("Trie::Get is not implemented.");
 
    // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
    // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
    // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
    // Otherwise, return the value.
}
//put的递归方法
// Put：递归方法具体实现
template <class T>
void PutCycle(const std::shared_ptr<bustub::TrieNode> &new_root, std::string_view key, T value) {
  // 判断元素是否为空的标志位
    bool flag = false;
  // 在new_root的children找key的第一个元素
    // 利用for(auto &a:b)循环体中修改a，b中对应内容也会修改及pair的特性
  for (auto &pair : new_root->children_) {
      // 如果找到了
      if (key.at(0) == pair.first) {
        flag = true;
      // 剩余键长度大于1
        if (key.size() > 1) {
        // 复制一份找到的子节点，然后递归对其写入
        std::shared_ptr<TrieNode> ptr = pair.second->Clone();
        // 递归写入 .substr(1,key.size()-1)也可以
        // 主要功能是复制子字符串，要求从指定位置开始，并具有指定的长度。
        PutCycle<T>(ptr, key.substr(1), std::move(value));
        // 覆盖原本的子节点
        pair.second = std::shared_ptr<const TrieNode>(ptr);
      } else {
          // 剩余键长度小于等于1，则直接插入
          // 创建新的带value的子节点
          std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
          TrieNodeWithValue node_with_val(pair.second->children_, val_p);
          // 覆盖原本的子节点
          pair.second = std::make_shared<const TrieNodeWithValue<T>>(node_with_val);
        }
      return;
      }
    }
  if (!flag) {
      // 没找到，则新建一个子节点
      char c = key.at(0);
      // 如果为键的最后一个元素
      if (key.size() == 1) {
        // 直接插入children
      std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
        new_root->children_.insert({c, std::make_shared<const TrieNodeWithValue<T>>(val_p)});
    } else {
      // 创建一个空的children节点
        auto ptr = std::make_shared<TrieNode>();
      // 递归
        PutCycle<T>(ptr, key.substr(1), std::move(value));
      // 插入
        new_root->children_.insert({c, std::move(ptr)});
    }
  }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // 第一种情况：值要放在根节点中，无根造根，有根放值
    // 更改根节点的值，即测试中的 trie = trie.Put<uint32_t>("", 123);key为空
  if (key.empty()) {
      std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
      // 建立新根
      std::unique_ptr<TrieNodeWithValue<T>> new_root = nullptr;
      // 如果原根节点无子节点
      if (root_->children_.empty()) {
        // 直接修改根节点
      new_root = std::make_unique<TrieNodeWithValue<T>>(std::move(val_p));
      } else {
        // 如果有，把原根的关系转移给新根：root_的children改为newRoot的children
      // 这里看tire.h里TireNodeWithValue方法，可以看到，传入不同数量的参数，对应实现不同的方法。
        new_root = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(val_p));
    }
    // 返回新的Trie
    return Trie(std::move(new_root));
  }
    // 第二种情况：值不放在根节点中
  // 2.1 根节点如果为空，新建一个空的TrieNode;
    // 2.2 如果不为空，调用clone方法复制根节点
  std::shared_ptr<TrieNode> new_root = nullptr;
    if (root_ == nullptr) {
    new_root = std::make_unique<TrieNode>();
  } else {
      new_root = root_->Clone();
    }
  // 递归插入，传递根节点，要放的路径：key, 要放入的值：value
    PutCycle<T>(new_root, key, std::move(value));
  // 返回新的Trie
    return Trie(std::move(new_root));
}
/*
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {

	std::shared_ptr<TrieNode> node = root_;
	std::unique_ptr<TrieNode> new_node = nullptr, new_root = nullptr;
	for(char ch : key)
	{
		if(node != nullptr && node->children_.find(ch) != node->children_.end())
		{
			if(new_root == nullptr)
				new_root = std::move(new_node);
			new_node = std::make_unique<TrieNode>();
			for(char child_ch : node->children_)
				if(child_ch != ch)
					new_node->children_[child_ch] = node->children_.at(child_ch);
		}
		else
			break;
	}
	return Trie(std::move(new_root));

//	std::cout<<"----------------I'm here---------------"<<std::endl;
//	while(1);

	// Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
	throw NotImplementedException("Trie::Put is not implemented.");

	// You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
	// exists, you should create a new `TrieNodeWithValue`.
}
*/
auto Trie::Remove(std::string_view key) const -> Trie {

//	std::cout<<"----------------I'm here---------------"<<std::endl;
//	while(1);



	throw NotImplementedException("Trie::Remove is not implemented.");

	// You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
	// you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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
