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

	TrieNodeWithValue<T> const *value_node = dynamic_cast<TrieNodeWithValue<T>*>(node.get());
	if(value_node != nullptr)
		return value_node->value_.get();
	return nullptr;

//	throw NotImplementedException("Trie::Get is not implemented.");
 
    // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
    // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
    // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
    // Otherwise, return the value.
}

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
