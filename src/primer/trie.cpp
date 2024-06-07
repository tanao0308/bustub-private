#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

void dfs(std::shared_ptr<const TrieNode> root, int dep=0)
{
	std::cout<<"depth="<<dep<<", "<<root->is_value_node_<<std::endl;
	for(auto iter : root->children_)
	{
		std::cout<<"->"<<iter.first<<std::endl;
		dfs(iter.second, dep+1);
	}
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
	// https://blog.csdn.net/qq_40878302/article/details/136005292
	/*
	dynamic_cast 是 C++ 中用于在运行时进行类型安全的类型转换操作符。它主要用于将基类的指针或引用转换为派生类的指针或引用。
	dynamic_cast 只能用于包含多态类型（即具有虚函数）的类，并且会在运行时检查转换是否成功。
	*/
//	throw NotImplementedException("Trie::Put is not implemented.");

//	std::cout<<"Get: "<<key<<std::endl;

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
	
//	std::cout<<"Get return nullptr"<<std::endl;
	return nullptr;

//	throw NotImplementedException("Trie::Get is not implemented.");
 
    // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
    // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
    // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
    // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
	
//	std::cout<<"Put: "<<key<<std::endl;

	// 第一种情况：值要放在根节点中，无根造根，有根放值
	// 更改根节点的值
	if (key.empty())
	{
		// 新根的值
		std::shared_ptr<T> new_value = std::make_shared<T>(std::move(value));

		// 建立新根
		std::unique_ptr<TrieNodeWithValue<T>> new_root = nullptr;
		
		// 将原根节点的孩子节点继承给新根，并给新根加上新值
		new_root = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(new_value));
		
		// 返回新的Trie
		return Trie(std::move(new_root));
	}

    // 第二种情况：值不放在根节点中
	// 根节点如果为空，新建一个空的TrieNode;
	std::shared_ptr<TrieNode> new_root = nullptr;
	if (root_ == nullptr)
		new_root = std::make_unique<TrieNode>();
	// 根节点不为空，调用clone方法复制根节点
	else
		new_root = root_->Clone();

	// 递归插入，传递根节点，要放的路径：key, 要放入的值：value
    // PutCycle<T>(new_root, key, std::move(value));
	std::shared_ptr<const TrieNode> node = root_;
	std::shared_ptr<TrieNode> new_node = new_root;
	std::shared_ptr<TrieNode> new_par_node = nullptr;
	
	char last_ch;
	for(auto ch : key)
	{
//		std::cout<<"ch="<<ch<<std::endl;
		if(node != nullptr && node->children_.find(ch) != node->children_.end())
		{
			new_node->children_[ch] = std::const_pointer_cast<TrieNode>( node->children_.find(ch)->second )->Clone();
			node = node->children_.at(ch);
			// 不能用[]，因为如果键不存在，[]会插入一个默认值
			// node = node->children_[ch];
		}
		else
		{
			node = nullptr;
//			std::cout<<"node is nullptr"<<std::endl;
			new_node->children_[ch] = std::make_shared<TrieNode>();
		}
		last_ch = ch;
		new_par_node = new_node;
		new_node = std::const_pointer_cast<TrieNode>(new_node->children_[ch]);
	}

	/*
	为什么使用 std::move：
	    性能优化：如果 T 是一个较大的对象或包含动态分配的资源，使用移动构造函数可以避免昂贵的拷贝操作，提高性能。
		独占资源：移动语义确保 value 在移动后不再持有原资源，这避免了重复使用同一资源的风险。
	new_value 是一个 std::shared_ptr，它指向一个类型为 T 的对象，该对象在堆上分配，并且 value 的内容已经被移动到这个新对象中。
	*/
    std::shared_ptr<T> new_value = std::make_shared<T>(std::move(value));
    TrieNodeWithValue new_value_node(new_node->children_, new_value);
    // 覆盖原本的子节点
    new_par_node->children_[last_ch] = std::make_shared<const TrieNodeWithValue<T>>(new_value_node);

//	dfs(new_root);

	// 返回新的Trie
    return Trie(std::move(new_root));
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
