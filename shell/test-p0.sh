cd ..
cd build

# -j$(nproc) 参数是用于并行化编译和执行的选项。
# -j 选项告诉 Make 工具并行地执行编译任务，而 $(nproc) 表示使用当前系统上可用的 CPU 核心数量来确定并行任务的数量。
# 这样可以加快构建和测试的速度。

make orset_test -j$(nproc)
make trie_test -j$(nproc)
make trie_noncopy_test -j$(nproc)
make trie_store_test -j$(nproc)
make trie_store_noncopy_test -j$(nproc)
make trie_debug_test -j$(nproc)

# ./test/trie_test
# ./test/trie_noncopy_test
# ./test/trie_store_test
# ./test/trie_store_noncopy_test
