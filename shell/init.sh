cd ..
rm -r build
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j`nproc`

replacement="cd /root/Github/myPrograms/bustub-private && zip project0-submission.zip src/planner/plan_func_call.cpp src/include/execution/expressions/string_expression.h src/include/primer/trie.h src/include/primer/trie_store.h src/include/primer/trie_answer.h src/primer/trie.cpp src/primer/trie_store.cpp"

printf "%s\n" "$replacement" | sed -i "70s|.*|$(cat -)|" CMakeFiles/submit-p0.dir/build.make
