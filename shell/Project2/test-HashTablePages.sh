cd ../../build
make extendible_htable_test -j$(nproc)
make extendible_htable_concurrent_test # -j$(nproc)
make extendible_htable_page_test -j$(nproc)

./test/extendible_htable_test
./test/extendible_htable_concurrent_test
./test/extendible_htable_page_test
