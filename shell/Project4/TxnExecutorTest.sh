cd ../../build
make txn_executor_test -j$(nproc)

./test/txn_executor_test
