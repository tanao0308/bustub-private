cd ../../build
make txn_scan_test -j$(nproc)

./test/txn_scan_test
