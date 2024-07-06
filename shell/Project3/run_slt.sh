cd ../../build
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.05-index-scan.slt --verbose
