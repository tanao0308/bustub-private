cd ../../build
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.16-sort-limit.slt --verbose
