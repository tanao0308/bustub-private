cd ../../build
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.10-simple-join.slt --verbose
