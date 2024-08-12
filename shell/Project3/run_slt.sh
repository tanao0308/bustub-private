cd ../../build
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.17-topn.slt --verbose
