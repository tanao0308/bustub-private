cd ../../build
make -j$(nproc) sqllogictest
./bin/bustub-sqllogictest ../test/sql/p3.01-seqscan.slt --verbose
