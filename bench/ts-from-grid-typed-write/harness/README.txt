# Compile-only shape check of the branch loops (not ClickHouse code: plain std::vector, not PODArray):
# clang++-20 -std=c++23 -O3 -march=x86-64-v3 -ffp-contract=off -fno-math-errno -falign-functions=64 -falign-loops=32 -mbranches-within-32B-boundaries -Wall -Wextra -c shape.cpp -o /tmp/shape.o -Rpass=loop-vectorize -Rpass-missed=loop-vectorize
# 2026-11-22: shape.cpp:20 vectorized (width 4 x Int64 for DateTime64, width 8 x UInt32); shape.cpp:25 (rows with NULL values, push_back) not vectorized.
