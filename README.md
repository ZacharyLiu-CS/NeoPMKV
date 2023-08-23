# NeoPMKV with PBRB and PLOG Design
## PMEM Configure
mount pmem device to /mnt/pmem0

## Build and Test
### Requirements
* Tools:
  * [CMake](https://cmake.org/download/) v3.14.5+
* Libraries:
  * [libpmem](https://pmem.io/pmdk/libpmem/) v1.11
  * [gtest](https://github.com/google/googletest) v1.12.1
  * [fmtlib](https://github.com/fmtlib/fmt) v9.1.0
  * [oneTBB](https://github.com/oneapi-src/oneTBB) v2021.7.0
## Build
```
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release/Debug -DBREAKDOWN=OFF/ON -DSTATISTICS=ON -DLOG_LEVEL=DEBUG/INFO/ERROR/DISABLE && make -j
```

## Test
```
make test
```
