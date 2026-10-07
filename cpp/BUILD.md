# 用 GCC 在 Linux 上编译

适用于 Ubuntu 26.04 + GCC 15。依赖来自系统包，外加两个从源码装的库，不走 vcpkg。

## 1. 安装依赖（只需做一次）

```bash
sudo apt-get install -y g++ cmake ninja-build pkg-config libssl-dev libyaml-cpp-dev \
  libspdlog-dev libfmt-dev libzmq3-dev cppzmq-dev libpq-dev nlohmann-json3-dev \
  libgtest-dev libgmock-dev libboost-dev libsimdjson-dev libglaze-dev zlib1g-dev
```

apt 里没有 prometheus-cpp，要从源码装：

```bash
git clone --depth 1 --branch v1.3.0 --recurse-submodules https://github.com/jupp0r/prometheus-cpp.git
cmake -S prometheus-cpp -B prometheus-cpp/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=OFF -DENABLE_PUSH=OFF -DENABLE_COMPRESSION=OFF -DENABLE_TESTING=OFF
cmake --build prometheus-cpp/build && sudo cmake --install prometheus-cpp/build
```

apt 的 `libpqxx-dev` 没有 CMake 配置文件，所以从源码装同一版本。如果之前用 apt 装过它，先卸掉，
免得 `/usr/include` 和 `/usr/local/include` 里的两份头文件混用：

```bash
sudo apt-get remove -y libpqxx-dev
git clone --depth 1 --branch 7.10.0 https://github.com/jtv/libpqxx.git
cmake -S libpqxx -B libpqxx/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DSKIP_BUILD_TEST=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_DOC=OFF
cmake --build libpqxx/build && sudo cmake --install libpqxx/build
```

上面两个仓库克隆到哪个目录都可以，装完就能删掉。

## 2. 下单服务

```bash
cd cpp/axon_order_execution
CC=gcc CXX=g++ cmake --preset default     # Release，带测试和基准，开 -Werror
cmake --build build -j$(nproc)
ctest --test-dir build -j$(nproc)
```

产物：`build/axon_engine`、`build/axon_strategy`、`build/bench/axon_bench`

## 3. 行情服务

```bash
cd cpp/axon_market_data
CC=gcc CXX=g++ cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
ctest --test-dir build -j$(nproc)
```

产物：`build/axon_market_data`、`build/mds_subscriber`、`build/tests/mds_tests`
