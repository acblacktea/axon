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

## 4. 当前的编译参数，以及还能怎么往上开

上面两组命令编出来的，就是 `test/bin` 里用的版本：

| | 下单引擎 | 行情服务 |
|---|---|---|
| 构建类型 | Release | Release |
| 优化级别 | `-O3 -DNDEBUG` | `-O3 -DNDEBUG` |
| 针对本机 CPU（`-march=native`） | 关 | 关 |
| 链接时优化（LTO） | 关 | 关 |
| 保留帧指针 | 是（`-fno-omit-frame-pointer`） | 否 |

下单引擎保留帧指针是有意为之：损失一个寄存器，换来能在生产环境用 `perf` 剖析热路径。建议不要去掉。

> 下面三种方式都**还没有实测过**，命令也没有在这台机器上验证过。开之前和开之后，都用
> `./build/bench/axon_bench` 和行情服务的指标（`axon_mds_handler_duration_seconds`）各量一遍，
> 有收益再用。内部延迟只占端到端的 1–5%，提升 10–20% 对端到端的影响也有限。

### 4.1 `-march=native`：针对本机 CPU 指令集

编译器可以使用这台 CPU 支持的全部指令集（比如 AVX-512）。**编出来的程序只能在同型号 CPU 上运行**，
换一台机器可能直接因为非法指令崩溃，所以要在和部署机器同型号的机器上编译。

```bash
# 下单引擎：现成的 prod 预设，产物在 build-prod/
cd cpp/axon_order_execution
CC=gcc CXX=g++ cmake --preset prod
cmake --build build-prod -j$(nproc)

# 行情服务：没有预设，直接加编译参数，产物放在单独的目录
cd cpp/axon_market_data
CC=gcc CXX=g++ cmake -B build-native -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="-march=native"
cmake --build build-native -j$(nproc)
```

### 4.2 LTO：链接时跨文件优化

允许编译器跨源文件内联，比如把 venue 解析器、OrderStore、EMS 之间的调用内联掉。代价是编译变慢。

```bash
# 在上面任意一条 cmake 配置命令后面加：
-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON
```

下单引擎开着 `-Werror`，LTO 可能多报出一些警告，导致编译失败，届时要逐个看。

### 4.3 PGO：按实际运行剖析结果优化

用真实负载跑一遍、收集剖析数据，再拿这份数据重新编译。通常收益最大，流程也最麻烦，
建议等热路径冻结以后再做。GCC 的流程如下：

```bash
# 1. 带插桩编译
cmake -B build-pgo-gen ... -DCMAKE_CXX_FLAGS="-fprofile-generate=$PWD/pgo"
cmake --build build-pgo-gen -j$(nproc)
# 2. 用有代表性的负载跑一段时间（行情：连真实交易所；引擎：测试网或回放），正常退出以写出剖析数据
# 3. 用剖析数据重新编译
cmake -B build-pgo -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="-fprofile-use=$PWD/pgo -fprofile-partial-training"
cmake --build build-pgo -j$(nproc)
```

用新参数编出来的程序，要拷到 `test/bin` 才会在端到端测试里生效，见 `test/README.md`。
