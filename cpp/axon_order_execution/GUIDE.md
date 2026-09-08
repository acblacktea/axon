# axon_order_execution_cpp 上手指南

> 这份文档回答**「这是什么、怎么搭起来的、怎么用」**。
> 想知道**「为什么这么做」**——选型论证、实测数据、各交易所的坑、被测试抓到的 bug——
> 看 [README.md](README.md)。

---

## 一、这是什么

一个**加密期权 / 永续合约的订单执行引擎**，C++20 实现，是同仓库那份
[Python 引擎](../../python/axon_order_execution/)的低延迟移植版。

它做三件事：

1. **连交易所**——Deribit（期权）、Binance / Bybit / OKX（U 本位永续），
   维护私有 WebSocket 会话，收订单、成交、持仓推送。
2. **管状态**——本地订单簿、成交去重、账户持仓，落 Postgres，
   并定期用 REST 和交易所对账，兜底 WebSocket 丢消息。
3. **接策略**——策略跑在**独立进程**里，通过 ZMQ（通用）或共享内存（同机、更快）
   下单和收回报。

**目标是内部延迟 10 µs 以内。** 这个数字决定了几乎所有实现选择：手写 WebSocket
而不用 Boost.Beast、定点数而不用 double、单线程忙轮询而不用线程池、
热路径不分配内存。

### 当前状态

| | |
|---|---|
| 传输层（TCP / TLS / WebSocket） | ✅ 手写 RFC 6455，对着 loopback 上的真 TLS 服务验证过 |
| 四家交易所协议 | ✅ 入站解析 + 出站构造，对着抓下来的报文验证过 |
| OMS / EMS / 对账 / 持久化 / 控制面 | ✅ |
| 下单 | ✅ **四家全走 WebSocket，没有 REST 下单** |
| 测试 | ✅ 395 个，ASan / TSan 均干净 |
| **连过真实交易所** | ❌ **从未** |

> ⚠️ **三家永续的 WebSocket 下单映射抄自交易所文档，不是抄自那份在生产上跑了很久的
> Python（它三家都走 REST）。REST 下单已被删除，所以旁边没有一条已验证的路可退。
> 拿各家 testnet 验一遍是上线前提，不是后续事项。**

---

## 二、架构

### 2.1 进程模型

```
  策略进程 A (C++)          策略进程 B (Python)        策略进程 C (C++, 同机)
  StrategyClient            StrategyClient             StrategyClient
    DEALER + SUB              DEALER + SUB               共享内存环
         \                        /                            |
          -- ZMQ ROUTER :5555 ----                             |  ~0.1-0.3 µs
          -- ZMQ PUB    :5556 ----                             |  单向、无 JSON
                 |  ~30-60 µs 往返                             |
        ┌────────┴────────────────────────────────────────────┴─────────┐
        │                    axon_engine（单进程、单线程）              │
        │                                                                │
        │  VenueSession × N ── 私有行情流（订单/成交/持仓）                │
        │  TradeSession × N ── 下单专用连接（Binance / Bybit）             │
        │  EmsService      ── 下单 / 撤单 / 改单                          │
        │  OrderStore / FillStore / PortfolioStore ── 本地状态             │
        │  Reconciler      ── 定期 REST 对账，补 WS 丢的消息               │
        │  PostgresWriter  ── 专用写线程，挂在 SpscRing 后面                │
        │  ZmqServer       ── 控制面   │   ShmBridge ── 数据面             │
        └────────────────────────────┬───────────────────────────────────┘
                                     |
            Deribit / Binance / Bybit / OKX
```

Python 策略能连这个引擎**不用改一行代码**——两边的 JSON 控制面逐字节兼容，
有 `python_wire_compat` 测试守着。

### 2.2 分层：下层不许碰上层的依赖

这是这个项目最重要的结构约束，由构建系统强制。

| 层 | 能链接什么 | 内容 |
|---|---|---|
| `core/` | **只有 libc++ / pthread** | 定点数、时间戳、TSC 时钟、HDR 直方图、SPSC 环、共享内存环、对象池、绑核 |
| `models/` | core | 领域类型 + 枚举字符串表 |
| `transport/` | models, nlohmann | 控制面 JSON 编解码、数据面 POD 消息、ZMQ 服务、共享内存桥 |
| `net/` | core, OpenSSL | 手写 WebSocket、非阻塞 TCP、TLS（memory BIO）、SHA-1/SHA-256/HMAC/base64 |
| `venue/` | transport, simdjson, net | 四家的入站解析和出站构造 |
| `app/` | 以上全部 + yaml-cpp / spdlog / prometheus-cpp / cppzmq / libpqxx | 配置、日志、指标、订单状态、会话、对账、持久化 |

**为什么值得这么严：** `core/` 和 `models/` 不链接任何 JSON 库，所以热路径代码
**在编译期就没有能力**误碰到 DOM 解析器。这不是靠代码规范，是靠链接失败。

顺带一提，两个 JSON 库各司其职：

- **nlohmann**（控制面）——冷路径，需要一个可拥有、可变、任意形状的 DOM，还要能**产出** JSON
- **simdjson On-Demand**（行情解析）——热路径，只读、只进游标，只碰你问到的字段
- **出站订单不用 JSON 库**——字节模板 + 定点数直写，12 ns

### 2.3 两个平面

| | 控制面 | 数据面（热路径） |
|---|---|---|
| 传输 | ZMQ DEALER/ROUTER + PUB/SUB | 共享内存 SPSC 环（mmap） |
| 编码 | JSON（nlohmann） | 定长 POD，memcpy |
| 延迟 | ~30–60 µs 往返 | **~0.1–0.3 µs 单向** |
| 承载 | 所有查询、订阅、Python 策略 | 只有 place / cancel / modify / order_update / fill |
| 语义 | 请求-响应 | **发出去没有回执**，结果稍后作为 order_update 回来 |

实测差距就是拆开它们的理由：构造一条下单消息，POD **37.5 ns** vs JSON 2125 ns（57×）；
消费一条，指针转换 **0.29 ns** vs 解析 2417 ns（~8000×）。

数据面**只放那 5 种消息**，其余一律走 JSON——JSON 那边加字段是免费的，
POD 那边加字段是破坏性变更。

数据面的三条硬约定：

- **定长字符串装不下就拒绝，不截断**——截断的合约名会把单发到错误的合约上
- **背压是丢弃 + 计数，不是阻塞**——为一个卡住的策略阻塞引擎循环，
  会把每一路行情一起干掉
- **消费者必须忙轮询**——阻塞在条件变量上等于把 2–10 µs 的 futex 唤醒还回去，
  结果比 ZMQ ipc 还差

### 2.4 引擎主循环：单线程、忙轮询

[src/engine_main.cpp](src/engine_main.cpp)。Python 跑的是 asyncio，每个交易所一个 task、
每条命令一个 task；这里跑的是**一个循环**：

```cpp
while (!stop) {
  for (auto& s : sessions_)      s->poll();   // 各家行情流
  for (auto& s : trade_sessions_) s->poll();  // 下单专用连接
  http_->poll();                              // 对账用的 REST
  ems_->poll();                               // 请求超时
  for (auto& [_, r] : reconcilers_) r->poll();
  shm_.poll();                                // 策略推来的命令
  zmq_.poll();                                // 控制面
  cpu_pause();
}
```

**从交易所消息到订单簿的路径上，没有调度器、没有唤醒、没有跨线程跳转。**
代价是那个核 CPU 常驻 100%——这是尾延迟可控的入场费，也是 `cpp.engine_core` 存在的理由。

唯一的另一个线程是 Postgres 写线程，它挂在 SpscRing 后面，完全不在热路径上。

### 2.5 一条订单更新的一生

```
交易所 WebSocket 帧
   → ws_frame 解帧（零拷贝，指针指向接收缓冲区内部）
   → venue parser（simdjson On-Demand，直接从 socket 缓冲区读那 6 个字段）
   → OrderUpdateMsg（定长 POD）
   → ① ShmBridge：字节先发给同机策略        ← 先做这个
     ② OrderStore：转成领域 Order，写缓存 + 落库
     ③ ZmqServer：转 JSON，按 strategy_id 广播
```

**顺序是刻意的**：①在②③之前，因为②③是控制面的副本，都不在策略的关键路径上。

### 2.6 下单路径：全部走 WebSocket

**没有 REST 下单。** 每一笔下单/撤单/改单都走一条已建立、已鉴权的 WebSocket 连接。
四家的落点结构上完全不同：

| | 端点 | 连接 | 鉴权 |
|---|---|---|---|
| **Deribit** | `private/buy` / `cancel` / `edit` | 它唯一那条连接 | 登录一次 |
| **OKX** | `op:"order"` | **复用** `/ws/v5/private` | 登录一次 |
| **Bybit** | `/v5/trade` | **另开一条** | 该连接单独 `op:"auth"` |
| **Binance** | `ws-fapi.binance.com` | **另开一条**（不同主机） | **无会话登录，每条请求自带签名** |

⚠️ **这意味着 session 不在线 = 这家不能下单。** 下单可用性从此**完全等于**连接可用性。
`axon_ws_connected` 和重连计数不再只是描述行情流，它们现在就是下单健康度。

REST 只剩定时的兜底读：对账快照、持仓刷新、行情、Binance 的 listenKey。
**这个进程里任何一次 HTTP 请求都不在关键路径上了。**

### 2.7 会话状态机

```
kDisconnected → kConnecting → kAuthenticating → kSubscribing → kLive
                     ↑                                           |
                     +--------------- kBackoff <-----------------+

                  kFatal ← 凭据错误（重试不会变好，停下来而不是死循环打 401）
```

任何一点掉线都回到 kBackoff 再走一整圈，**没有部分恢复**——在一个已经失败过的
socket 上重新鉴权，会得到一个"收订单但不收成交"的半订阅会话，看起来像交易所坏了。

退避是**指数 + 抖动**。抖动不是装饰：没有它，一起掉线的会话会齐步重连，
把速率限制当成一个请求打爆。

---

## 三、如何使用

### 3.1 依赖与构建

需要 CMake ≥ 3.24、C++20 编译器、OpenSSL，以及 yaml-cpp / spdlog / prometheus-cpp /
cppzmq / libpqxx。macOS 上 CMakeLists 会自动去 brew 的前缀里找。

```bash
# macOS
brew install cmake openssl yaml-cpp spdlog prometheus-cpp cppzmq libpqxx

cmake --preset default && cmake --build build
```

首次 configure 需要联网（拉 GoogleTest 和 nlohmann、simdjson 单文件）。

可用的 preset：

| preset | 构建目录 | 用途 |
|---|---|---|
| `default` | `build/` | Release + 测试 + 基准 |
| `prod` | `build-prod/` | 加 `-march=native` |
| `debug` | `build-debug/` | |
| `asan` | `build-asan/` | Address + UB sanitizer |
| `tsan` | `build-tsan/` | Thread sanitizer |

产物：`build/axon_engine`、`build/axon_strategy`、`build/bench/axon_bench`。

### 3.2 配置

Python 和 C++ 读**同一份 config.yaml**。C++ 独有的旋钮在 `cpp:` 段里，
Python 加载器会忽略它。

```yaml
exchanges:
  deribit:
    env: testnet              # testnet | production
    api_key: "..."
    api_secret: "..."
  okx:
    env: testnet
    api_key: "..."
    api_secret: "..."
    passphrase: "..."         # 只有 OKX 需要

reconciliation:               # 订单对账
  enabled: true
  interval_seconds: 30

fill_reconciliation:          # 成交对账
  enabled: true
  interval_seconds: 60
  lookback_seconds: 86400     # 启动时回补多久，补上停机期间漏掉的
  overlap_seconds: 30         # 每次多拉一段防边界漏单

websocket:
  heartbeat_interval_seconds: 10
  reconnect_delay_seconds: 1
  max_reconnect_delay_seconds: 60
  request_timeout_seconds: 30 # 也是每个握手阶段的预算

portfolio:
  currencies: ["BTC"]
  position_refresh_interval_seconds: 10

zmq:
  router_endpoint: "tcp://*:5555"
  pub_endpoint: "tcp://*:5556"

database:                     # 不配就纯内存，重启丢历史
  dsn: "postgresql://localhost:5432/axon"
  pool_min: 2
  pool_max: 10

metrics:
  enabled: true
  host: "0.0.0.0"
  port: 9100

cpp:                          # 仅 C++ 引擎读
  engine_core: -1             # 绑核，负数 = 不绑（仅 Linux）
  lock_memory: false          # mlockall，避免热路径缺页
  order_pool_size: 65536      # 在途订单硬上限，池子不会长大
  verify_tls: true            # 关掉它只用于对着诊断代理，不要对着真交易所
  extra_ca_file: ""           # 企业 MITM 代理的额外信任锚
  rx_buffer_bytes: 262144
  tx_buffer_bytes: 65536
  shm_strategies: []          # 给哪些策略开共享内存快路径；空 = 全走 ZMQ
  shm_directory: "/dev/shm"   # Linux 上必须是 tmpfs
  shm_slots: 4096
```

**凭据建议走环境变量**，这样 config.yaml 能进版本库：

```bash
export AXON_DERIBIT_API_KEY=...
export AXON_DERIBIT_API_SECRET=...
export AXON_OKX_PASSPHRASE=...
```

`AXON_<EXCHANGE>_API_KEY` / `_API_SECRET` / `_PASSPHRASE` 会覆盖文件里的值。

### 3.3 跑引擎

```bash
./build/axon_engine --config config.yaml
```

启动时它会打印时钟源和分辨率、绑核和 mlockall 的结果、每家会话的连接进度。
之后每分钟一行心跳：缓存订单数、成交数（含去重数）、命令数、事件数、各家会话状态。
**沉默的引擎和卡死的引擎能靠这行区分开。**

`SIGINT` / `SIGTERM` 优雅退出：停 ZMQ 和共享内存 → 停 trade session → 停行情会话 →
把队列里的写刷进 Postgres。

### 3.4 写策略

#### 走 ZMQ（通用，不需要同机）

```cpp
#include "axon/client/strategy_client.h"

axon::client::StrategyClientConfig cfg;
cfg.strategy_id = "my_strategy";
cfg.router_endpoint = "tcp://localhost:5555";
cfg.pub_endpoint = "tcp://localhost:5556";

axon::client::StrategyEvents events;
events.on_order = [](const axon::models::Order& o) {
  // to_string 返回 string_view，不保证以 \0 结尾——别对它用 %s
  const auto status = axon::models::to_string(o.status);
  std::printf("订单 %s -> %.*s\n", o.order_id.c_str(),
              static_cast<int>(status.size()), status.data());
};
events.on_fill = [](const axon::models::Fill& f) { /* ... */ };

axon::client::StrategyClient client;
client.connect(cfg, events);

// 阻塞式下单，能拿回交易所给的 Order
std::string error;
axon::models::OrderRequest req;
req.instrument = "BTC-30JAN26-100000-C";
req.side = axon::models::OrderSide::kBuy;
req.order_type = axon::models::OrderType::kLimit;
req.amount = *axon::core::Qty::from_string("0.1");
req.price = axon::core::Price::from_string("500");

if (auto order = client.place_order("deribit", req, error)) {
  std::printf("已下单 %s\n", order->order_id.c_str());
}

while (running) {
  client.poll();      // 收事件、派发回调
}
```

#### 走共享内存（同机，快 ~100×）

引擎侧先在配置里给这个策略开环：

```yaml
cpp:
  shm_strategies: ["my_strategy"]
  shm_directory: "/dev/shm"
```

策略侧只多一行：

```cpp
cfg.shm_directory = "/dev/shm";   // 有就走快路径，没有自动回落 ZMQ
client.connect(cfg, events);

if (client.fast_path_available()) {
  client.place_order_fast("deribit", req);   // 发出去没有回执
}

while (running) {
  client.poll();       // 必须忙轮询，不能 sleep
}
```

**快路径是发出去没有回执的**：`place_order_fast` 不返回 order_id，
交易所的答复稍后作为 order_update 从同一个环回来。需要同步拿到 order_id 就用
`place_order`（走控制面）——这是真实取舍，"等回复"正是慢路径慢的原因。

#### 现成的例子

[src/strategy_main.cpp](src/strategy_main.cpp) 是一个可跑的完整例子：

```bash
./build/axon_strategy --id my_strategy --ticker deribit:BTC-PERPETUAL
./build/axon_strategy --id my_strategy --orders
./build/axon_strategy --id my_strategy --watch              # 流式看回报
./build/axon_strategy --id my_strategy --shm /dev/shm --watch
```

它也是唯一实例化客户端算法模板（`chase_maker`、`hedge_deribit`）的编译单元——
所以那些模板由每次构建做类型检查，而不是等第一个用它的人来发现问题。

### 3.5 观测

Prometheus 端点默认在 `http://0.0.0.0:9100/metrics`。

重点关注：

| 指标 | 为什么重要 |
|---|---|
| `axon_ws_connected` | **现在它就是下单可用性**，不再只是行情流 |
| `axon_ws_reconnect_total` | 频繁重连 = 下单在反复中断 |
| `axon_order_submit_latency_seconds` | 下单往返，按交易所分 |
| `axon_reconciler_recovered_total` | 对账补回来的条数。持续非零说明 WS 在丢消息 |
| `axon_order_rejected_total` | 带 reason 标签 |
| `axon_db_write_failure_total` | 持久化在掉数据 |

引擎日志里那行每分钟心跳同样重要，尤其是 `hot path: N commands, M events (K dropped)`
——**dropped 非零意味着某个策略没跟上，它的事件被丢了**。

### 3.6 测试与基准

```bash
ctest --test-dir build                        # 395 个

cmake --preset asan && ctest --preset asan    # Address + UB
cmake --preset tsan && ctest --preset tsan    # Thread（环形队列的内存序靠它证明）

./build/bench/axon_bench                    # 延迟基准
./build/bench/axon_bench_coroutine          # 协程开销
```

几个分量最重的测试：

- **`python_wire_compat`**——把 41 个标准对象从 C++ 输出成 JSON，交给**真实的** Python
  `transport/serialization.py` 反序列化再序列化，逐字节比对。
  这是"现有 Python 策略仍然能用"的唯一保证。
- **`ShmRing.TwoProcessesExchangeMessages`**——fork 两个真进程传 20 万条消息。
- **`BinanceWsOrderEntry.SignatureCoversExactlyWhatIsSent`**——从**实际发出的 JSON**
  重算 HMAC 比对签名。Binance 签名串是参数按字母序拼的，签错了它回一个 `-1022`，
  里面没有任何关于哪个字段不对的信息。
- **`test_ws_connection.cpp`**——进程内起真 WebSocket 服务：真 TCP、
  运行时生成证书的真 TLS、真分帧，且**证书校验保持开启**。

---

## 四、边界与注意事项

### 已知的有意取舍

1. **共享内存快路径没有回执**。要同步 order_id 就走控制面。
2. **快路径背压是丢弃不是阻塞**。策略卡住就丢它的事件并计数。
3. **定长字符串装不下就拒绝**，不截断。
4. **热路径不用协程**——实测帧逃逸时是 45 ns malloc 且尾部无界，
   而热路径根本不挂起。冷路径（连接、握手、鉴权、重连退避）适合用。
5. **`Price` 和 `Qty` 是同一个类型的别名**，编译器抓不到传反。
6. **下单没有回退路径**（见 2.6）。

### 部署（Linux）

绑核、`isolcpus` / `nohz_full`、`mlockall`、`SO_BUSY_POLL` **全都只有 Linux 有**。
macOS 上开发没问题，但**只把 Linux 的数字当真**。

共享内存环放 `/dev/shm`（tmpfs）——放在真实文件系统上，内核会试图把环的页面写回磁盘。

`TCP_NODELAY` 默认已开。忘了这一条要付出几十毫秒的代价，
而且它是把前面所有努力一次性作废的最常见方式。

### 上线前必须做的

1. **拿各家 testnet 验证 WebSocket 下单映射**——三家永续的映射抄自文档，
   而且现在没有 REST 可退。**这是第一优先级。**
2. **测网络基线**——候选 region × 每个交易所，24 小时 p99 RTT 和抖动。
   这决定部署拓扑，而且这个仓库对此帮不上任何忙。
3. 冻结热路径：零分配检查、绑核、per-thread 指标。

> 提醒一件容易搞反优先级的事：加密期权执行的主要延迟**不在引擎里**。
> 应用层到交易所的往返通常是 **0.5–2 ms**，所以 10 µs 的内部预算只占端到端的 **1–5%**。
> 它值得做，但理由是**控尾延迟**不是中位数。**部署 region 选错值 ~100 ms**，
> 那个比这里的一切都重要。
