# Axon

加密货币交易基础设施 monorepo。按 `<语言>/<服务>/` 组织，目前有两个服务：

| 服务 | Python | C++ | 做什么 |
|---|---|---|---|
| **订单执行** | [python/axon_order_execution/](python/axon_order_execution/) | [cpp/axon_order_execution/](cpp/axon_order_execution/) | EMS + OMS 执行引擎，策略通过 ZMQ / 共享内存下单、撤单、订阅成交回报 |
| **行情** | [python/axon_market_data/](python/axon_market_data/) | [cpp/axon_market_data/](cpp/axon_market_data/) | 多交易所行情采集，标准化后经 ZMQ PUB 分发 |

> 本文档余下部分讲的是**订单执行服务**。行情服务的文档在
> [python/axon_market_data/README.md](python/axon_market_data/README.md) 和
> [cpp/axon_market_data/README.md](cpp/axon_market_data/README.md)。

### 端口分配

两个服务可能跑在同一台机器上，ZMQ 端口**不能重叠**：

| 端口 | 服务 | 用途 |
|---|---|---|
| 5555 | 订单执行 | ZMQ ROUTER，策略下单 / 撤单的控制面 |
| 5556 | 订单执行 | ZMQ PUB，订单与成交回报广播 |
| 5557 | 行情 | ZMQ PUB，Deribit 期权行情 |
| 5558 | 行情 | ZMQ PUB，通用多交易所行情 |

新增服务时请在这里登记，别复用上面任何一个。

---

# 订单执行服务

加密货币**期权 / 永续合约**订单执行系统。核心是一个独立的**执行引擎进程**（EMS + OMS），
多个策略进程通过 ZMQ（或共享内存）连接上来下单、撤单、订阅成交回报。

订单执行有**两套完整实现**，共享同一套配置文件和同一套线上协议：

| | 目录 | 定位 | 状态 |
|---|---|---|---|
| **Python** | [python/axon_order_execution/](python/axon_order_execution/) | 生产在跑的实现，asyncio，四个交易所全通 | 可用 |
| **C++** | [cpp/axon_order_execution/](cpp/axon_order_execution/) | 低延迟移植版，单线程忙轮询，下单全走 WS，目标 <10 µs | 功能完整，**未接过真实交易所** |

两者的 JSON 控制面是**逐字节兼容**的（有专门的 `python_wire_compat` 测试守着），
所以现有的 Python 策略不改一行代码就能连到 C++ 引擎上。

---

## 一、这个系统解决什么问题

做期权/永续做市和对冲时，策略本身需要的是"我要在 Deribit 买 0.1 个
`BTC-30JAN26-100000-C`"，而不是去关心：

- 每个交易所的 REST 签名方式、WebSocket 鉴权方式、字段名各不相同；
- WebSocket 会断、会丢消息，订单的真实状态必须靠定期对账（reconcile）兜底；
- 多个策略跑在不同进程里，但共用同一套交易所连接和同一份订单状态；
- 成交（fill）必须去重，重复计一笔就是仓位算错。

所以架构上把**"和交易所打交道"**这件事抽成一个独立引擎进程，策略只面对一个统一的
`place_order / cancel_order / get_ticker / 回调` 接口。

---

## 二、整体架构

```
  策略进程 A                    策略进程 B                策略进程 C (C++, 同机)
  StrategyClient (Py)          StrategyClient (Py)       StrategyClient (C++)
    DEALER + SUB                 DEALER + SUB              共享内存环 (可选)
         \                            /                          |
          --- ZMQ ROUTER :5555 -------                           |  ~0.1-0.3 µs
          --- ZMQ PUB    :5556 -------                           |  单向、无 JSON
                  |  ~30-60 µs 往返                              |
          ┌───────┴─────────────────────────────────────────────┴────────┐
          │                     执行引擎进程                              │
          │   AxonExecutionService / axon_engine                     │
          │                                                              │
          │   EMS  ── REST 下单/撤单/改单/行情                            │
          │   OMS  ── WebSocket 订单&成交&持仓推送 + 本地状态 + 定期对账   │
          │   Repository ── Postgres / 内存                              │
          │   Metrics ── Prometheus :9100/metrics                        │
          └───────────────────┬──────────────────────────────────────────┘
                              |
              Deribit(期权) / Binance / Bybit / OKX (U 本位永续)
```

### 双平面设计（C++ 版特有）

| | 控制面 | 数据面（热路径） |
|---|---|---|
| 传输 | ZMQ DEALER/ROUTER + PUB/SUB | 共享内存 SPSC 环（mmap） |
| 编码 | JSON | 定长 POD 结构体，memcpy |
| 延迟 | ~30-60 µs 往返 | ~0.1-0.3 µs 单向 |
| 承载 | 所有查询、订阅、Python 策略 | 只有 place / cancel / modify / order_update / fill |
| 语义 | 请求-响应 | **发出去没有回执**，结果靠后续 order_update 送回 |

数据面只放那 5 种消息，其它一律走 JSON——JSON 那边加字段是免费的，POD 那边加字段是
破坏性变更。共享内存**一个策略一对环**（SPSC 决定的），消费者必须忙轮询；写满了是
**丢弃 + 计数**，绝不阻塞引擎循环。

---

## 三、模块地图（Python）

### `models/` — 领域模型与协议消息
- `order.py` — `Order` / `OrderRequest` / `Ticker`，以及 `OrderStatus`、`OrderSide`、
  `OrderType`、`Liquidity` 枚举。每个订单带 `strategy_id`，这是多策略隔离的依据。
- `fill.py` / `portfolio.py` — 成交、账户汇总（`AccountSummary`）、持仓（`Position`）。
- `messages.py` — ZMQ 协议三件套：`Command`（策略→引擎）、`Response`（引擎→策略）、
  `Event`（引擎广播）。

### `ems/` — Execution Management System（走 REST，负责"动作"）
- `base.py` — `BaseEMS` 抽象接口：下单、撤单、改单、查单、查未成交、查行情、查账户/持仓。
- `ems_service.py` — 按交易所名把请求路由到对应实现。
- `deribit/`、`binance/`、`bybit/`、`okx/` — 四个交易所各自的 REST 实现，
  共用 `util/http.py` 的重试 + 指数退避。

### `oms/` — Order Management System（走 WebSocket，负责"状态"）
- `base.py` — `BaseOMS`，继承 `WebSocketBase`。
- `order_manager.py` — 内存订单缓存 + 落库。带**时间戳防旧数据覆盖**，
  更新时保留 `strategy_id`，支持同步/异步回调。
- `fill_manager.py` — 成交去重（按 `trade_id`）与分发。
- `portfolio_manager.py` / `position_refresher.py` — 账户余额与持仓，
  WS 推送 + 定期 REST 刷新。
- `order_reconciler.py` — 定期用 REST 和交易所对账订单：本地还是 active、
  交易所已经终态的，说明 WS 消息丢了，补回来。
- `fill_reconciler.py` — 同理对账成交。启动时按 `lookback_seconds` 拉一段历史，
  补上停机期间漏掉的；每次多拉 `overlap_seconds` 防边界漏单。
- `oms_service.py` — 把上面这些和四个交易所的 WS 客户端串起来。
- `deribit/deribit_ws.py` 等 — 各交易所 WS：JSON-RPC 2.0 / 事件流、订阅、心跳。

### `transport/` — ZMQ 传输层
- `types.py` — `CommandType`（place_order / cancel_order / get_ticker /
  get_positions / get_fills_by_strategy …）与 `EventType`（order_update /
  fill_update / account_update / position_update）。
- `serialization.py` — 所有领域对象和协议消息的 JSON 编解码。**C++ 侧和这个文件严格对齐。**
- `server.py` — 引擎侧。ROUTER 收命令 → 派发给 `AxonExecutionService` → 回响应；
  PUB 广播事件，**topic 就是 `strategy_id`**，策略只收自己的。

### `client/` — 策略侧 SDK
- `strategy_client.py` — `StrategyClient`，接口和 `AxonExecutionService` **完全一致**，
  只是底下走 ZMQ（DEALER 请求-响应，SUB 收事件）。
- `algorithms/chase_maker.py` — `chase_maker_fill`：在盘口最优价上持续挂 maker 单追价，
  超时后剩余量用 market 单打掉。
- `algorithms/hedge_deribit.py` — `hedge_deribit_options`：两腿期权分批对冲
  （maker 腿 + taker 腿）。

> 算法用 duck typing 接 client，所以既能配 `StrategyClient`（多进程），
> 也能直接配 `AxonExecutionService`（进程内）。

### `repository/` — 持久化
`order` / `fill` / `account` / `position` 四类，各有 `*_base.py`（抽象接口）、
`*_memory.py`（内存）、`*_postgres.py`（asyncpg）。Postgres 实现是 try-import，
没装 asyncpg 也能跑。

### `util/`
- `http.py` — `AsyncHttpClient`，重试 + 指数退避。
- `websocket_base.py` — 自动重连、心跳、JSON-RPC 请求-响应配对。
- `metrics.py` — `MetricsClient`，**业务代码只调它的领域方法**
  （`inc_order_rejected`、`observe_order_submit_latency` …），
  prometheus_client 被封在里面。关掉时所有调用是廉价 no-op。
- `logging.py` — 彩色控制台 + 按天轮转的文件日志。

### 根文件
- `service.py` — `AxonExecutionService`，EMS + OMS 的统一门面。
  既能被策略进程内直接调，也是 `ZMQTransportServer` 的后端。
- `engine.py` — 引擎进程入口。初始化 metrics、建 Postgres 连接池和表、
  启动 service，处理 SIGINT/SIGTERM 优雅退出。
- `config.py` — 全部配置 dataclass + YAML 加载。

---

## 四、模块地图（C++）

分层严格，**下层不许碰上层的依赖**，这样每层都能单独编译、单独测：

| 层 | 依赖 | 内容 |
|---|---|---|
| `core/` | 只有 libc++ / pthread | `decimal.h`（int64 定点，9 位小数）、`timestamp.h`（和 Python `isoformat()` 逐字节一致）、`clock.h`（TSC，0.29 ns）、`histogram.h`（HDR）、`spsc_ring.h`、`shm_ring.h`、`object_pool.h`、`latency.h`、`platform.h` |
| `models/` | core | 领域类型 + 枚举字符串表，字段顺序对齐 Python dataclass |
| `transport/` | models | `wire.h`（JSON 控制面，nlohmann）+ `hot_messages.h`（定长 POD 数据面）+ `shm_bridge` + `zmq_server` |
| `net/` | core + OpenSSL | **手写 RFC 6455 WebSocket**：分帧、掩码、分片重组、握手；非阻塞 TCP；OpenSSL 走 memory BIO（socket 和缓冲区留在自己手里）；SHA-1/SHA-256/HMAC/base64 本地实现 |
| `venue/` | transport + simdjson + net | 四个交易所的入站解析（simdjson On-Demand）和出站构造（字节模板，不用 JSON 库） |
| `app/` | 以上全部 + yaml-cpp/spdlog/prometheus-cpp/cppzmq/libpqxx | config、logging、metrics、`OrderStore`、`PortfolioStore`、`VenueSession`、`Reconciler`、Postgres 写线程、ZMQ 服务 |

引擎主循环（`src/engine_main.cpp`）是**单线程忙轮询**：依次 poll 每个 venue session、
HTTP、EMS、各 reconciler、共享内存桥、ZMQ，然后 `cpu_pause()`。没有调度器、没有唤醒、
从交易所消息到订单簿没有跨线程跳转。代价是那个核 100% CPU——这是尾延迟可控的入场费。

### 为什么手写 WebSocket 而不用 Boost.Beast
在 10 µs 预算下，Beast 的 `dynamic_buffer` 在**接收路径上分配内存**，一次 malloc 长尾就
吃掉整个预算。`on_message` 直接给出**接收缓冲区内的指针**，venue 解析器从 socket 缓冲区
里直接读那 6 个字段，中间零拷贝。

### 关键实测数字（M 系列笔记本，未绑核，仅供相对比较）
```
encode PlaceOrderMsg (POD)      37.5 ns   vs  JSON 序列化   2125 ns   57×
消费一条（指针转换）              0.29 ns   vs  JSON 解析    2417 ns   ~8000×
shm ring push+pop (216 B)       10.5 ns
WS 解帧 + 重组（288 B）          16.3 ns
Deribit 订单更新解析             280 ns（simdjson On-Demand）
```
JSON 库选型也是量出来的：simdjson On-Demand 比 nlohmann 快 33×、比 rapidjson 快 8×；
出站消息干脆不用解析器，字节模板 12 ns（比 nlohmann `dump()` 快 120×）。
详见 [C++ README](cpp/axon_order_execution/README.md)。

---

## 五、各交易所的坑（都是踩过的）

| | 品种 | 入站形态 | 数字 | WS 鉴权 |
|---|---|---|---|---|
| **Deribit** | 期权 | JSON-RPC 2.0 订阅 | 裸数字 | client_id / secret |
| **Binance** | U 本位永续 | event 标签的 user data stream | 字符串 | listenKey 放 URL |
| **Bybit** | U 本位永续 | topic / data 数组 | 字符串（含时间戳） | HMAC-SHA256，hex |
| **OKX** | 永续 swap | arg / data 数组 | 字符串（含时间戳） | HMAC-SHA256，**base64** + passphrase |

- **Deribit** 没有独立的"部分成交"状态，要从 `filled_amount > 0` 且订单仍 open 推出来。
- **Binance** 一条 `ORDER_TRADE_UPDATE` 既是订单更新**又是**成交（当 `execType == TRADE`），
  漏了就永远记不上这笔 fill；`"p":"0"` 表示**没有价格**，不是价格为 0；
  `listenKeyExpired` 必须重建会话，不能只打日志。
- **Bybit** 一条私有流同时推 linear / inverse / option / spot，
  `category != linear` 的必须丢掉，否则现货成交会落到永续持仓上；
  `isMaker` 有时是 bool 有时是字符串 `"true"`。
- **OKX** 心跳是**纯文本** `ping`/`pong` 不是 JSON；`"px":""` 表示缺失不是 0；
  **手续费是负数**，要归一成正的，否则和其它三家符号相反；`post_only` 是**订单类型**不是标志位。

C++ 的字段映射全部是从对应的 Python OMS/EMS **抄过来的，不是照文档写的**——
Python 已经和真实交易所对账很久了，文档没有。

---

## 六、快速开始

### 多进程模式（生产）

```bash
# 终端 1：启动引擎
cd python/axon_order_execution
python -m axon_order_execution.engine --config ../../config.yaml

# 终端 2：跑策略
cd python/axon_order_execution
python examples/strategy_one.py
```

策略代码：

```python
import asyncio
from axon_order_execution.client import StrategyClient
from axon_order_execution.config import ZMQConfig
from axon_order_execution.models import OrderRequest, OrderSide, OrderType

async def main():
    client = StrategyClient(ZMQConfig(), strategy_id="my_strategy")
    await client.connect()

    client.register_order_update_callback(
        lambda o: print(f"订单更新: {o.order_id} -> {o.status.value}")
    )
    client.register_fill_update_callback(
        lambda f: print(f"成交: {f.trade_id} {f.amount}@{f.price}")
    )

    order = await client.place_order("deribit", OrderRequest(
        instrument="BTC-30JAN26-100000-C",
        side=OrderSide.BUY,
        amount=0.1,
        order_type=OrderType.LIMIT,
        price=500.0,
    ))
    print(f"已下单: {order.order_id}")

    await client.disconnect()

asyncio.run(main())
```

### 进程内模式（开发/测试）

```python
from axon_order_execution import AxonExecutionService, load_config

service = AxonExecutionService(load_config("config.yaml"))
await service.start()
order = await service.place_order("deribit", request)   # 接口和 StrategyClient 一样
```

### C++ 引擎

```bash
cd cpp/axon_order_execution
cmake --preset default && cmake --build build && ctest --test-dir build
./build/bin/axon_engine --config ../../config.yaml
./build/bench/axon_bench          # 跑基准
```
需要 CMake ≥ 3.24、C++20 编译器、OpenSSL，以及 yaml-cpp / spdlog / prometheus-cpp /
cppzmq / libpqxx（macOS 上 CMakeLists 会自动去 brew 的前缀里找）。
首次 configure 需要联网（拉 GoogleTest 和 nlohmann 单头文件）。

---

## 七、配置

Python 和 C++ 读**同一份 config.yaml**。C++ 独有的旋钮放在 `cpp:` 段里，
Python 加载器会忽略。

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
  lookback_seconds: 86400     # 启动时回补多久
  overlap_seconds: 30         # 每次多拉一段防边界漏单

websocket:
  heartbeat_interval_seconds: 10
  reconnect_delay_seconds: 1
  max_reconnect_delay_seconds: 60

portfolio:
  currencies: ["BTC"]
  position_refresh_interval_seconds: 10

zmq:
  router_endpoint: "tcp://*:5555"
  pub_endpoint: "tcp://*:5556"
  router_connect: "tcp://localhost:5555"
  pub_connect: "tcp://localhost:5556"

database:                     # 不配就纯内存，重启丢历史
  dsn: "postgresql://localhost:5432/axon"
  pool_min: 2
  pool_max: 10

metrics:
  enabled: true
  host: "0.0.0.0"
  port: 9100                  # http://host:9100/metrics

cpp:                          # 仅 C++ 引擎读
  engine_core: -1             # 绑核，负数=不绑（仅 Linux）
  lock_memory: false          # mlockall，避免热路径缺页
  order_pool_size: 65536      # 在途订单硬上限，池子不会长大
  verify_tls: true
  shm_strategies: []          # 走共享内存快路径的策略名；空=全走 ZMQ
  shm_directory: "/dev/shm"   # Linux 上必须是 tmpfs
  shm_slots: 4096
```

凭据也可以走环境变量（这样 config.yaml 能进版本库）：
`AXON_<EXCHANGE>_API_KEY` / `_API_SECRET` / `_PASSPHRASE`，会覆盖文件里的值。

---

## 八、可观测性

Prometheus 指标（默认 `:9100/metrics`），业务代码统一通过 `MetricsClient` 上报：

- **下单链路**：submit / cancel / modify 延迟直方图，ack 延迟，fill 延迟，
  拒单计数（带 reason），各类失败计数（带 error_type）。
- **WebSocket**：连接状态、重连次数、心跳丢失、消息新鲜度（message age）。
- **对账**：各类 reconciler 补回的条数、失败次数。
- **账户**：保证金率。
- **持久化**：DB 写失败计数。

---

## 九、测试

```bash
# Python：python/axon_order_execution/{examples,tests}/ 下是对着 testnet 跑的端到端脚本
cd python/axon_order_execution
python tests/test_place_order.py
python tests/test_multi_strategy_alpha.py     # 配合 test_multi_strategy_beta.py

# C++：383 个单测
cd cpp/axon_order_execution
ctest --test-dir build
cmake --preset asan && ctest --preset asan    # ASan/UBSan 全过
cmake --preset tsan && ctest --preset tsan    # TSan 全过（环形队列的内存序靠它证明）
```

几个分量最重的测试：

- **`python_wire_compat`** — 用 C++ 生成 41 个标准对象的 JSON，交给**真实的**
  Python `transport/serialization.py` 反序列化再序列化，逐字节比对。
  这是"现有 Python 策略仍然能用"的唯一保证。
- **`ShmRing.TwoProcessesExchangeMessages`** — fork 出两个真进程传 20 万条消息。
- **`test_ws_connection.cpp`** — 进程内起真的 WebSocket 服务：真 TCP、真 TLS
  （运行时生成证书）、真分帧；证书校验保持**开启**，并测试不受信证书和域名不匹配会被拒。

---

## 十、当前状态与已知取舍

**Python 实现**：四个交易所全通，生产可用。

**C++ 实现**：传输层、协议层、OMS/EMS、ZMQ 控制面、Postgres 写线程、metrics 都已落地，
**下单全部走 WebSocket（没有 REST 下单）**，`ctest` 全绿（395 个测试，ASan/TSan 均干净）
——但**从未连过真实交易所**。所有验证都是对着 loopback 上的真 TLS 服务
和抓下来的报文做的。下一步应该是拿一个交易所的 testnet 凭据实跑。

> ⚠️ [cpp/axon_order_execution/README.md](cpp/axon_order_execution/README.md)
> 里的 "What is not here" 一节已经过时（写于 OMS/EMS/ZMQ/持久化落地之前），
> 以代码为准。其余章节（选型论证、实测数字、各交易所的坑）仍然有效且值得读。

已知的**有意设计取舍**：

1. **共享内存快路径是发出去没有回执的**。策略不能同步拿到 order_id，只能等
   order_update 回来。想同步就走 ZMQ 控制面——这是真实取舍，等回执正是慢路径慢的原因。
2. **快路径背压是丢弃不是阻塞**。策略卡住了就丢它的事件并计数；阻塞引擎循环会把
   所有交易所的行情一起拖死。
3. **定长字符串装不下就拒绝，不截断**。截断的合约名会把单子发到错误的合约上。
4. **热路径不用协程**。实测协程帧逃逸时是 45 ns malloc 且尾部无界，而热路径根本不挂起。
   冷路径（连接、握手、鉴权、订阅、重连退避）用协程。
5. **`Price` 和 `Qty` 目前是同一个类型的别名**，编译器抓不到传反。
6. **下单只有 WebSocket 一条路，没有回退**。省掉了每单 1–2 个 RTT，代价是
   **session 不在线这家就不能下单**；下单可用性从此等于连接可用性。
   而且三家永续的映射抄自交易所文档、从未验证过，旁边也不再有已验证的路可退——
   testnet 验证是上线前提，不是后续事项。

### 移植时在 Python 里发现的一个 bug
`_parse_order` / `_parse_fill` 用 `datetime.fromtimestamp(ts / 1000)` 得到的是**本地时间**，
而 `_handle_portfolio_update` 和 `Order` 的默认值用的是 `datetime.utcnow()`（**UTC**），
两者混在同一个对象里。UTC 服务器上看不出来，非 UTC 时区的机器上 Deribit 来的
`created_at` / `updated_at` 会整体偏移，静默污染任何时间比较（包括延迟统计和按时间对账）。
**C++ 版全程用 UTC。**

---

## 十一、延迟优化的优先级（别搞反）

加密期权执行的主要延迟**不在引擎里**，而在到交易所的 WAN 往返和交易所自己的撮合。
主流加密交易所跑在公有云上，"同机房"实际是"同 region、最好同 AZ"，同 AZ 网络 RTT
大约 50-150 µs，但过一遍交易所网关和撮合引擎的应用层往返通常是 **0.5-2 ms**。

所以 10 µs 的内部预算只占端到端的 **1-5%**，它值得做，但理由是**控尾延迟**而不是中位数。
按收益排序：

1. **部署 region** — 搞错值 ~100 ms。先测 24 小时 p99 RTT 再定。**这个仓库帮不上忙。**
2. **用 WebSocket 下单代替 REST** — 值 1-5 个 RTT。**C++ 版已经全部走 WS，没有 REST
   下单**。代价是 session 不在线就下不了单，而且三家永续的映射抄自文档、尚未验证。
   Python 版仍然三家走 REST。
3. **策略↔引擎传输** — 值 15-60 µs。就是 `core/shm_ring.h`。
4. **内部代码路径** — 值 5-50 µs。其余部分。

这个仓库做的是第 3 和第 4 项。别让它分散了对第 1、2 项的注意力。
