# axon_order_execution_cpp

[Python 引擎](../../python/axon_order_execution/)的低延迟 C++ 移植版，和它住在同一个仓库里：

```
axon/                                <- 仓库根
├── python/
│   └── axon_order_execution/        Python 实现（未改动）
└── cpp/
    └── axon_order_execution/        本项目
```

**故意放在一个仓库里。** 两套实现共享同一套线上协议，而证明这件事的测试
（下文的 `python_wire_compat`）跑的是**真实的** Python 序列化器去解析 C++ 的输出。
拆成两个仓库，这个测试就变成跨仓库的版本匹配问题，第一次挂掉时它会被禁用而不是被修好。

**状态：传输层、协议层、OMS/EMS、控制面、持久化均已落地，下单全部走 WebSocket，
`ctest` 全绿（395 个测试，ASan/TSan 均干净）——但从未连过真实交易所。** 所有验证都是对着 loopback 上的真 TLS 服务和抓下来的报文做的。
详见[还差什么](#还差什么)。

**目标：内部延迟 10 µs 以内，部署在和交易所同一个 region。** 这个数字是"为什么手写
WebSocket 而不用 Boost.Beast"的唯一理由，也是为什么这里每个组件都是**量出来的**而不是拍出来的。

```bash
cmake --preset default && cmake --build build && ctest --test-dir build
./build/bench/axon_bench
```

需要 CMake ≥ 3.24、C++20 编译器、OpenSSL，以及 yaml-cpp / spdlog / prometheus-cpp /
cppzmq / libpqxx（macOS 上 CMakeLists 会自动去 brew 的前缀里找）。
首次 configure 需要联网（拉 GoogleTest 和 nlohmann 单头文件）。

---

## 一、这个项目要解决什么，以及**不**打算解决什么

加密期权执行的主要延迟**不在引擎里**。它在到交易所的 WAN 往返，和交易所自己的撮合上。

有必要把"colocated（同机房）"这个词说清楚：主流加密交易所跑在**公有云上**，不是在交易所
机柜里拉根交叉线。实践中"同机房"意味着**同 region、最好同 AZ**，走的是普通 TCP、普通网卡。
同 AZ 的网络 RTT 大约 **50–150 µs**，但过一遍交易所网关和撮合引擎的**应用层往返**
通常是 **0.5–2 ms**。这个数每个交易所都要自己测，别信任何人的文档，包括这一段。

所以 10 µs 的内部预算只占端到端路径的 **1–5%**。它仍然值得做——但理由是**控制尾延迟**，
不是中位数。一旦你已经 colocated 了，决定你在撮合队列里位置的就是你自己的抖动。

**按收益排序：**

1. **部署 region** — 搞错了值 ~100 ms。候选 region 到每个交易所的 p99 RTT 和抖动，
   先测满 24 小时再定。**这个仓库对此毫无帮助。**
2. **用 WebSocket 下单代替 REST** — 值 1–5 个 RTT。这是架构问题不是语言问题。
   Python 引擎按 EMS(REST) / OMS(WS) 分开；C++ 版应该在**已经鉴权好的 WS 连接上**发单。
3. **策略↔引擎的传输** — 值 15–60 µs。就是 `core/shm_ring.h` 干的事。
4. **内部代码路径** — 值 5–50 µs。其余全部。

**这个仓库做的是第 3 和第 4 项。别让它分散了你对第 1、2 项的注意力。**

### 10 µs 预算到底花在哪

在这个量级上，以前是舍入误差的东西变成了主要成本：

| 步骤 | 成本 | 占 10 µs 的比例 |
|---|---|---|
| `send()` 系统调用 | 1–3 µs | **10–30%** |
| TLS 记录加解密（AES-NI） | 单向 0.5–2 µs | **10–40%** |
| JSON 取字段（simdjson on-demand） | 0.2–0.5 µs | 2–5% |
| WebSocket 解帧 + 重组 | **16 ns**（实测） | 0.2% |

两个结论，第二个推翻了早先的判断：

- **Beast 出局。** 它的 `dynamic_buffer` 在**接收路径上分配内存**，
  而一次 malloc 长尾卡在交易决策中间，正是 10 µs 预算吸收不了的东西。所以有了下面的 `net/`。
- **内核旁路从"过度设计"变成"值得评估"。** 在 WAN 距离上系统调用是噪音；
  colocated 之后 `send()` 占整个预算的十分之一到三分之一。云虚机里没有 Solarflare，
  现实的选项是 **io_uring + SQPOLL**。等 socket 层存在之后拿数据决定，不要提前。

---

## 二、有什么

### `core/` — 无依赖的地基

`core/` 里的任何东西都不链接除 libc++ 和 pthread 以外的库。这是**故意的**：
热路径代码必须**没有能力**误碰到 JSON 解析器或第三方分配器。

| 头文件 | 是什么 | 为什么存在 |
|---|---|---|
| `decimal.h` | int64 定点数，9 位小数 | 把 double 格式化进 JSON 要 20–50 ns 而且会分配内存，int64 只要 ~9 ns。而且用 double 累加成交量会漂移出幽灵残余仓位。 |
| `timestamp.h` | ns 精度 UTC 时间戳 | 格式化结果和 Python 的 `datetime.isoformat()` **逐字节一致**，包括微秒为 0 时省略小数部分这条规则。 |
| `clock.h` | TSC / CNTVCT 计数器 | `now_ticks()` 是 0.29 ns。热路径只记原始 tick，转换成 ns 放到上报线程去做。 |
| `histogram.h` | HDR 式延迟直方图 | 尾部决定盈亏，均值掩盖尾部。`record()` 是常数开销，所以你负担得起"每条消息都测"。 |
| `spsc_ring.h` | 进程内无锁环 | 一个有竞争的 mutex 要 2–10 µs——比整个内部预算还多。 |
| `shm_ring.h` | 跨进程 mmap 环 | 替掉热路径上的 ZMQ 那一跳。 |
| `object_pool.h` | 定容 freelist | malloc 的**尾部是无界的**，而且恰好在你最忙的时候飙起来。 |
| `latency.h` | 多阶段打点 | 回答"卡在哪个阶段"，而不是"它慢"。 |
| `platform.h` | 缓存行、pause、绑核 | 绑核只有 Linux 支持，它会明说做不到，而不是静默地什么也不干。 |

### `models/` — 领域类型

`Order`、`OrderRequest`、`Ticker`、`Fill`、`AccountSummary`、`Position` 和一堆枚举。
**字段声明顺序照抄 Python dataclass**，因为 JSON 编码器按声明顺序输出键。

### `transport/` — 两个平面，故意分开

**控制面**（`wire.h`、`messages.h`）—— JSON over ZMQ，结构上和 Python 的
`transport/serialization.py` 完全一致：同样的键、同样的顺序、同样的值。
**这就是"现有 Python `StrategyClient` 不改代码就能连 C++ 引擎"的依据**，也是渐进式移植的前提。

**数据面**（`hot_messages.h`）—— 定长 POD，只做 memcpy。生产一条消息是几次 store，
消费一条是一次指针转换。

实测差距就是拆开它们的论据：

| | 二进制 | JSON | 倍数 |
|---|---|---|---|
| 构造一条下单消息 | **37.5 ns** | 2125 ns | 57× |
| 消费它 | **0.29 ns**（转换） | 2417 ns（解析） | ~8000× |
| 线上大小 | 216 B | 264 B | |

**只有** place / cancel / modify / order-update / fill 走二进制平面。其余一律留在 JSON，
在那边加字段是免费的。

数据面的代价是**刚性**，而且是真实的代价：

- **字符串是定宽数组。装不下的值会被拒绝，不是截断**——一个被静默截断的合约名会把订单
  发到错误的合约上。
- **两端必须由同一份头文件编译。** 除了消息头里的 `version` 字段没有版本协商，
  而那个字段的存在就是为了让不匹配在启动时**大声失败**，而不是错误地解析下去。
- **加字段是破坏性变更。** 这正是控制面还存在的理由。

### `venue/` — 各交易所协议，四家齐全

入站解析和出站构造。纯字节变换，不碰 socket，所以能拿抓下来的报文穷举测试。

| | 品种 | 入站形态 | 数字 | WS 鉴权 |
|---|---|---|---|---|
| **Deribit** | 期权 | JSON-RPC 2.0 订阅 | 裸数字 | client_id / secret |
| **Binance** | U 本位永续 | event 标签的 user data stream | 字符串 | 无（listenKey 放 URL） |
| **Bybit** | U 本位永续 | topic / data 数组 | 字符串（含时间戳） | HMAC-SHA256，hex |
| **OKX** | 永续 swap | arg / data 数组 | 字符串（含时间戳） | HMAC-SHA256，**base64** + passphrase |

公共部分：`json_view.h`（simdjson 封装）、`json_writer.h`（字节写入器）、
`venue_events.h`（四家统一成一种结果形状，这样上层 OMS 不需要知道消息是哪个协议来的）。

订单更新的解析耗时，按各家自己的报文形状实测：

```
deribit (期权, 裸 JSON 数字)            280 ns
binance (永续, 字符串数字)               308 ns
bybit   (永续, 字符串数字 + 时间戳)       307 ns
okx     (永续, 字符串数字 + 时间戳)       278 ns

构造 private/buy 请求 (deribit)          69 ns
构造 private/cancel 请求                 24 ns
```

#### 各家的坑，每一条都是承重的

**Deribit** —— 没有独立的"部分成交"状态，它是从 `filled_amount > 0` 推出来的，
而且只对 open 的订单成立。

**Binance** —— 一条 `ORDER_TRADE_UPDATE` 既是订单更新**又是**成交（当 execution type
是 `TRADE` 时）。漏掉这点，成交就会永远静默地记不上。字段名是单字母，包括 `o` ——
它既是 payload 对象名，又是对象里的订单类型字段名。`"p":"0"` 表示**没有价格**，
不是价格为 0。`listenKeyExpired` 报成 `kSessionExpired`——正确的反应是重建会话，
不是打条日志继续跑。

**Bybit** —— 一条私有流同时承载 linear、inverse、option **和** spot。
任何 `category` 不是 `linear` 的都必须丢掉，否则一笔现货成交会落到永续持仓上。
`isMaker` 在某些路径上是 JSON bool，在另一些路径上是字符串 `"true"`。
`req_id` 是字符串，和其它所有交易所的整数都不一样。

**OKX** —— 心跳是**纯文本** `ping`/`pong` 不是 JSON，所以它根本到不了解析器。
`"px":""` 表示缺失不是 0（`json_view` 把空字符串当 `nullopt` 就是为了这个）。
**手续费上报是负数**，要归一成正的，否则费用会和其它三家符号相反。
`post_only` 是一个**订单类型**，不是标志位。它的签名是 base64 而 Bybit 和 Binance 是 hex，
而且需要一个从任何东西都推导不出来的 passphrase。

#### WebSocket 下单（唯一的下单路径）

**这个系统里没有 REST 下单。** 每一笔下单、撤单、改单都走一条已经建立、已经鉴权的
WebSocket 连接，省掉每单一次 HTTP 往返——**1–2 个 RTT，colocated 预算的一大半**。

四家的落点结构上完全不同：

| | 端点 | 连接 | 鉴权 |
|---|---|---|---|
| **Deribit** | `private/buy` / `cancel` / `edit` | 它唯一那条连接 | 登录一次 |
| **OKX** | `op:"order"` | **复用** `/ws/v5/private` 私有流 | 登录一次 |
| **Bybit** | `/v5/trade` | **另开一条**（和 `/v5/private` 不同路径） | 该连接单独 `op:"auth"` |
| **Binance** | `ws-fapi.binance.com/ws-fapi/v1` | **另开一条**（不同主机） | **无会话登录，每条请求自带签名** |

##### 这么做的代价，说清楚

**session 不在线 = 这家不能下单。** 下单的可用性从此**完全等于**它底下那条连接的可用性。
以前三家永续可以退回一个只依赖凭据的 HTTP 请求；现在 session 在退避里就意味着下单被拒，
直到它回来为止。

这是为了砍掉那 1–2 个 RTT 付出的、明确的代价。它还意味着 **session 健康度现在就是下单
健康度**——`axon_ws_connected` 和重连计数要盯，它们不再只是描述行情流。

而且**三家永续的映射从未验证过**：它们抄自交易所文档，不是抄自那份和真实交易所对账很久的
Python。现在旁边**没有一条已被验证的路可以退**，所以"拿各家 testnet 验一遍"从"后续事项"
变成了**上线前提**。

##### Binance 是最麻烦的那个

它的签名串是**参数按名字排序**后拼成 `key=value&...`。签错顺序，交易所回一个 `-1022`，
里面**没有任何**关于哪个字段不对的信息。

所以那些参数走**同一个 emitter** 同时驱动签名串和 JSON 体
（`each_place_param` + `QueryPass` / `JsonPass`）。写两遍，就会在某天有人往其中一处加了
字段而另一处没加时开始漂移——而"签的是一份、发的是另一份"是这里最难查的故障。

两个测试钉这条性质：`SignatureCoversExactlyWhatIsSent` 从**实际发出的 JSON** 里重算 HMAC
去比对报文里的签名（自洽性）；`SigningStringMatchesAnIndependentReference` 再拿一个用
Python `hmac` 独立算出的常量比对（正确性）——只有前者的话，签名串构造错了也照样通过。

连带的取舍：所有值都以 **JSON 字符串**发出，这样"签的字节"和"发的字节"literally 是同一段
文本。因此一个含需要 JSON 转义字符的 label（比如 `a"b`）会被**拒绝**——它没法既按原文签名
又按转义后发送。

##### 另外两条

- **trade 连接平时是静默的**——没人下单它一条消息都不来，而基类的静默看门狗会把这个当成
  半开连接每几秒重连一次。所以 trade session 自己发保活（Bybit `{"op":"ping"}`、
  Binance `{"id":"0","method":"ping"}`），靠**对方的回复**去喂看门狗——只发不收
  证明不了链路活着。
- **OKX 有两层结果码**：顶层 `code` 是"请求处理了吗"，每个订单的 `sCode` 才是"这一单
  收了吗"。顶层 `code:"0"` 配 `sCode:"51008"`（余额不足）是完全正常的形状，
  只看顶层就会把一笔被拒的单当成成功。

##### REST 还剩什么

`oms/venue_rest.h` 还在，但它现在**只做定时的兜底读**：对账快照
（`get_open_orders` / `get_user_trades`）、持仓刷新（`get_positions`）、行情
（`get_ticker`），以及 Binance 的 listenKey。

也就是说，**这个进程里的任何一次 HTTP 请求都不在关键路径上了**。如果延迟剖析里冒出来一个，
说明有东西被加错了地方。

**每一个字段映射都是从对应的 Python OMS / EMS 抄过来的，不是照交易所文档写的。**
Python 已经和真实交易所对账很久了，文档没有。两者冲突时以运行中的代码为准；
任何非故意的分歧都是一个只会在生产环境冒出来的 bug。

有三处分歧是**故意的**，源码里都标了 `DIVERGENCE`：

1. **`post_only` 发的是 JSON bool，不是字符串 `"true"`。** Python 发 `"true"` 然后
   Deribit 帮它转了，但 bool 才是文档规定的类型。**这条未经验证——上生产前先在 testnet
   上验**，因为这是 venue 层唯一一处做了和运行中的 Python 不一样的事。
2. **没有 `timestamp` 的成交保持在 epoch**，而不是打上 `utcnow()`。Python 的兜底做法
   让"交易所侧延迟"和"零延迟"变得无法区分；一个明显错误的 epoch 是看得见的。
3. **无法识别的 `order_state` 会被上报，而不只是走默认值。** Python 把任何未知状态静默
   映射成 `open`；这里保留了那个行为（整条丢掉更糟），但同时抛出 `unknown_order_state`
   指标，以便计数和告警。

#### 移植时在 Python 里发现的一个 bug

`_parse_order` 和 `_parse_fill` 用 `datetime.fromtimestamp(ts / 1000)` 构造时间戳，
返回的是**本地时间**；而 `_handle_portfolio_update` 和 `Order` 模型的默认值用的是
`datetime.utcnow()`，是 **UTC**。两者混在同一个对象里。

在 UTC 服务器上看不出任何差别，所以它一直活着。在任何非 UTC 时区的机器上，
Deribit 来的 `created_at` / `updated_at` 会和其它一切整体偏移一个时区量——
静默污染任何比较它们的东西，包括延迟测量和按时间做的对账。**C++ 版全程用 UTC。**

### `net/` — WebSocket 协议层，手写

RFC 6455 客户端侧：分帧、掩码、分片重组、开场握手。不碰 socket 也不碰 TLS ——
这一层是纯字节变换，所以能在完全没有网络的情况下穷举测试。

| 头文件 | 干什么 |
|---|---|
| `ws_frame.h` | 解帧（零拷贝，返回指向你缓冲区里的指针）和编帧。按字长做掩码。UTF-8 校验。close code 解析。 |
| `ws_message.h` | 重组分片；把交织进来的控制帧**直接透传**，这样消息中间来的 ping 能立刻回。 |
| `ws_handshake.h` | 构造 upgrade 请求，**严格**校验响应。 |
| `crypto_lite.h` | SHA-1 + base64 算 `Sec-WebSocket-Accept`，SHA-256 + HMAC-SHA256 供 Bybit 和 OKX 的签名登录。本地实现而不用 OpenSSL——四个都是每连接跑一次，四个都有权威的公开测试向量。 |
| `byte_buffer.h` | 连续接收缓冲区，带压缩整理。**一辈子只分配一次。** |
| `tcp_socket.h` | 非阻塞 TCP。`TCP_NODELAY` 默认开；暴露 Linux 的 `TCP_QUICKACK` / `SO_BUSY_POLL` / keepalive 旋钮。 |
| `tls_stream.h` | OpenSSL 走 **memory BIO**，所以 socket 和缓冲区都留在调用方手里。证书校验和域名校验默认开。 |
| `ws_connection.h` | 把上面这些串起来的状态机，由一个非阻塞的 `poll()` 驱动。 |

#### 连接状态机

```
kTcpConnecting -> kTlsHandshaking -> kWsHandshaking -> kOpen
```

是**扁平状态机，不是协程**。那三个建连状态写成一个协程确实更好读——但 `kOpen`，
也就是每条消息真正度过一生的地方，**从不真的挂起**：`poll()` 去看的时候数据已经在缓冲区里了。
在那里用协程是为一套用不到的机制付钱，而且一旦 handle 逃进调度器，帧就是一次 45 ns 的
malloc 外加无界尾部（实测，见 `bench/bench_coroutine.cpp`）。所以这里哪儿都不用协程，
建连路径拿一点可读性换一个统一的模型。

`on_message` 交出去的是**指向接收缓冲区内部的指针**，只在那一次调用内有效。
这正是重点：venue 解析器直接从 socket 缓冲区里读它要的那 6 个字段，中间什么都不拷。
缓冲区预留了 simdjson 需要的 padding，所以这是安全的而不是堆越界读。

Ping/pong 在内部就回掉了。收不到 pong 的交易所会断开连接，而把这件事丢给应用层
正是它被忘掉的方式。

**TLS 走 memory BIO 而不是 `SSL_set_fd`。** 通常的做法是把 socket 交给 OpenSSL，
让它自己去 `read`/`write`，这会把系统调用放到一个**无法批处理、无法计时、
无法由忙轮询循环驱动**的地方，还强制多一次拷贝进 OpenSSL 的缓冲区。
这里的泵模型把这一切留在自己手里。

两个值得知道的决定：

**不提供 permessage-deflate，服务端返回了也会拒。** 压缩一个 200 字节的订单要几十微秒，
而在 colocated 链路上什么都省不下来。真开了压缩的服务端会发带 RSV1 的帧，
解码器会当协议错误处理——所以握手会**大声失败**，而不是连接过一会儿莫名其妙断掉。

**解码器在安全的地方宽松，在不安全的地方严格。** 它接受非最小的长度编码（RFC 说
*发送方*不该这么发）：为了三个浪费的字节把一条活的交易所连接中途掐掉，
比容忍它们糟糕得多。它拒绝保留位、保留 opcode、超长或分片的控制帧、非法 UTF-8，
因为这几样每一样都意味着这个流已经没法继续解析了。握手校验器则**全程严格**——
握手失败的代价是一次重连，而错误地接受一次握手意味着你在对一个不是 WebSocket 的东西
做分帧，然后很久以后才发现。

### `app/` — 协议层之上的一切

config、logging、metrics、订单状态、venue 会话、对账、持久化、ZMQ 控制面。

这是第三方依赖面**变宽**的地方：yaml-cpp、spdlog、prometheus-cpp、cppzmq、libpqxx
都只在这里出现，下面各层一个都碰不到。底下那些层只用一个编译器加 OpenSSL 就能构建，
这正是它们能被独立测试的原因。

| 组件 | 说明 |
|---|---|
| `oms/venue_session.h` | 每个交易所一个会话状态机：连接 → 鉴权 → 订阅 → 运行，失败进 backoff。**指数退避 + 抖动**，上限是 `max_reconnect_delay_seconds`。静默超时也会触发重连。 |
| `oms/order_store.h` | 内存订单缓存，通过 repository 写穿到持久层。读永远来自内存，绝不在这个线程上做阻塞 SELECT。 |
| `oms/portfolio_store.h` | 账户与持仓。 |
| `oms/reconciler.h` | 定期用 REST 和交易所对账，补回 WS 丢掉的订单和成交。它会被告知已经正常收到的 `trade_id`，避免把正常到达的成交"恢复"一遍造成重复计数。 |
| `oms/venue_rest.h` | 对账用的 REST 拉取。 |
| `ems/ems_service.h` | 下单/撤单/改单，路由到各交易所。撤单和改单需要订单的 symbol，所以它持有一个到 `OrderStore` 的查询回调。 |
| `repository/postgres.h` | `PostgresWriter` —— **一个专用写线程挂在 SpscRing 后面**，完全在热路径之外。libpqxx 没有像样的异步方案，而持久化本来就不是延迟敏感的。 |
| `transport/zmq_server.h` | ROUTER 收命令 + PUB 广播事件，和 Python 的 `transport/server.py` 线上兼容。 |
| `transport/shm_bridge.h` | 共享内存快路径，见下。 |
| `client/strategy_client.h` | 策略侧客户端，**两种传输自动选择**。 |
| `util/metrics.h` | prometheus-cpp 封在项目自己的 `MetricsClient` 后面，业务代码只调领域方法。 |

#### 引擎主循环：单线程，忙轮询

`src/engine_main.cpp`。Python 跑的是 asyncio 事件循环，每个交易所一个 task、每条命令一个
task；这里跑的是**一个循环**，依次 poll 每个 venue session、HTTP、EMS、各 reconciler、
共享内存桥、ZMQ，然后 `cpu_pause()`。

这就是整个设计的要点：**从交易所消息到订单簿的路径上，没有调度器、没有唤醒、
没有跨线程跳转。**

代价是引擎那个核 CPU 常驻 100%。这是尾延迟可控的入场费，也是 `cpp.engine_core`
（绑核配置）存在的理由。

订单更新到达时，**字节先走快路径**发给同机策略，**然后**才做控制面那份会分配内存的拷贝——
因为控制面那份不在策略的关键路径上。

#### `transport/shm_bridge.h` — 快路径

**这是整个设计存在的理由。** ZMQ 控制面是给查询、订阅和 Python 策略用的，
一次往返 ~30–60 µs 而且两端都要构造 JSON。这条路是**单向 ~0.1–0.3 µs，拷一个 POD 结构体**。
对比基准在 `bench/bench_main.cpp`。

**一个策略一对环**，因为环是 SPSC 的：

```
<dir>/axon.<strategy>.events    引擎 -> 策略  (订单更新、成交)
<dir>/axon.<strategy>.commands  策略 -> 引擎  (下单、撤单)
```

多个读者共享一个广播环需要 MPMC，那会丢掉让它快起来的那个性质。分开还意味着
**一个慢策略拖不慢另一个**。

三条硬性约定：

- **消费者必须忙轮询。** 为了省一个核而阻塞在条件变量上，等于把 2–10 µs 的 futex 唤醒
  还回去，结果比 ZMQ ipc 还差。这是入场费，也是为什么它是**逐策略选择加入**而不是默认。
- **背压是丢弃，不是阻塞。** 策略不读了，它的事件环写满，后续事件被丢弃并计数。
  为了一个卡住的策略阻塞引擎循环，会把每一路交易所行情一起干掉。
- **快路径是发出去没有回执的。** 环上的推送没有回复：交易所的答复稍后作为订单更新到达，
  和它从 WebSocket 来时一模一样。需要同步拿到 order_id 的策略必须走控制面——
  这是一个**真实的取舍，不是疏漏**，因为"等回复"正是慢路径慢的原因。

Linux 上把 `dir` 放在 `/dev/shm`（tmpfs）。放在真实文件系统上，内核会试图把环的页面写回磁盘。

策略侧的 `StrategyClient` **配了快路径就用，没配就回落到 ZMQ**，
所以策略只写一遍就能拿到当前可用的那条路。

---

## 三、实测数据

Apple M 系列笔记本，未绑核，macOS。**只能做相对比较**——一台调优过、核隔离的 Linux
机器尾部会紧得多。用 `./build/bench/axon_bench` 复现。

```
now_ticks()                              0.29 ns
Price::from_string("64000.5")            5.25 ns      vs std::stod        20.32 ns
Price::write() -> chars                  8.63 ns      vs snprintf %.9g    94.91 ns
Price::mul() [__int128, 饱和]             0.29 ns
Histogram::record()                      3.25 ns
ObjectPool acquire+release               2.62 ns      vs new/delete       14.29 ns
SPSC 环 push+pop（单线程）                2.10 ns
SPSC 环 跨线程往返                        p50 83 ns   p99 209 ns   p99.9 3.7 µs
shm 环 push+pop (216 B)                  10.48 ns
encode PlaceOrderMsg                     37.53 ns
序列化 OrderRequest (JSON)                p50 2.1 µs  p99 2.7 µs   p99.9 9.5 µs
解析 Order (JSON, DOM)                    p50 4.3 µs  p99 14.0 µs  p99.9 60.8 µs

--- websocket，288 字节的真实交易所报文 ---
解帧头                                    5.58 ns
编帧（带掩码）                             12.02 ns
掩码 payload                              5.50 ns
校验 UTF-8                                12.42 ns
解帧 + 重组（完整接收路径）                 16.25 ns
```

**关于时钟，以及基准程序为什么有两种模式。** Apple Silicon 的 `CNTFRQ_EL0` 报 1 GHz，
但计数器实际是按 ~41 ns 一步在走。信了那个标称频率，导致所有低于 41 ns 的测量都读成
"0 或者 42"——**一套看起来在工作、实际不工作的仪表**。现在 `clock_info()` 在启动时
**实测**粒度，基准程序对亚分辨率的操作**成批计时**，标记为 `[amortised]`
（只报 p50，尾部没有意义）。换新机器时，先看 `clock_info().resolution_ns`
再决定要不要相信一个 100 ns 以下的数字。

---

## 四、库和语言的选型，全部由测量决定

三个反复被问到的问题，用 `bench/` 里的基准回答，而不是靠传说。复现：

```bash
cmake --preset default -DAXON_BENCH_JSON_LIBS=ON && cmake --build build
./build/bench/axon_bench_json
./build/bench/axon_bench_coroutine
```

### 用哪个 JSON 解析器？

从一条 456 字节的 Deribit 订单更新里取 OMS 需要的那 6 个字段（p50）：

| | 解析 | vs nlohmann |
|---|---|---|
| nlohmann DOM | 4127 ns | 1× |
| rapidjson DOM | 1000 ns | 4× |
| rapidjson DOM in-situ | 833 ns | 5× |
| rapidjson SAX | 792 ns | 5× |
| **simdjson On-Demand** | **125 ns** | **33×** |
| 手写 `memmem` 扫描器 | 1105 ns | 4× |

rapidjson 确实是实打实的 4–5× 提升，但停在那里是错的：**simdjson On-Demand 在它之上
还要快 8×**，因为它**从不物化整个文档**——它惰性游走，只碰你问到的字段，一趟 SIMD 走完。
在一条"15 个字段里只要 6 个"的消息上，这个结构性差异压过了原始解析速度。

手写扫描器是意外，也是有用的教训：它**比 rapidjson SAX 还慢**。六趟独立的 `memmem`
扫缓冲区再加 `strtod`，打不过一趟向量化。**手写不会自动更快**，
而且这里它比 simdjson 慢 9× 的同时，还会在交易所调整字段顺序时静默出错。

**出站**消息则根本不需要解析器：

| | 构造 | |
|---|---|---|
| nlohmann `dump()` | 1458 ns | |
| rapidjson `Writer` | 250 ns | 6× |
| **字节模板 + `Decimal::write`** | **12 ns** | **120×** |

结论：ZMQ 控制面继续用 nlohmann（冷路径，它的易用性在那里比纳秒值钱）；
交易所行情用 **simdjson On-Demand**；出站订单消息用字节模板 + 定点数直写。
**rapidjson 在这个设计里没有位置。**

集成注意：simdjson On-Demand 要求输入末尾之后有 `SIMDJSON_PADDING` 个可读字节，
所以 `net/ByteBuffer` 需要多分配这么多。

### C++23 还是 C++26？

Apple Clang 16 接受 `-std=c++26`，这什么也说明不了：

```
std::expected      202211   可用 (C++23)
std::print         202207   可用 (C++23)
coroutines         201902   可用 (C++20)
pack indexing      缺失     (一个 C++26 核心语言特性)
ranges::zip        缺失     (一个 C++23 库特性)
```

那个 flag 在这里是个空壳。真正能改变这个代码库的 C++26 特性——**反射 (P2996)**，
它能像 Python 的 `dataclasses.fields()` 那样，从结构体定义直接生成整个
`transport/wire.cpp`——**任何已发布的编译器都还没实现**。连 C++23 的库在这个 libc++ 里都不完整。

结论：**留在 C++20**。`std::expected` 能把现在这些"optional + 错误字符串"的返回值整理干净，
以后升到 C++23 是合理的，但它买不到任何性能，而且约束条件是生产工具链而不是这台笔记本。
等反射真的落地再说。

### 用协程吗？

量过了，答案是"用，但不是到处用"：

| | ns |
|---|---|
| 普通函数调用链（基线） | 2.7 |
| 协程，帧被消除 (HALO) | 2.7 |
| **协程，帧逃逸** | **45** |
| 裸 `resume()`，帧是热的 | 0.5 |

**HALO（堆分配消除）**触发时，协程是免费的。触发不了的时候——也就是 handle 被存起来、
排进队列、或者跨越接口的任何时候，即任何真实的调度器——帧就是一次 `malloc`，
**45 ns 外加无界尾部**。

而且 **HALO 不能指望**。基准程序意外但可复现地证明了这点：池化那个变体**每次运行都恰好
记录 1000 次分配**——就是预热循环——测量循环里**零次**。同一个协程、同一个 lambda、
同一个函数里的两个循环，优化器在一个里消除了帧，另一个里没有。**源码上没有任何东西能区分它们。**

结论：**热路径（recv → parse → decide → send）不用协程**。它在那里根本不挂起——
`poll()` 去看的时候数据已经在缓冲区里了——所以那套机制是白付钱。
**冷路径用协程**：连接、TLS 握手、WebSocket 握手、鉴权、订阅、重连退避。
这些天然是"顺序执行 + 等待"，也正是手写状态机最容易长 bug 的地方。
给它们一个带池化 `operator new` 的 `promise_type`，这样不管 HALO 触发与否分配都是有界的，
并且由同一个忙轮询循环驱动，而不是拉进来一个 `io_context`。

### 测量唯一一次真正逼出来的优化

第一次跑 WebSocket 基准时，UTF-8 校验在 288 字节的 payload 上要 **99.8 ns**——
是解帧的 18 倍，基本上就是整条接收路径。交易所行情是 JSON，实际上全是 ASCII，
所以校验器改成**一次测 8 个字节的高位**，只有某一位被置上时才回落到逐字节解码：

```
校验 UTF-8         99.78 ns  ->  12.42 ns
解帧 + 重组         96.89 ns  ->  16.25 ns
```

**正确性没有变**：所有 overlong 编码、代理对、截断的测试仍然通过，
另外新增了在 8 字节步长的**每一个偏移**上都植入非法序列的测试——
那正是一个粗心的快路径会漏掉的地方。

这就是整个项目围绕的那个循环：**测量 → 找出唯一那个占大头的东西 → 修它 → 再测**。

---

## 五、测试

395 个单测。`ctest --test-dir build`。

有几个分量比其它的重：

- **`python_wire_compat`** —— 把 41 个标准对象从 C++ 输出成 JSON，交给**真实的** Python
  `transport/serialization.py`，断言 Python 能重建出每个 dataclass 并且**再序列化成相同的字节**。
  覆盖了每一种枚举拼写、null 的 optional、带和不带小数的时间戳，以及从 `1e-09` 到
  `9999999.999999999` 的数值边界。**这是"现有 Python 策略仍然能用"的那个保证。**
- **`ShmRing.TwoProcessesExchangeMessages`** —— fork 出两个**真进程**传 20 万条消息，
  检查顺序和内容。这是证明那些原子操作确实是 address-free 的唯一办法。
- **`SpscRing.ConcurrentProducerConsumerPreservesEveryItemInOrder`** —— 两个线程之间
  200 万个元素，断言没有丢失、重复、乱序或损坏。
- **`test_ws_connection.cpp`** —— 进程内起一个**真的** WebSocket 服务：loopback 上的真 TCP、
  运行时生成证书的真 TLS、真分帧。**证书校验保持开启**，客户端只信任那张生成的证书，
  所以测试集里包含了"不受信证书被拒"和"域名不匹配被拒"两条。
  把传输层 mock 掉只能证明 mock 和客户端意见一致。
- **`WsFrame.DecodeNeverReadsPastTheBuffer`** —— 在 ASan 下把一个大帧的**每一个前缀**
  喂给解码器。这里一个字节的越界读，就是"对端发来的畸形帧"变成"生产环境崩溃"的方式。
- **SHA-1 和 base64 对着 RFC 3174 / RFC 4648 的测试向量**，包括 55/56/63/64/119/120 字节
  这几个能走遍 SHA-1 padding 每一个分支的长度，以及 RFC 6455 §1.3 的握手完整示例。

Sanitizer，整个测试集两个都干净：

```bash
cmake --preset asan && ctest --preset asan
cmake --preset tsan && ctest --preset tsan
```

**在环形缓冲区上跑 TSan 不是可选项**——它是内存序正确性的唯一真实证据。

### 测试真正抓到的三个 bug

值得记下来，因为每一个都是静默的：

1. **`mul()` 溢出时回绕。** `100000 × 100000 = 1e10` 超出了 `Decimal<1e9>` 的
   ±9.22e9 范围，回绕成 **−8.45e9** —— **符号翻转了**。在名义价值计算里，
   这会把一个多头变成空头，而下游没有任何东西能察觉。现在所有运算都**饱和**；
   `checked_mul` / `checked_div` 给那些"必须知道"而不只是"活下来"的调用方返回 `nullopt`。
2. **`div()` 在正负混合时舍入方向错了。** `10 ÷ −4` 得到 `−2.499999999`。
   舍入偏移被加在有符号空间里；现在改成对**绝对值**舍入再贴回符号。
3. **`clock_info().resolution_ns` 从 `CNTFRQ_EL0` 推导出来，错了 41 倍**，见上文。

---

## 六、还差什么

以下这些**还不存在**：

- **从未连过真实交易所。** 传输层是对着 loopback 上的真 TLS 服务证明的，
  解析器是对着抓下来的报文证明的，但两者都不能证明**交易所会接受我们发出去的东西**。
  这需要凭据和一个 testnet。**这是当前最重要的一步。**
- **三家永续的 WebSocket 下单从未验证过，而且现在没有退路。** 它是这个仓库里唯一一处
  字段映射来自文档而非运行中的 Python 的地方，REST 下单已经删掉，所以映射错了就是
  发不出单。**在验证之前不要用它下真钱。**
- **`Price` 和 `Qty` 是同一个类型的别名**（`decimal.h:486`），所以编译器**不会**
  抓到你把其中一个传给另一个的地方。要区分它们需要一个 phantom tag 加上跨 tag 的乘法
  （price × qty → notional）；等订单路径存在、有意义的组合都清楚了之后再做。
- **日志后端还是 spdlog。** 原计划换成 Quill（更低的调用方开销），没换。
  metrics 是 prometheus-cpp 封在项目自己的 `MetricsClient` 后面，符合原计划；
  但热路径还没有做成"per-thread POD 计数器 + 后台线程聚合"。
- **热路径还没有冻结。** 零分配、绑核这些配置项存在（`cpp.engine_core`、`cpp.lock_memory`），
  但没有一个自动化检查在守着"热路径不分配内存"这条。

> 注：早期版本的这一节列出的"没有 socket/TLS/REST/重连/OMS/EMS/持久化/ZMQ/metrics"
> 现在**都已经落地了**——`net/`、`http_client`、`venue_session` 的指数退避重连、
> `oms/`、`ems/`、`repository/postgres`、`transport/zmq_server` 都在。以代码为准。

---

## 七、下一步，按顺序

1. **网络基线测量。** 候选 region × 每个交易所，24 小时的 p99 RTT 和抖动。
   这决定部署拓扑，部署拓扑决定 `oms_service` 和 `ems_service` 怎么拆。
   **在写更多代码之前先做这个。**
2. ~~**WebSocket 协议层。**~~ 完成 —— `net/`。
3. ~~**Socket 和 TLS。**~~ 完成 —— `net/tcp_socket.h`、`net/tls_stream.h`、`net/ws_connection.h`。
4. ~~**ZMQ 控制面服务。**~~ 完成 —— `transport/zmq_server.h`。
5. ~~**OMS / EMS / 对账 / 持久化。**~~ 完成 —— `oms/`、`ems/`、`repository/`。
6. **拿一个交易所打 testnet。** 需要的东西现在都齐了，缺的是凭据。
   这一步把"能解析那些形状"变成"能用"，**它应该发生在往上再写任何代码之前。**
7. **交叉验证**：把现有的 Python `StrategyClient` 和 `tests/test_*.py` 指向 C++ 引擎跑一遍。
8. **冻结热路径**：零分配、per-thread metrics、绑核、`TCP_NODELAY`
   （忘了这一条要付出几十毫秒的代价，而且它是把上面所有努力全部作废的最常见方式）。
9. ~~**三家永续的 WebSocket 下单。**~~ 已实现，且是**唯一**的下单路径——REST 下单已删除。
   正因为没有退路，第 6 步（testnet 验证）从"后续"变成了**上线前提**。
10. 生产加固。

### 部署注意事项（Linux）

线程绑核、`isolcpus` / `nohz_full`、`mlockall`、`SO_BUSY_POLL` **全都只有 Linux 有**。
macOS 没有真正的亲和性 API，所以在这边开发，但**只把 Linux 的数字当真**。

共享内存环放 `/dev/shm`（tmpfs）—— 放在真实文件系统上，内核会试图把环的页面写回磁盘。

共享内存环要求消费者**忙轮询**。为了省一个核而阻塞在条件变量上，等于把 2–10 µs 的
futex 唤醒还回去，结果比 ZMQ ipc 还差。**热核 100% CPU 是入场费。**
