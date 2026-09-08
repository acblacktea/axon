# axon_market_data (C++)

加密货币行情服务。通过 WebSocket 连接各交易所,在本地维护订单簿,并通过 ZMQ 发布数据。

目前在以下交易所支持 Depth、Ticker 和 Kline:

| 交易所 | 市场类型 (`market_type`) | 说明 |
|---|---|---|
| **Binance** | `spot` / `usdt_futures` / `coin_futures` | 本地订单簿由 REST 快照 + `@depth` 增量维护 |
| **OKX** | (不使用 — 由 symbol 后缀决定 instId) | 订单簿来自 `books` 频道;K 线走 `/ws/v5/business` 上的第二条连接 |
| **Bybit** | `spot` / `usdt_futures` (linear) / `coin_futures` (inverse) | V5 公共流;BBO 来自 `orderbook.1` |
| **Hyperliquid** | (不使用 — 由 coin 名决定市场) | `l2Book` 推送的是完整订单簿,因此没有增量状态 |

## 依赖

- C++23 编译器 (Clang 16+ / GCC 13+)
- CMake 3.25+
- Ninja (推荐)
- vcpkg

第三方库(由 vcpkg 管理):
- Boost.Beast / Boost.Asio — WebSocket 与 HTTP
- simdjson — JSON 解析
- glaze — JSON 序列化
- cppzmq — ZMQ 消息
- yaml-cpp — 配置解析
- spdlog — 日志
- OpenSSL — TLS

## 构建

### 1. 安装前置工具 (macOS)

```bash
brew install cmake ninja pkg-config autoconf automake libtool
```

### 2. 安装 vcpkg

```bash
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
~/vcpkg/bootstrap-vcpkg.sh
```

### 3. 配置与编译

```bash
cd cpp/axon_market_data
cmake -B build -G Ninja -DCMAKE_TOOLCHAIN_FILE=~/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build build
```

首次构建耗时较长(vcpkg 需要编译全部依赖),后续增量构建很快。

## 使用

### 启动行情服务

```bash
./build/axon_market_data config.yml
```

### 订阅 (C++)

```bash
./build/mds_subscriber                                          # 全部 topic
./build/mds_subscriber tcp://localhost:5558 binance_spot.depth   # 仅 depth
```

### 订阅 (Python)

```bash
pip install pyzmq
python example/subscriber.py
python example/subscriber.py tcp://localhost:5558 binance_spot.depth.ETH_USDT_SPOT
```

## 配置

编辑 `config.yml`:

```yaml
zmq:
  pub_address: "tcp://*:5558"

exchanges:
  - name: binance
    market_type: spot           # spot / usdt_futures / coin_futures
    update_speed: "100ms"       # 仅 binance:@depth 推送频率
    depth_levels: 400           # 每侧发布的档位数(默认 400)
    subscriptions:
      depth:
        - ETH_USDT_SPOT
      ticker:
        - ETH_USDT_SPOT
      kline:
        - ETH_USDT_SPOT

  - name: okx
    market_type: spot
    verify_checksum: false      # 仅 okx:CRC32 订单簿校验
    subscriptions:
      depth:
        - ETH_USDT_SPOT

  - name: bybit
    market_type: usdt_futures   # spot / usdt_futures / coin_futures
    subscriptions:
      depth:
        - ETH_USDT_PERP

  - name: hyperliquid
    subscriptions:
      depth:
        - ETH_USD_PERP
```

各交易所注意事项:

- **OKX** 忽略 `market_type` —— `ETH_USDT_SPOT` 映射为 `ETH-USDT`,
  `ETH_USDT_PERP` 映射为 `ETH-USDT-SWAP`。`verify_checksum` 会重算前 25 档的
  CRC32,不匹配时重新订阅订单簿;OKX 目前发送 `"checksum":0`,会被视为
  "未提供" 而跳过。
- **Bybit** 的现货 `tickers` topic 不含买卖价,因此所有 category 的 ticker
  数据都取自 `orderbook.1.{symbol}`。Depth 使用 `orderbook.50`。
- **Hyperliquid** 同样忽略 `market_type`:`ETH_USD_PERP` 映射为 `ETH`,
  `PURR_USDC_SPOT` 映射为 `PURR/USDC`。每条 `l2Book` 消息都是完整订单簿,
  因此 depth 事件的 `event_type` 为 `"snapshot"`、`sequence` 为 `0` —— 该交易所
  不发布序列号,也没有增量订单簿频道。`l2Book` 使用默认模式订阅:每侧 20 档、
  约 0.24 msg/s。`fast: true` 变体可达约 2 msg/s 但每侧只有 5 档;而 `bbo`
  频道已经以约 8 msg/s 覆盖了盘口,所以 depth 这边保留档位数。
- K 线固定为 1 分钟。Bybit 和 Hyperliquid 并非在所有市场都发布成交额 /
  成交笔数,因此这些字段可能为 `0`。

### `depth_levels`

每侧发布的档位数,按交易所条目单独设置,默认 400。它同时决定了**各适配器向交易所
请求多少档** —— 没有任何一家接受单纯的档位数,因此每个适配器都会把它翻译成自己的
请求参数,并在启动时打印结果:

```
[binance_spot]         depth_levels=400 via REST limit=400
[binance_usdt_futures] depth_levels=400 via REST limit=500
[okx]                  depth_levels=400 via channel "books"
[bybit_linear]         depth_levels=400 via orderbook.1000
[hyperliquid]          depth_levels=20  via l2Book fast=false
```

| 交易所 | 请求参数 | 可接受的值 | 上限 |
|---|---|---|---|
| Binance 现货 | REST `limit` | 任意值 | 5000 |
| Binance 合约 | REST `limit` | 5/10/20/50/100/500/1000 —— 向**上**取整(`limit=400` 会被拒绝并返回 `-4021`) | 1000 |
| OKX | 频道 | `books5`(5 档)或 `books`(400 档) | 400 |
| Bybit | topic | `orderbook.`50/200/1000 —— 向**上**取整(不存在 `orderbook.400`) | 1000 |
| Hyperliquid | `l2Book` 的 `fast` 标志 | `true` → 5 档,`false` → 20 档 | 20 |

超过交易所上限的请求会被截断,并在启动时记录日志。

Binance 是唯一一个请求量刻意**不**按 1:1 计算的交易所。它的 REST 调用只用于给
订单簿播种 —— 之后的增量流只报告发生变化的档位,因此比快照更深的档位在它变动之前
一直不可见,订单簿会慢慢向下侵蚀。由于快照是每次同步一次性的开销,而非每条消息的
开销,适配器会请求双倍余量(`depth_levels: 400` → 现货 `limit=800`,合约
`limit=1000`)。

调高它之前需要知道两项代价:

- **带宽。** 400 档时,单个 symbol 在 Binance 或 OKX 上约 280 KB/s,
  Bybit 上约 134 KB/s。乘以 symbol 数量。
- **部分交易所档位越深推送越慢。** Bybit 的 `orderbook.50` 约 44 msg/s,
  而 `orderbook.1000` 约 5 msg/s;Hyperliquid 的 `fast` 订单簿约 2 msg/s,
  而 20 档的约 0.24 msg/s。两种情况下盘口的新鲜度都不受影响 —— 那来自独立的
  ticker 流。

单 symbol 实测,`depth_levels: 400` 对比 `depth_levels: 5`:

| | 400 档 | 5 档 |
|---|---|---|
| Binance 现货 | 279 KB/s @ 9.9/s | 4.9 KB/s @ 9.9/s |
| OKX | 295 KB/s @ 10.0/s | 3.6 KB/s @ 7.2/s |
| Bybit linear | 135 KB/s @ 5.0/s | 18.6 KB/s @ 39.0/s |
| Hyperliquid | 0.3 KB/s @ 0.2/s(20 档) | 0.9 KB/s @ 1.9/s |

## 日志

```yaml
logging:
  file: "logs/axon_market_data.log"   # 按日轮转 -> logs/axon_market_data_YYYY-MM-DD.log
  level: info                   # trace / debug / info / warn / error / critical
  max_files: 30                 # 保留多少天的历史;0 表示全部保留
```

日志同时输出到控制台和文件。两者都会给级别名着色 —— 文件中自带 ANSI 转义序列,
因为 file sink 会忽略 spdlog 的 `%^`/`%$` 着色区间。转义序列只包裹级别这个单词,
所以文件依然可以正常 grep:

```bash
tail -f logs/axon_market_data_$(date +%F).log      # 在终端中显示颜色
grep error logs/axon_market_data_2026-08-28.log    # 仍然可以匹配
sed -E 's/\x1b\[[0-9;]*m//g' logs/*.log     # 为日志采集器剥离颜色
```

设置 `file: ""` 则只输出到控制台。

`warn` 及以上级别会立即刷盘,其余在一秒内刷盘。

历史清理在启动时执行,而不是完全交给 spdlog:`daily_file_sink` 只在轮转时清理,
从不处理已经存在的积压,而且回溯到第一个缺失的日期就会停止 —— 否则只要停机一天,
就会让比这个缺口更早的所有文件成为孤儿。

## ZMQ 消息格式

两帧消息:`[topic, json]`

Topic 格式:`{exchange}.{data_type}.{symbol}`

示例:
- `binance_spot.depth.ETH_USDT_SPOT`
- `binance_usdt_futures.kline.BTC_USDT_PERP`
- `okx.ticker.ETH_USDT_SPOT`
- `bybit_linear.depth.ETH_USDT_PERP`
- `bybit_inverse.depth.BTC_USD_PERP`
- `hyperliquid.depth.ETH_USD_PERP`

## IDE 配置 (VSCode)

安装 **clangd** 扩展,然后在 `.vscode/settings.json` 中加入:

```json
{
  "clangd.arguments": [
    "--compile-commands-dir=cpp/build"
  ]
}
```
