# 端到端测试工作区

在这里把行情服务（MDS）和下单引擎（EMS + OMS）一起跑起来，用来接策略、做端到端测试。
所有命令都在 `test/` 目录下执行：日志、熔断文件等相对路径都以它为准。

```
test/
  bin/axon_market_data   行情服务
  bin/axon_engine        下单引擎（EMS + OMS + 风控）
  bin/mds_subscriber     行情订阅示例，看行情有没有在推
  bin/axon_strategy      策略客户端示例：查询、看回报、测连通
  mds_config.yml         行情配置：Binance U 本位永续，BTC / ETH
  engine_config.yml      引擎配置：Binance 测试网，带风控
```

`bin/` 里是**拷贝过来的编译产物**，不进 git。改了代码之后要重新编译再拷：

```bash
cmake --build ../cpp/axon_order_execution/build -j && cmake --build ../cpp/axon_market_data/build -j
cp ../cpp/axon_order_execution/build/{axon_engine,axon_strategy} \
   ../cpp/axon_market_data/build/{axon_market_data,mds_subscriber} bin/
```

编译参数（目前是 Release、`-O3`，没开 `-march=native` 和 LTO），以及怎么往上开，见 [cpp/BUILD.md](../cpp/BUILD.md) 第 4 节。

## 启动

两个服务各开一个终端：

```bash
# 1. 行情（公开数据，不需要密钥）
./bin/axon_market_data mds_config.yml

# 2. 引擎（Binance 测试网；密钥只从环境变量读）
source ~/.config/axon/binance-testnet.env
./bin/axon_engine --config engine_config.yml
```

## 确认两边都通了

```bash
# 行情：应该持续打印 ticker / depth / kline
./bin/mds_subscriber tcp://127.0.0.1:5558 binance_usdt_futures.ticker

# 引擎：查挂单，然后持续看订单和成交回报
./bin/axon_strategy --id my_strategy --router tcp://127.0.0.1:5555 --pub tcp://127.0.0.1:5556 --orders
./bin/axon_strategy --id my_strategy --router tcp://127.0.0.1:5555 --pub tcp://127.0.0.1:5556 --watch
```

指标：引擎 <http://127.0.0.1:9100/metrics>，行情 <http://127.0.0.1:9101/metrics>。

## 策略怎么接

| | 地址 | 协议 |
|---|---|---|
| 行情 | `tcp://127.0.0.1:5558`（ZMQ SUB） | 两帧 `[topic, json]`，topic 为 `{exchange}.{data_type}.{symbol}`，例如 `binance_usdt_futures.ticker.BTC_USDT_PERP` |
| 下单 / 查询 | `tcp://127.0.0.1:5555`（ZMQ DEALER） | C++ 用 `axon::client::StrategyClient`，Python 用 `python_deprecated/axon_order_execution/axon_order_execution/client/strategy_client.py` |
| 订单 / 成交回报 | `tcp://127.0.0.1:5556`（ZMQ SUB） | 按 strategy_id 订阅 |

C++ 策略下单用 `StrategyClient::submit_order()`，它会区分三种结果：

- `kAccepted`：交易所已接受。
- `kRejected`：被拒绝（交易所拒单，或者风控拒单，错误信息以 `risk:` 开头），可以修改后重试。
- `kUnknown`：请求已经发出，但没在超时内收到结果，**这一单可能已经在交易所生效**。不要直接重发，先用
  `internal_order_id` 在回报或 `get_active_orders()` 里确认。用同一个 `internal_order_id` 重发会被引擎拒绝。

## 要注意的坑

- **行情是主网的，下单是测试网的。** 行情服务没有测试网地址，两边价格可能有差距。用行情价格去挂测试网的限价单时，
  要考虑测试网 ±5% 的 PERCENT_PRICE 限制，以及引擎风控 5% 的价格偏离限制（它用的是测试网价格）。
- **风控限额是按交易所单位算的**：BTCUSDT 的数量单位是 BTC。持仓上限包括账户上**已有的持仓**，测试网账户如果
  已经有仓位，可用额度会相应减少。
- **熔断开关**：`touch axon.kill` 后拒绝一切新单并撤掉所有挂单，`rm axon.kill` 恢复。也可以用
  `kill -USR1 <engine-pid>` / `kill -USR2 <engine-pid>`。
- 不配 `database:`，订单和成交历史只保存在内存里，引擎重启后就没了。
- 端口：引擎 5555 / 5556 / 9100，行情 5558 / 9101。如果和别的程序冲突，两边的配置和策略要一起改。
