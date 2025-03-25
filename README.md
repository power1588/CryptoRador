# CryptoRador

A cryptocurrency market scanner that detects abnormal price movements and volume spikes across various exchanges.

## 功能特点

- 每30秒扫描所有交易所的现货/合约交易对
- 检测异常价格上涨（5分钟内上涨超过2%）
- 识别成交量异常放大（超过5分钟滚动平均值的5倍）
- 监控现货和期货交易对之间的价差（超过设定阈值自动报警）
- 支持实时WebSocket订阅模式，更高效地监控现货-期货价差
- 发送警报到飞书(Lark)群聊，现货-期货价差监控支持独立通知通道
- 支持多家交易所：Binance、OKX、Bybit、Gate.io
- 支持公共数据模式 - 无需API密钥即可进行基本扫描
- 提供三种运行模式：同步、异步和事件驱动，满足不同需求
- 模块化设计，便于扩展
- 增强的错误处理和重试机制
- 智能速率限制和资源管理
- 监控多个交易所（Binance、OKX、Bybit、Gate等）的价格和成交量数据
- 实时检测异常价格波动和成交量突增
- 监控交易所内的现货-期货基差变化
- **跨交易所永续合约价差监控与套利机会检测**
- 通过Lark（飞书）发送实时通知
- 支持自定义价格变动阈值和监控参数
- **基于WebSocket的高效数据订阅机制**

## 安装步骤

1. 克隆此仓库
2. 安装所需的包:
   ```
   pip install -r requirements.txt
   ```
3. 复制 `.env.example` 为 `.env` 并配置您的设置:
   ```
   cp .env.example .env
   ```
4. 编辑 `.env` 文件配置您的偏好设置

## 配置飞书(Lark)机器人

1. 在飞书中创建一个群组
2. 在群设置中选择"添加机器人"
3. 选择"自定义机器人"
4. 设置机器人名称(如"CryptoRador警报")
5. 获取webhook URL
6. 如需安全验证，可以启用签名并获取Secret
7. 将获取的URL和Secret添加到.env文件

对于异常价格和成交量通知:
```
LARK_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/你的webhook令牌
LARK_SECRET=你的签名密钥(如果启用了签名)
```

对于现货-期货价差通知(可设置独立通道):
```
SPOT_FUTURES_LARK_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/你的webhook令牌
SPOT_FUTURES_LARK_SECRET=你的签名密钥(如果启用了签名)
```

## 运行方式

CryptoRador提供三种不同的运行模式，适合不同的使用场景：

### 同步版本（标准）
使用标准的同步请求方式，定期轮询交易所数据：
```
python run.py
```

### 异步版本（高性能）
使用asyncio异步处理，提高数据获取并发性能，但仍使用轮询方式：
```
python run_async.py
```

### 事件驱动版本（实时推送）
使用WebSocket连接订阅实时数据，基于事件驱动设计，完全实时响应：
```
python run_event_driven.py
```

### 基于订阅的现货-期货价差监控（专用实时监控）
使用WebSocket连接专门监控现货和期货交易对之间的价差：
```
python run_subscription_spot_futures.py
```

### 5. 跨交易所永续合约价差监控模式（REST API版）

```bash
python run_perp_exchange_monitor.py
```

### 6. 跨交易所永续合约价差监控模式（WebSocket版，推荐）

```bash
python run_perp_exchange_ws_monitor.py
```

## 三种模式对比

| 特性 | 同步版本 | 异步版本 | 事件驱动版本 | 订阅式价差监控 |
|------|----------|----------|--------------|--------------|
| 数据获取方式 | 轮询 | 并发轮询 | WebSocket推送 | WebSocket推送 |
| 实时性 | 低 | 中 | 高 | 极高 |
| 资源占用 | 高 | 中 | 低 | 低 |
| 响应速度 | 慢 | 较快 | 即时 | 即时 |
| 适用场景 | 简单监控 | 大量交易对扫描 | 实时监控和预警 | 现货-期货价差监控 |
| 延迟 | 30秒+ | 5-10秒 | 毫秒级 | 毫秒级 |
| 交易所负载 | 高 | 中高 | 低 | 低 |

## 事件驱动版本特点

事件驱动版本是CryptoRador的最新模式，主要优势：

- **实时数据推送**：通过WebSocket接收K线更新，无需等待轮询间隔
- **极低延迟**：在行情变动的几毫秒内即可检测到异常波动
- **更高效的资源利用**：减少不必要的数据请求，降低服务器负载
- **降低交易所API压力**：遵循交易所的最佳实践，避免频繁请求
- **资源消耗更低**：降低CPU和网络带宽使用

### 事件驱动版本的命令行参数

事件驱动版本提供了便捷的命令行参数：

```
python run_event_driven.py --help
```

常用参数:

```
基本参数:
  -e, --exchanges EXCHANGES     要监控的交易所，逗号分隔
  -m, --markets MARKETS         要监控的市场类型，逗号分隔(spot,future)
  -l, --lookback LOOKBACK       历史K线数量

分析参数:
  -p, --price-threshold PRICE   价格上涨阈值(百分比)
  -v, --volume-threshold VOL    成交量放大阈值(倍数)
  --log-level {DEBUG,INFO,...}  日志级别
```

例如，监控Binance和OKX的现货市场，价格阈值设为3%：

```
python run_event_driven.py -e binance,okx -m spot -p 3.0
```

### 异步版本的命令行参数

异步版本支持通过命令行参数调整配置，无需修改.env文件:

```
python run_async.py --help
```

常用参数:

```
基本参数:
  -i, --interval INTERVAL     扫描间隔(秒)
  -e, --exchanges EXCHANGES   要扫描的交易所，逗号分隔
  -m, --markets MARKETS       要扫描的市场类型，逗号分隔(spot,future)
  -l, --lookback LOOKBACK     历史回溯时间(分钟)

性能参数:
  -c, --concurrent CONCURRENT 最大并发请求数
  -t, --timeout TIMEOUT       请求超时时间(秒)
  -r, --retries RETRIES       最大重试次数
  --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}  日志级别
```

例如，扫描Binance的现货市场，使用40个并发请求:

```
python run_async.py -e binance -m spot -c 40
```

## 测试Lark通知

要测试飞书通知功能是否正常工作:
```
python src/notifier/test_lark.py
```
这将发送测试消息到您配置的飞书群组。

## 配置项说明

### 公共数据模式

默认情况下，CryptoRador运行在公共数据模式，这意味着它只使用公共API，不需要任何API密钥。

```
# 设置为true只使用公共API（不需要API密钥）
USE_PUBLIC_DATA_ONLY=true
```

如果您需要使用私有API（用于可能需要认证的功能），请将此设置为`false`并提供您的API密钥。

### 交易所配置

您可以配置要监控的交易所:

```
EXCHANGES=binance,okx,bybit,gate
```

### 扫描参数

配置扫描器行为:

```
# 扫描间隔(秒)
SCAN_INTERVAL_SECONDS=30
# 价格上涨阈值(百分比)
MIN_PRICE_INCREASE_PERCENT=2.0
# 回溯分钟数
LOOKBACK_MINUTES=5
# 成交量放大倍数阈值
VOLUME_SPIKE_THRESHOLD=5.0
# 市场类型(现货和/或合约)
MARKET_TYPES=spot,future
# 现货-期货价差阈值(百分比)
SPOT_FUTURES_DIFF_THRESHOLD=0.1
```

### 现货-期货价差监控

现货-期货价差监控功能可以检测交易所上同时存在的现货和期货交易对之间的价格差异:

1. 系统会将相同基础资产的现货和期货交易对进行匹配（如BTC/USDT与BTC/USDT-PERP）
2. 计算两者之间的价格差异百分比
3. 当价格差异超过配置的阈值（默认0.1%）时，触发报警
4. 报警消息会发送到配置的飞书通知通道

价差通知可以配置独立的飞书通知通道，与异常价格和成交量监控分开:

```
# 现货-期货价差监控参数
SPOT_FUTURES_DIFF_THRESHOLD=0.1     # 现货与期货价格差异阈值(百分比)
SPOT_FUTURES_BASIS_DIRECTION=both  # 'both', 'premium', 'discount'
```

如果未配置专用通知通道，系统将使用默认的Lark通知设置。

您可以使用以下命令单独测试现货-期货价差监控功能:
```
python test_spot_futures_monitor.py  # 轮询模式测试
python run_subscription_spot_futures.py  # 订阅模式（推荐）
```

### 基于订阅的现货-期货价差监控

订阅式监控使用WebSocket订阅交易所实时K线数据，比轮询式监控具有以下优势：

1. **更低延迟**: 实时接收价格更新，无需等待轮询
2. **更高效**: 减少不必要的API请求，只处理变化的数据
3. **更实时**: 毫秒级响应，及时捕捉瞬时价差
4. **更节省资源**: 减少带宽和CPU使用

运行订阅模式:
```
python run_subscription_spot_futures.py
```

订阅模式支持的命令行参数:
```
-e, --exchanges      要监控的交易所，逗号分隔
-t, --threshold      价差阈值(百分比)
-d, --direction      基差方向监控(both=双向, premium=升水, discount=贴水)
-i, --interval       检查价差的间隔(秒)，默认为5秒
-c, --cooldown       报警冷却时间(秒)，默认为300秒
--log-level          日志级别
```

例如，监控Binance和OKX交易所，价差阈值设为0.2%:
```
python run_subscription_spot_futures.py -e binance,okx -t 0.2
```

仅监控价格升水(premium)情况:
```
python run_subscription_spot_futures.py -d premium
```

仅监控价格贴水(discount)情况:
```
python run_subscription_spot_futures.py -d discount
```

### 异步扫描器高级参数

异步版本提供了更多高级配置选项:

```
# 最大并发请求数
MAX_CONCURRENT_REQUESTS=20
# 请求超时时间(秒)
REQUEST_TIMEOUT_SECONDS=30
# 速率限制调整因子(0-1)
RATE_LIMIT_FACTOR=0.8
# 最大重试次数
MAX_RETRIES=3
# 重试延迟时间(秒)
RETRY_DELAY_SECONDS=2
```

## 错误处理

所有版本都实现了多层错误处理:

1. **无效交易对过滤**: 自动识别和排除无效或已下线的交易对
2. **超时重试**: 智能重试超时的请求
3. **速率限制处理**: 使用指数退避算法处理速率限制错误
4. **连接恢复**: 在连接问题后能够自动恢复
5. **资源清理**: 确保在应用关闭时正确释放资源

## 架构设计

- `fetcher`: 使用CCXT的数据获取模块
- `analyzer`: 市场分析算法
- `notifier`: 向飞书发送警报
- `scheduler`: 任务调度和管理 (同步版本)
- `config`: 配置设置

## 各版本架构

- **同步版本**: 使用传统的请求-响应模式，定期轮询数据
  - `fetcher/data_fetcher.py`: 同步数据获取
  - `main.py`: 主应用程序

- **异步版本**: 使用asyncio提高并发性能，但仍使用轮询
  - `fetcher/async_data_fetcher.py`: 使用ccxt.pro和asyncio的异步数据获取器
  - `async_main.py`: 异步主应用程序

- **事件驱动版本**: 基于WebSocket的实时推送模式
  - `fetcher/websocket_data_subscriber.py`: WebSocket订阅器
  - `analyzer/realtime_analyzer.py`: 实时分析器
  - `event_driven_main.py`: 事件驱动主程序

## 使用方法

### 资源清理与内存管理

CryptoRador事件驱动版本使用WebSocket连接实时获取数据，程序退出时会自动清理所有资源，包括：

1. 关闭所有交易所WebSocket连接
2. 释放所有aiohttp会话和连接
3. 取消所有未完成的异步任务
4. 清理内存中的数据缓存

这确保了程序在退出时不会有资源泄漏，特别是在长时间运行后进行重启的情况下。

如果你观察到"Unclosed client session"或"Unclosed connector"警告，可以尝试以下解决方法：

1. 使用Ctrl+C而不是终端的强制关闭按钮来停止程序
2. 确保程序有足够的时间(1-2秒)来清理资源
3. 如果问题持续存在，可以升级到最新版本，或者检查日志中的错误信息

### 跨交易所永续合约价差监控

跨交易所永续合约价差监控模式可以检测不同交易所之间相同币种的USDT永续合约价格差异:

1. 系统会自动发现各交易所中共有的永续合约交易对
2. 计算不同交易所之间的价格差异百分比
3. 当价格差异超过配置的阈值（默认0.2%）时，触发报警
4. 报警消息会发送到配置的飞书通知通道

价差通知可以配置独立的飞书通知通道:

```
# 跨所永续合约价差监控参数
PERP_EXCHANGES=binance,gate
PERP_DIFF_THRESHOLD=0.2
PERP_BLACKLIST=LINA,BIFI,SUN  # 永续合约黑名单，逗号分隔
```

提供两种运行模式:

- REST API模式: 周期性查询API获取价格数据 (`run_perp_exchange_monitor.py`)
- WebSocket模式: 通过WebSocket实时订阅价格变化，更加高效和及时 (`run_perp_exchange_ws_monitor.py`)

你可以使用`PERP_BLACKLIST`设置不需要监控的币种，比如已知即将下架的币种或者波动过大不适合套利的币种。

建议使用WebSocket模式来监控价差，能够更及时地捕捉套利机会。

### Lark（飞书）配置

```
# Lark (飞书) 配置 - 异常价格和成交量通知
LARK_WEBHOOK_URL="your_lark_webhook_url"
LARK_SECRET="your_lark_secret"

# Lark (飞书) 配置 - 专用于现货-期货价差通知
SPOT_FUTURES_LARK_WEBHOOK_URL="your_spot_futures_lark_webhook_url"
SPOT_FUTURES_LARK_SECRET="your_spot_futures_lark_secret"

# Lark (飞书) 配置 - 专用于跨所永续合约价差通知
PERP_EXCHANGE_LARK_WEBHOOK_URL="your_perp_exchange_lark_webhook_url"
PERP_EXCHANGE_LARK_SECRET="your_perp_exchange_lark_secret"
```
