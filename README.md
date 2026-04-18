# QLiBRD-QMT

用多个 AI 大模型（Claude、GPT、GLM、Kimi、MiniMax 等）独立选股，通过 QMT 模拟盘自动下单，每日结算复盘并追踪各 Agent 表现的实验项目。

简单来说：**AI 调动 QMT 智能选股**。

> 这是一个个人娱乐项目，用于探索 AI Agent 在股票选股中的应用。详见底部免责声明。

## 项目定位

这不是一个开箱即用的量化平台，而是一个 AI Agent Trading Lab：

- 用多个 LLM Agent 独立生成次日买入候选；
- 每个 Agent 拥有独立虚拟资金账户，用于长期追踪表现；
- 可把多个 Agent 的信号合并后交给 QMT 模拟盘执行；
- 每日收盘后根据真实/模拟成交和价格数据做结算与复盘；
- 将命中率、累计收益、置信度分位收益、持仓和交易流水展示在 Web Dashboard 中。

## 核心功能

| 模块 | 能力 |
|------|------|
| 多 Agent 选股 | 支持 OpenAI-compatible 与 Anthropic-compatible API，可接入 Claude、GPT、GLM、Kimi、MiniMax、本地 vLLM 等模型 |
| 候选股票池 | 从市场数据和规则筛选候选股票，生成供 Agent 判断的 market brief |
| 信号归一化 | 将不同模型输出解析为统一 `TradeSignal`，并按资金池换算买入数量 |
| 合并执行 | 可将多个 Agent 的同日信号合并，同一股票按数量合并后交给模拟账户执行 |
| QMT 接入 | 通过 xtquant 连接本地 QMT 模拟/实盘客户端，下发买卖委托并查询订单/成交 |
| 结算系统 | 根据成交、持仓、价格和现金计算每日快照、盈亏、账户权益 |
| Agent 复盘 | 对预测与实际表现做命中率、超额收益、经验教训和记忆写入 |
| Dashboard | 提供账户看板、Arena 首页、Agent 详情、对比页、Review、Timeline、Health 页面 |
| 调度 | 使用 APScheduler 定时生成次日信号、重试失败 Agent、盘前合并信号、盘中执行买卖、盘后结算复盘 |

## 架构概览

```text
MongoDB market data / QMT data upload
        |
        v
Candidate Pool + Market Brief
        |
        v
Prompt Builder
        |
        v
AI Providers ── Claude / GPT / GLM / Kimi / MiniMax / vLLM / NadirClaw
        |
        v
Signal Normalizer ──> arena_signals (MongoDB)
        |
        +── per-agent virtual portfolios ──> arena_accounts / snapshots / reviews
        |
        v
Merged Signal Batch
        |
        v
Scheduler / Signal Client
        |
        v
QMT Engine / QMT Proxy ──> 本地 QMT 客户端
        |
        v
Orders / Trades / Settlement / Dashboard
```

## 典型交易日流程

```text
18:00  使用当日收盘后上传到 MongoDB 的市场数据，生成次日所有 Agent 信号
18:15-20:45  每 15 分钟重试失败或 fallback 的 Agent
09:10  读取并合并当日 Arena 信号
09:15  下发集合竞价买入委托
14:55  第一阶段卖出
14:56  撤销未成交卖单
14:57  收盘集合竞价卖出剩余仓位
15:05  日终结算、快照、复盘和记忆写入
```

## 运行环境

- Python 3.10+
- MongoDB：保存市场数据、信号、账户、成交、结算和复盘数据
- [迅投 MiniQMT](https://xuntou.net/) 客户端：本地安装并登录，xtquant 由 QMT 安装目录提供。需要开通 MiniQMT 权限（一般券商开户后可申请）
- AI 模型 API：NadirClaw 代理、本地 vLLM、或任何 OpenAI/Anthropic 兼容 API
- Windows 环境（QMT 和 xtquant 仅支持 Windows）

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 准备配置

复制或创建本地配置文件：

```bash
cp config/settings.example.yaml config/settings.yaml
```

如果仓库中暂未提供 `settings.example.yaml`，请参考 `config/settings.yaml` 的结构自行创建本地文件。公开仓库不应提交真实账号、API Key、内网 IP、MongoDB URI 或 QMT 账户信息。

关键配置包括：

```yaml
accounts:
  simulation:
    qmt_path: "D:\\QMTSIM\\userdata_mini"
    account_id: "YOUR_SIM_ACCOUNT_ID"
    account_type: "STOCK"
    proxy_port: 8011

signal_source:
  mode: "arena"          # arena / remote / mock
  remote_url: "http://YOUR_SIGNAL_SERVER/api/signals"

arena:
  enabled: true
  execution_provider: "vllm_trader_pro"
  providers:
    vllm_trader_pro:
      type: "vllm"
      base_url: "http://localhost:8000/v1"
      model: "YOUR_MODEL"
      api_key: "none"
      enabled: true
      capital_pool: 5000000

mongodb:
  uri: "mongodb://YOUR_MONGODB_HOST:27017"
  database: "qlibrd_qmt"
```

### 3. 准备模型服务

项目可以直接调用：

- OpenAI-compatible `/v1/chat/completions` 接口；
- Anthropic-compatible `/v1/messages` 接口；
- NadirClaw 本地代理；
- 本地 vLLM OpenAI-compatible 服务。

当前没有图形化页面配置 NadirClaw 或模型列表。需要在本地配置文件中写入 `arena.providers`，并保证对应服务已启动。

### 4. 启动 QMT

先打开并登录 QMT 客户端，再启动本项目。QMT 路径目前也通过配置文件设置，没有单独的控制面板。

### 5. 常用命令

```bash
# 启动 Dashboard + 交易调度
python main.py serve

# 只启动交易调度
python main.py trade

# 启动 Dashboard
python main.py dashboard

# 启动 mock 信号 API
python main.py api

# 手动生成 Arena 信号
python main.py arena-signals

# 手动执行 Arena 买入
python main.py arena-buy --account simulation

# 手动执行 Arena 卖出
python main.py arena-sell --account simulation

# 手动结算和复盘
python main.py arena-settle
python main.py arena-review

# 历史回测
python main.py backtest -f data/history_signals.csv -m paper_calc
```

## Dashboard 页面

默认 Dashboard 服务启动后可访问：

| 页面 | 用途 |
|------|------|
| `/dashboard?account=simulation` | QMT 账户资产、持仓、信号、订单、成交和结算 |
| `/api/arena/page` | Arena 总览 |
| `/api/arena/page/agent` | 单 Agent 详情、持仓、信号、记忆和提示词 |
| `/api/arena/page/compare` | 多 Agent 选股结果对比 |
| `/api/arena/page/review` | 命中率、超额收益、分位收益等复盘展示 |
| `/api/arena/page/timeline` | 每日信号、交易和复盘时间线 |
| `/api/arena/page/health` | 系统健康状态 |
| `/api/arena/page/settings` | 配置管理：信号源、Provider、账户、连通性检查 |

## 当前限制

这些限制会影响别人直接复用项目：

1. **Settings 页面已上线**：可切换模型、信号源、启停 Agent，但首次安装仍需手动编辑 `config/settings.yaml` 填入基本配置。
2. **配置仍偏本地化**：QMT 路径、MongoDB URI、模型 API、账户 ID 都需要手工写入本地 YAML 或环境变量。
3. **依赖外部数据上传**：18:00 信号生成假设当日收盘数据已经由其它程序上传到 MongoDB。
4. **QMT 强绑定 Windows**：xtquant 和 QMT 客户端环境不适合纯 Linux 服务器直接运行。
5. **缺少安装向导**：没有首次启动检查、配置校验页面或一键初始化脚本。
6. **公共发布需脱敏**：真实 API Key、账号、IP、交易记录、日志和 Agent memory 不应提交。

## 建议的后续路线

如果要从“自用实验项目”升级成别人可运行的开源项目，优先补齐：

1. `config/settings.example.yaml`：完整脱敏模板；
2. `.env.example`：API Key、MongoDB、模型服务和 QMT 路径变量；
3. Web 设置页：选择 signal source、execution provider、启停 Agent、配置 QMT 路径；
4. 启动前健康检查：MongoDB、QMT proxy、模型 API、数据日期是否可用；
5. 安装文档：从 MongoDB、QMT、NadirClaw 到 Dashboard 的完整 walkthrough；
6. 测试和 demo mode：无 QMT、无真实模型时也能跑通 mock 演示。

## 安全与脱敏

公开仓库请不要提交：

- `config/settings.yaml`
- `.env` 或任何 API Key 文件
- QMT 账号、资金账号、session id
- MongoDB 内网地址和密码
- `logs/`
- `data/` 中的真实交易数据
- `memory/` 中的 Agent 复盘记忆

本仓库 `.gitignore` 已按公开发布场景排除上述路径，但如果这些文件已经被 Git 跟踪，需要先从索引中移除后再提交。

## 免责声明

这是一个个人娱乐项目，目的是探索 AI Agent 在股票选股中的应用可能性。

- 项目中的所有 AI 模型（Claude、GPT、GLM、Kimi、MiniMax 等）产生的选股信号仅供实验参考，**不构成任何投资建议**
- 项目中的"模拟交易"是在 QMT 模拟环境中运行的纸上交易，**不涉及真实资金**
- AI 模型的预测能力有限，历史表现不代表未来收益
- 任何人如果将本项目用于真实交易，**所有风险和损失由使用者自行承担**
- 作者不对因使用本项目造成的任何直接或间接损失负责
