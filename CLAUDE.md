# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

QLiBRD-QMT 是基于 QMT (迅投) 模拟环境的自动化交易系统。从远程/本地服务器获取交易信号，按照信号指定的时间策略（集合竞价/收盘/VWAP）在本地 QMT 模拟盘中执行交易。

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| CLI entrypoint | `argparse` in `main.py` |
| Config | YAML via `pyyaml`, global cache in `app/config.py` |
| Data models | Pydantic v2 (`app/models.py`) |
| Signal API | FastAPI + uvicorn (`app/signal_api.py`) |
| HTTP client | `httpx` |
| Scheduling | APScheduler (`app/scheduler.py`) |
| Broker integration | `xtquant` (bundled with QMT at `D:\QMTSIM`) |
| Logging | Loguru |
| Data processing | pandas (backtest CSV handling) |

No database. No frontend. No CI/CD pipeline. No test framework configured.

## Environment Setup

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. QMT client must be installed at D:\QMTSIM and running
#    xtquant is loaded from D:\QMTSIM\bin.x64\Lib\site-packages
#    (injected into sys.path by app/qmt_engine.py)

# 3. Set your QMT account in config/settings.yaml
#    qmt.account_id is mandatory for trade/test/live_replay commands
```

## Common Commands

```bash
# Install
pip install -r requirements.txt

# Mock signal API (development)
python main.py api                          # FastAPI on 0.0.0.0:8000 with reload

# Manual one-shot test against QMT
python main.py test                         # all timings
python main.py test --timing open_auction   # auction only
python main.py test --timing close          # close only
python main.py test --timing vwap           # vwap only

# Scheduled trading (production)
python main.py trade                        # APScheduler, Ctrl+C to stop

# Backtest
python main.py generate-sample              # create sample CSV
python main.py backtest -f data/history_signals.csv -m paper_calc
python main.py backtest -f data/history_signals.csv -m live_replay
python main.py backtest -f data/history_signals.csv -m paper_calc -s 2025-01-01 -e 2025-01-31
python main.py backtest -f data/history_signals.csv -m paper_calc -o data/report.csv
```

No formal test/lint/format commands exist yet. Validate changes via the executable flows above.

## Architecture

### Execution Pipeline

```
Signal Source (mock API / remote HTTP)
    → app/signal_client.py  (fetch + validate into SignalBatch)
    → app/models.py         (TradeSignal, OrderRecord, SignalBatch)
    ↓
    ├── Live:  app/scheduler.py → app/qmt_engine.py → xtquant → QMT client
    └── BT:    app/backtest.py  → [paper_calc | QmtEngine.place_order()]
```

### Key Architectural Boundaries

**`app/qmt_engine.py`** — the only file that imports `xtquant`. All broker-specific logic (price type mapping, order placement, connection management) lives here. Never spread xtquant imports into scheduler or backtest code.

**`app/scheduler.py`** — owns intraday timing via APScheduler cron jobs. Keeps in-memory state for today's signals and order records. Does not touch HTTP or xtquant directly; delegates to `fetch_signals()` and `QmtEngine.place_order()`.

**`app/signal_client.py`** — decides mock vs remote based on `config/settings.yaml` → `signal_source.mode`. Falls back to hardcoded signals if mock API is unreachable.

**`app/models.py`** — canonical Pydantic models. Changes to `TradeSignal` fields propagate to API, client, scheduler, engine, and backtest.

**`app/config.py`** — lazy-loads `config/settings.yaml` into a process-global `dict`. All modules call `get_config()` instead of receiving config as a parameter.

### Schedule Timeline

```
09:10  fetch daily signals
09:25  open_auction execution
09:30-11:30  VWAP slices (every N min, configurable)
13:00-14:55  VWAP slices continue
14:55  close sell execution
15:05  end-of-day summary + state reset
```

### Backtest Modes

- `paper_calc`: offline record generation, no QMT connection needed
- `live_replay`: sends historical signals through `QmtEngine.place_order()` — operational validation, not strategy research

### Signal CSV Format

Required columns: `signal_date, stock_code, direction, volume`. Optional: `price, timing, reason`.

## Key Conventions

- **Config is a global dict** — accessed via `get_config()`, not dependency-injected
- **Enums for bounded choices** — `SignalDirection` (buy/sell), `TimingType` (open_auction/close/vwap)
- **Pydantic models everywhere** — signals, batches, order records, settlements all validated at parse time
- **Stock codes** use exchange suffix: `600519.SH`, `000858.SZ`
- **Volumes** are in lots of 100 shares (A-share convention)
- **Price = 0.0** means market order; price > 0 means limit order (resolved in `_resolve_price_type`)

## Operational Assumptions

- QMT client must be running before any `trade`, `test`, or `live_replay` command
- `qmt.account_id` in `config/settings.yaml` is mandatory for QMT-backed commands
- Mock signal API: `http://127.0.0.1:8000/api/signals`
- Remote signal API: configured in `signal_source.remote_url` (default: `http://192.168.8.234:8000/api/signals`)
- Scheduler runs Monday-Friday only (`day_of_week="mon-fri"`)

## Collaboration Principles

从原始需求和问题本质出发，不从惯例和模板出发：
- 不假设用户清楚自己想要什么 — 动机或目标不清晰时，停下来讨论
- 目标清晰但路径不是最短的 — 直接告诉用户并建议更好的办法
- 遇到问题追根本原因，不打补丁
- 每个决策都要能回答"为什么"
- 输出说重点，砍掉一切不改变决策的信息
