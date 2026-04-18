"""
交易信号 API 服务 —— 提供模拟信号用于开发测试，
同时也作为远程信号服务器的参考实现。

启动方式: uvicorn app.signal_api:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import random
import uuid
from datetime import date, datetime
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles

from .arena_dashboard import router as arena_dashboard_router
from .dashboard import router as dashboard_router
from .models import SignalBatch, SignalDirection, TimingType, TradeSignal

app = FastAPI(title="QLiBRD Signal API", version="0.1.0")
app.include_router(dashboard_router)
app.include_router(arena_dashboard_router)

static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

MOCK_STOCKS_BUY = [
    "600519.SH", "000858.SZ", "601318.SH",
    "000001.SZ", "600036.SH", "002594.SZ",
]

MOCK_STOCKS_SELL = [
    "600519.SH", "000858.SZ",
]


def _generate_mock_signals(
    signal_date: date,
    buy_timing: TimingType = TimingType.OPEN_AUCTION,
    sell_timing: TimingType = TimingType.CLOSE,
) -> list[TradeSignal]:
    """生成模拟交易信号（随机选股、随机数量）"""
    signals: list[TradeSignal] = []

    buy_count = random.randint(1, 3)
    buy_stocks = random.sample(MOCK_STOCKS_BUY, min(buy_count, len(MOCK_STOCKS_BUY)))
    for code in buy_stocks:
        signals.append(
            TradeSignal(
                signal_id=uuid.uuid4().hex[:12],
                stock_code=code,
                direction=SignalDirection.BUY,
                volume=random.choice([100, 200, 300, 500]),
                price=0.0,
                timing=buy_timing,
                signal_date=signal_date,
                reason="mock_buy_signal",
            )
        )

    sell_count = random.randint(0, 2)
    sell_stocks = random.sample(MOCK_STOCKS_SELL, min(sell_count, len(MOCK_STOCKS_SELL)))
    for code in sell_stocks:
        signals.append(
            TradeSignal(
                signal_id=uuid.uuid4().hex[:12],
                stock_code=code,
                direction=SignalDirection.SELL,
                volume=random.choice([100, 200]),
                price=0.0,
                timing=sell_timing,
                signal_date=signal_date,
                reason="mock_sell_signal",
            )
        )

    return signals


@app.get("/api/signals", response_model=SignalBatch)
def get_signals(
    signal_date: date = Query(default_factory=date.today, description="信号日期"),
    buy_timing: TimingType = Query(TimingType.OPEN_AUCTION),
    sell_timing: TimingType = Query(TimingType.CLOSE),
):
    """获取指定日期的交易信号（模拟）"""
    signals = _generate_mock_signals(signal_date, buy_timing, sell_timing)
    return SignalBatch(
        batch_id=uuid.uuid4().hex[:16],
        signals=signals,
        generated_at=datetime.now(),
    )


@app.post("/api/signals", response_model=SignalBatch)
def post_signals(batch: SignalBatch):
    """接收外部推送的真实交易信号（直通）"""
    return batch
