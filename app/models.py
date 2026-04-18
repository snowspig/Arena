"""交易信号和订单数据模型"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SignalDirection(str, Enum):
    BUY = "buy"
    SELL = "sell"


class TimingType(str, Enum):
    OPEN_AUCTION = "open_auction"   # 集合竞价（9:25）
    CLOSE = "close"                 # 收盘前N分钟
    VWAP = "vwap"                   # VWAP拆单


class TradeSignal(BaseModel):
    """外部策略推送的交易信号"""
    signal_id: str
    stock_code: str = Field(..., description="证券代码, 如 600000.SH")
    direction: SignalDirection
    volume: int = Field(..., gt=0, description="委托股数，100的整数倍")
    price: float = Field(0.0, ge=0, description="参考价格，0表示市价")
    estimated_order_price: float = Field(0.0, ge=0, description="预计委托价格，用于预算和执行")
    timing: TimingType = TimingType.OPEN_AUCTION
    signal_date: date = Field(default_factory=date.today)
    created_at: datetime = Field(default_factory=datetime.now)
    reason: str = ""


class SignalBatch(BaseModel):
    """一批交易信号"""
    batch_id: str
    signals: list[TradeSignal]
    generated_at: datetime = Field(default_factory=datetime.now)


class OrderRecord(BaseModel):
    """本地订单记录"""
    signal_id: str
    stock_code: str
    direction: SignalDirection
    order_id: int = 0
    order_volume: int = 0
    traded_volume: int = 0
    traded_price: float = 0.0
    status: str = "pending"
    created_at: datetime = Field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
    error_msg: str = ""


class DailySettlement(BaseModel):
    """每日交割记录（回测用）"""
    trade_date: date
    stock_code: str
    direction: SignalDirection
    volume: int
    price: float
    amount: float = 0.0
    commission: float = 0.0
    pnl: float = 0.0


class AssetSnapshot(BaseModel):
    """账户资产快照"""
    total_asset: float = 0.0
    cash: float = 0.0
    market_value: float = 0.0
    frozen_cash: float = 0.0


class PositionSnapshot(BaseModel):
    """持仓快照"""
    stock_code: str
    volume: int = 0
    can_use_volume: int = 0
    cost_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0


class OrderSnapshot(BaseModel):
    """委托快照"""
    order_id: int
    stock_code: str
    direction: str
    order_volume: int
    traded_volume: int = 0
    traded_price: float = 0.0
    price_type: int = 0
    order_status: str = ""


class TradeSnapshot(BaseModel):
    """成交快照"""
    trade_id: int
    stock_code: str
    direction: str
    traded_price: float = 0.0
    traded_volume: int = 0


class DailySnapshot(BaseModel):
    """每日结算快照（存入 MongoDB + 导出文件）"""
    date: str
    account_type: str
    account_id: str
    asset: AssetSnapshot
    positions: list[PositionSnapshot] = []
    orders: list[OrderSnapshot] = []
    trades: list[TradeSnapshot] = []
    daily_pnl: float = 0.0
    daily_return_pct: float = 0.0
    created_at: datetime = Field(default_factory=datetime.now)
