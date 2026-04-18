"""
回测模块 —— 读取历史交易信号，通过 QMT 模拟环境逐日回放下单。

支持两种回测方式：
  1. live_replay: 连接 QMT 模拟客户端，真实下单到模拟盘
  2. paper_calc:  纯计算模式，不连接 QMT，用历史价格计算盈亏

历史信号文件格式 (CSV):
  signal_date, stock_code, direction, volume, price, timing, reason
  2025-01-02, 600519.SH, buy, 100, 0, open_auction, alpha_signal
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger

from .config import get_config
from .models import (
    DailySettlement,
    OrderRecord,
    SignalBatch,
    SignalDirection,
    TimingType,
    TradeSignal,
)
from .qmt_engine import QmtEngine


def load_history_signals(file_path: str | Path) -> pd.DataFrame:
    """加载历史信号CSV文件"""
    df = pd.read_csv(file_path, parse_dates=["signal_date"])
    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.date
    required_cols = {"signal_date", "stock_code", "direction", "volume"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV 缺少必需列: {missing}")

    df.setdefault("price", 0.0)
    df.setdefault("timing", "open_auction")
    df.setdefault("reason", "")
    return df


def signals_for_date(df: pd.DataFrame, target_date: date) -> list[TradeSignal]:
    """从 DataFrame 中提取指定日期的信号"""
    day_df = df[df["signal_date"] == target_date]
    signals = []
    for _, row in day_df.iterrows():
        signals.append(
            TradeSignal(
                signal_id=uuid.uuid4().hex[:12],
                stock_code=row["stock_code"].strip(),
                direction=SignalDirection(row["direction"].strip().lower()),
                volume=int(row["volume"]),
                price=float(row.get("price", 0)),
                timing=TimingType(row.get("timing", "open_auction").strip().lower()),
                signal_date=target_date,
                reason=str(row.get("reason", "")),
            )
        )
    return signals


def generate_sample_history(output_path: str | Path):
    """生成示例历史信号文件，用于测试"""
    data = {
        "signal_date": [
            "2025-01-02", "2025-01-02",
            "2025-01-03", "2025-01-03",
            "2025-01-06", "2025-01-06",
            "2025-01-07",
        ],
        "stock_code": [
            "600519.SH", "000858.SZ",
            "600519.SH", "000858.SZ",
            "601318.SH", "600036.SH",
            "601318.SH",
        ],
        "direction": [
            "buy", "buy",
            "sell", "sell",
            "buy", "buy",
            "sell",
        ],
        "volume": [100, 200, 100, 200, 300, 100, 300],
        "price": [0, 0, 0, 0, 0, 0, 0],
        "timing": [
            "open_auction", "open_auction",
            "close", "close",
            "open_auction", "open_auction",
            "close",
        ],
        "reason": [
            "alpha_001", "alpha_001",
            "alpha_001_exit", "alpha_001_exit",
            "alpha_002", "alpha_002",
            "alpha_002_exit",
        ],
    }
    df = pd.DataFrame(data)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"示例历史信号已生成: {path}")
    return path


class Backtester:
    """
    回测引擎：逐日回放历史信号，通过 QMT 模拟盘下单。

    使用流程:
        bt = Backtester(engine)
        bt.load_signals("data/history_signals.csv")
        bt.run("2025-01-02", "2025-01-10")
        bt.report()
    """

    def __init__(self, engine: Optional[QmtEngine] = None):
        self.engine = engine
        self._signals_df: Optional[pd.DataFrame] = None
        self._records: list[OrderRecord] = []
        self._settlements: list[DailySettlement] = []

    def load_signals(self, file_path: str | Path):
        """加载历史信号文件"""
        self._signals_df = load_history_signals(file_path)
        dates = sorted(self._signals_df["signal_date"].unique())
        logger.info(
            f"加载历史信号: {len(self._signals_df)} 条, "
            f"覆盖 {len(dates)} 个交易日 ({dates[0]} ~ {dates[-1]})"
        )

    def run(
        self,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        mode: str = "live_replay",
    ):
        """
        执行回测

        Args:
            start_date: 开始日期
            end_date: 结束日期
            mode: live_replay(连接QMT模拟盘) 或 paper_calc(纯计算)
        """
        if self._signals_df is None:
            raise RuntimeError("请先调用 load_signals() 加载信号")

        cfg = get_config()
        if start_date is None:
            start_date = cfg["backtest"]["start_date"]
        if end_date is None:
            end_date = cfg["backtest"]["end_date"]

        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        all_dates = sorted(self._signals_df["signal_date"].unique())
        target_dates = [d for d in all_dates if start_date <= d <= end_date]

        logger.info(
            f"=== 回测开始 ({mode}) ===\n"
            f"  日期范围: {start_date} ~ {end_date}\n"
            f"  有信号交易日: {len(target_dates)} 天"
        )

        if mode == "live_replay":
            self._run_live_replay(target_dates)
        else:
            self._run_paper_calc(target_dates)

        logger.info("=== 回测完成 ===")

    def _run_live_replay(self, dates: list[date]):
        """连接 QMT 模拟盘逐日下单"""
        if self.engine is None or not self.engine.connected:
            raise RuntimeError("live_replay 模式需要已连接的 QmtEngine")

        for trade_date in dates:
            signals = signals_for_date(self._signals_df, trade_date)
            if not signals:
                continue

            logger.info(f"\n--- 回测日: {trade_date} ({len(signals)} 条信号) ---")

            for signal in signals:
                record = self.engine.place_order(signal)
                self._records.append(record)
                logger.info(
                    f"  [{signal.timing.value}] {signal.stock_code} "
                    f"{signal.direction.value} {signal.volume}股 "
                    f"-> order_id={record.order_id} {record.status}"
                )

    def _run_paper_calc(self, dates: list[date]):
        """纯计算模式（不连接QMT）"""
        logger.info("Paper calc 模式: 记录信号但不实际下单")
        for trade_date in dates:
            signals = signals_for_date(self._signals_df, trade_date)
            for signal in signals:
                record = OrderRecord(
                    signal_id=signal.signal_id,
                    stock_code=signal.stock_code,
                    direction=signal.direction,
                    order_volume=signal.volume,
                    status="paper",
                )
                self._records.append(record)
                self._settlements.append(
                    DailySettlement(
                        trade_date=trade_date,
                        stock_code=signal.stock_code,
                        direction=signal.direction,
                        volume=signal.volume,
                        price=signal.price,
                        amount=signal.price * signal.volume,
                    )
                )

    def report(self) -> pd.DataFrame:
        """生成回测报告"""
        if not self._records:
            logger.warning("无回测记录")
            return pd.DataFrame()

        data = []
        for r in self._records:
            data.append({
                "signal_id": r.signal_id,
                "stock_code": r.stock_code,
                "direction": r.direction.value,
                "volume": r.order_volume,
                "order_id": r.order_id,
                "status": r.status,
                "error_msg": r.error_msg,
            })

        df = pd.DataFrame(data)
        logger.info(f"\n=== 回测报告 ===\n{df.to_string(index=False)}")

        summary = {
            "总信号数": len(self._records),
            "买入信号": sum(1 for r in self._records if r.direction == SignalDirection.BUY),
            "卖出信号": sum(1 for r in self._records if r.direction == SignalDirection.SELL),
            "成功提交": sum(1 for r in self._records if r.status in ("submitted", "paper")),
            "失败": sum(1 for r in self._records if r.status == "error"),
        }
        logger.info(f"汇总: {summary}")
        return df

    def export_records(self, output_path: str | Path):
        """导出回测记录到 CSV"""
        df = self.report()
        if not df.empty:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path, index=False)
            logger.info(f"回测记录已导出: {path}")
