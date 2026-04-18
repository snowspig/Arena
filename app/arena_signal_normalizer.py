"""AI Signal Arena —— 信号归一化。"""

from __future__ import annotations

from datetime import date
from typing import Any

from loguru import logger

from .models import SignalDirection, TimingType, TradeSignal

_LOT_SIZE = 100


def normalize_ai_picks(
    picks_result: dict[str, Any],
    candidate_pool: list[dict[str, Any]],
    total_capital: float,
    trade_date: date,
    max_buy_count: int = 10,
) -> list[TradeSignal]:
    """把 AI picks 转成可执行的 TradeSignal 列表。

    Args:
        picks_result: AI provider 返回的 {provider, trade_date, picks}。
        candidate_pool: 候选池（用于校验 stock_code）。
        total_capital: 该 agent 分配的总资金（元）。
        trade_date: 交易日期。
        max_buy_count: 最多保留的 pick 数。

    Returns:
        TradeSignal 列表。
    """
    provider = picks_result.get("provider", "unknown")
    raw_picks = picks_result.get("picks", [])

    valid_codes = {stock["stock_code"] for stock in candidate_pool}
    filtered = _validate_and_dedup(raw_picks, valid_codes, max_buy_count)

    if not filtered:
        logger.warning(f"[{provider}] 无有效 picks，跳过信号生成")
        return []

    price_map = {
        stock["stock_code"]: stock.get("close", 0.0)
        for stock in candidate_pool
    }

    per_stock_capital = total_capital / len(filtered)
    signals = []

    for pick in filtered:
        code = pick["stock_code"]
        close_price = price_map.get(code, 0.0)
        if close_price <= 0:
            logger.warning(f"[{provider}] {code} 价格无效 ({close_price})，跳过")
            continue

        estimated_order_price = _calc_estimated_order_price(code, close_price)
        volume = _calc_volume(per_stock_capital, estimated_order_price)
        if volume <= 0:
            continue

        signals.append(TradeSignal(
            signal_id=f"arena-{provider}-{trade_date.isoformat()}-{code}",
            stock_code=code,
            direction=SignalDirection.BUY,
            volume=volume,
            price=close_price,
            estimated_order_price=estimated_order_price,
            timing=TimingType.OPEN_AUCTION,
            signal_date=trade_date,
            reason=f"arena:{provider}:conf={pick.get('confidence', 0):.2f}",
        ))

    logger.info(
        f"[{provider}] 归一化: {len(raw_picks)} picks → "
        f"{len(filtered)} 有效 → {len(signals)} 条信号"
    )
    return signals


def _validate_and_dedup(
    picks: list[dict[str, Any]],
    valid_codes: set[str],
    max_count: int,
) -> list[dict[str, Any]]:
    """校验、去重、截断。"""
    seen: set[str] = set()
    result = []

    for pick in picks:
        code = pick.get("stock_code", "")
        if code not in valid_codes:
            logger.debug(f"剔除非候选池股票: {code}")
            continue
        if code in seen:
            continue
        seen.add(code)
        result.append(pick)

    result.sort(key=lambda p: float(p.get("confidence", 0)), reverse=True)
    return result[:max_count]


def _calc_estimated_order_price(stock_code: str, close_price: float) -> float:
    """根据涨跌停规则估算买入委托价格。"""
    from .arena_portfolio import get_limit_rate

    return round(close_price * (1 + get_limit_rate(stock_code)), 2)


def _calc_volume(capital: float, price: float) -> int:
    """计算等权买入股数，向下取整到 100 股。"""
    raw_lots = int(capital / price / _LOT_SIZE)
    return max(0, raw_lots * _LOT_SIZE)
