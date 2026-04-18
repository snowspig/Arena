"""AI Signal Arena —— 市场 briefing 构建。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

from .arena_candidate_pool import build_candidate_pool

DEFAULT_MAX_BUY_COUNT = 10
HOT_SECTOR_LIMIT = 5
RISING_THRESHOLD = 0.6
FALLING_THRESHOLD = 0.4


def build_market_brief(
    target_date: date | None = None,
    pool_size: int = 250,
    max_buy_count: int = DEFAULT_MAX_BUY_COUNT,
    current_positions: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """构造给 AI 职业交易员使用的市场 briefing。

    Args:
        target_date: 目标交易日，默认取候选池最新日期。
        pool_size: 候选池规模。
        max_buy_count: AI 最多可选股票数。
        current_positions: 当前持仓快照。

    Returns:
        包含市场摘要、候选池、当前持仓和约束的结构化字典。
    """
    candidate_pool = build_candidate_pool(target_date=target_date, pool_size=pool_size)
    brief_date = target_date.isoformat() if target_date else date.today().isoformat()
    if candidate_pool:
        brief_date = candidate_pool[0].get("trade_date", brief_date)

    return {
        "trade_date": brief_date,
        "market_summary": _build_market_summary(candidate_pool),
        "candidate_pool": candidate_pool,
        "current_positions": list(current_positions or []),
        "constraints": {
            "max_buy_count": max_buy_count,
            "equal_weight": True,
            "must_use_given_candidates": True,
        },
    }


def _build_market_summary(candidate_pool: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not candidate_pool:
        return {
            "index_tone": "候选池为空，市场信号不足",
            "hot_sectors": [],
            "capital_flow_summary": "无候选池数据，无法判断资金偏好",
            "risk_notes": ["今日无可用候选，禁止生成新增买入结论"],
        }

    avg_pct_change = _average_metric(candidate_pool, "pct_change")
    avg_amount = _average_metric(candidate_pool, "amount")
    risk_notes = _build_risk_notes(candidate_pool, avg_pct_change)

    return {
        "index_tone": _describe_index_tone(candidate_pool, avg_pct_change),
        "hot_sectors": _infer_hot_sectors(candidate_pool),
        "capital_flow_summary": (
            f"候选池平均成交额约 {avg_amount / 1e8:.2f} 亿元，"
            f"说明资金主要集中在高流动性标的。"
        ),
        "risk_notes": risk_notes,
    }


def _describe_index_tone(
    candidate_pool: Sequence[dict[str, Any]],
    avg_pct_change: float,
) -> str:
    positive_ratio = _positive_ratio(candidate_pool)
    if positive_ratio >= RISING_THRESHOLD:
        return f"候选池偏强，平均涨幅 {avg_pct_change:.2f}%，短线情绪积极。"
    if positive_ratio <= FALLING_THRESHOLD:
        return f"候选池分化偏弱，平均涨幅 {avg_pct_change:.2f}%，需优先控制回撤。"
    return f"候选池强弱均衡，平均涨幅 {avg_pct_change:.2f}%，适合精选龙头。"


def _infer_hot_sectors(candidate_pool: Sequence[dict[str, Any]]) -> list[str]:
    sector_scores: dict[str, float] = {}
    for candidate in candidate_pool:
        sector = str(candidate.get("sector") or candidate.get("industry") or "未分类")
        sector_scores[sector] = sector_scores.get(sector, 0.0) + float(candidate.get("composite_score", 0.0))

    ranked = sorted(sector_scores.items(), key=lambda item: item[1], reverse=True)
    return [name for name, _ in ranked[:HOT_SECTOR_LIMIT] if name != "未分类"]


def _build_risk_notes(
    candidate_pool: Sequence[dict[str, Any]],
    avg_pct_change: float,
) -> list[str]:
    high_momentum_count = sum(
        1 for candidate in candidate_pool if float(candidate.get("pct_change", 0.0)) >= 7.0
    )
    notes = ["仅允许从候选池内选股，禁止编造代码或扩展股票范围"]
    if high_momentum_count >= 20:
        notes.append("高涨幅个股较多，注意次日高开回落风险")
    if avg_pct_change < 0:
        notes.append("候选池平均涨幅为负，买入时应优先考虑防守型标的")
    return notes


def _average_metric(candidate_pool: Sequence[dict[str, Any]], key: str) -> float:
    total = sum(float(candidate.get(key, 0.0)) for candidate in candidate_pool)
    return total / len(candidate_pool)


def _positive_ratio(candidate_pool: Sequence[dict[str, Any]]) -> float:
    positive_count = sum(1 for candidate in candidate_pool if float(candidate.get("pct_change", 0.0)) > 0)
    return positive_count / len(candidate_pool)
