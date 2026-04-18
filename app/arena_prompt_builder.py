"""AI Signal Arena —— 职业交易员 prompt 构建。"""

from __future__ import annotations

import json
from typing import Any

_SYSTEM_PROMPT = """\
你是一位中国 A 股职业短线交易员。

你将收到一份市场 briefing，包含市场摘要、候选股票池和当前持仓。
你需要从中挑选最适合次日开盘买入的股票。

硬性规则：
1. 只能从候选池中选股，不能编造代码或选择池外股票
2. 最多选 {max_buy_count} 只
3. 统一使用 markdown 列表输出，不要输出 JSON
4. 每只股票必须包含 stock_code（与候选池格式一致）、confidence（0~1）、reason（一句话理由，尽量控制在 20 个字以内）
5. 不要输出长篇分析，不要重复 market briefing，不要输出任何额外说明

输出格式（每行一只）：
- stock_code: 600519.SH | confidence: 0.85 | reason: 资金流入明显，趋势延续
- stock_code: 000858.SZ | confidence: 0.72 | reason: 板块共振，量价配合"""


def build_pro_trader_prompt(
    brief: dict[str, Any],
    provider_name: str | None = None,
) -> tuple[str, str]:
    """构建职业交易员 prompt。

    Args:
        brief: build_market_brief() 的输出。
        provider_name: agent 名称，用于注入历史经验记忆。

    Returns:
        (system_prompt, user_prompt) 元组。
    """
    constraints = brief.get("constraints", {})
    max_buy_count = constraints.get("max_buy_count", 10)

    system_prompt = _SYSTEM_PROMPT.format(max_buy_count=max_buy_count)
    user_prompt = _build_user_prompt(brief)

    if provider_name:
        from .arena_memory import load_agent_memory
        memory = load_agent_memory(provider_name)
        if memory:
            user_prompt += f"\n\n## 你的历史经验\n{memory}"

    return system_prompt, user_prompt


def _build_user_prompt(brief: dict[str, Any]) -> str:
    parts = [
        f"## 交易日期\n{brief.get('trade_date', '未知')}",
        _format_market_summary(brief.get("market_summary", {})),
        _format_candidate_pool(brief.get("candidate_pool", [])),
        _format_positions(brief.get("current_positions", [])),
        _format_constraints(brief.get("constraints", {})),
    ]
    return "\n\n".join(parts)


def _format_market_summary(summary: dict[str, Any]) -> str:
    lines = ["## 市场摘要"]
    lines.append(f"- 大盘基调: {summary.get('index_tone', '未知')}")
    lines.append(f"- 资金偏好: {summary.get('capital_flow_summary', '未知')}")

    hot_sectors = summary.get("hot_sectors", [])
    if hot_sectors:
        lines.append(f"- 热门方向: {', '.join(hot_sectors)}")

    risk_notes = summary.get("risk_notes", [])
    if risk_notes:
        lines.append("- 风险提示:")
        for note in risk_notes:
            lines.append(f"  - {note}")

    return "\n".join(lines)


def _format_candidate_pool(pool: list[dict[str, Any]]) -> str:
    if not pool:
        return "## 候选池\n（空）"

    header = "stock_code,name,close,pct_change,amount(亿),composite_score"
    rows = []
    for stock in pool:
        amount_yi = stock.get("amount", 0) / 1e8
        rows.append(
            f"{stock.get('stock_code', '')},"
            f"{stock.get('name', '')},"
            f"{stock.get('close', 0):.2f},"
            f"{stock.get('pct_change', 0):.2f},"
            f"{amount_yi:.2f},"
            f"{stock.get('composite_score', 0):.3f}"
        )

    return f"## 候选股票池（{len(pool)} 只）\n{header}\n" + "\n".join(rows)


def _format_positions(positions: list[dict[str, Any]]) -> str:
    if not positions:
        return "## 当前持仓\n（空仓）"

    header = "stock_code,volume,cost_price,market_value,unrealized_pnl"
    rows = []
    for pos in positions:
        rows.append(
            f"{pos.get('stock_code', '')},"
            f"{pos.get('volume', 0)},"
            f"{pos.get('cost_price', 0):.2f},"
            f"{pos.get('market_value', 0):.2f},"
            f"{pos.get('unrealized_pnl', 0):.2f}"
        )

    return f"## 当前持仓（{len(positions)} 只）\n{header}\n" + "\n".join(rows)


def _format_constraints(constraints: dict[str, Any]) -> str:
    max_count = constraints.get("max_buy_count", 10)
    equal_weight = constraints.get("equal_weight", True)
    return (
        f"## 约束\n"
        f"- 最多选 {max_count} 只\n"
        f"- 等权分仓: {'是' if equal_weight else '否'}\n"
        f"- 必须从候选池中选股"
    )
