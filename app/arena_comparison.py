"""AI Signal Arena —— Agent 对比分析。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Any

from loguru import logger
from pymongo import MongoClient


def compare_providers(
    target_date: date | None = None,
) -> dict[str, Any]:
    """对比所有 agent 在指定日期的选股结果。

    Args:
        target_date: 交易日期，默认今天。

    Returns:
        包含 overlap, divergence, per_provider_stats 的对比报告。
    """
    date_str = (target_date or date.today()).isoformat()
    docs = _load_arena_signals(date_str)

    if len(docs) < 2:
        return {
            "trade_date": date_str,
            "providers": list(docs.keys()),
            "error": "需要至少 2 个 provider 才能对比",
        }

    provider_names = sorted(docs.keys())
    report = {
        "trade_date": date_str,
        "providers": provider_names,
        "per_provider": {},
    }

    for pname in provider_names:
        report["per_provider"][pname] = _provider_stats(docs[pname])

    report["overlap"] = _compute_overlap(docs, provider_names)
    report["divergence"] = _compute_divergence(docs, provider_names)

    return report


def format_comparison_report(report: dict[str, Any]) -> str:
    """把对比报告格式化为可读文本。"""
    lines = [
        f"## Arena 对比报告 — {report['trade_date']}",
        "",
    ]

    per_provider = report.get("per_provider", {})
    for pname, stats in per_provider.items():
        lines.append(f"### {pname}")
        lines.append(f"- 选股数量: {stats['pick_count']}")
        lines.append(f"- 平均 confidence: {stats['avg_confidence']:.3f}")
        lines.append(f"- 最高 confidence: {stats['max_confidence']:.3f}")
        lines.append(f"- Top 5: {', '.join(stats['top5'])}")
        lines.append("")

    overlap = report.get("overlap", {})
    if overlap:
        lines.append("### 重合分析")
        lines.append(f"- 所有 agent 都选的股票: {len(overlap.get('common_all', []))} 只")
        common_all = overlap.get("common_all", [])
        if common_all:
            for stock in common_all[:10]:
                lines.append(f"  - {stock}")
        lines.append("")
        lines.append(f"- 仅被单一 agent 选择的: {len(overlap.get('unique_picks', []))} 只")
        unique = overlap.get("unique_picks", [])
        if unique:
            for item in unique[:10]:
                lines.append(f"  - [{item['provider']}] {item['stock_code']}")
        lines.append("")

    divergence = report.get("divergence", {})
    if divergence:
        lines.append("### 分歧最大的股票")
        divergent = divergence.get("top_divergent", [])
        if divergent:
            for item in divergent[:10]:
                lines.append(
                    f"  - {item['stock_code']}: "
                    + ", ".join(
                        f"{p}={d['confidence']:.2f}"
                        for p, d in item["providers"].items()
                    )
                )
        lines.append("")

    return "\n".join(lines)


# ---- 内部函数 ----


def _load_arena_signals(date_str: str) -> dict[str, dict[str, Any]]:
    """从 MongoDB 加载指定日期所有 provider 的信号。"""
    from .config import get_config, get_mongo_db, get_mongo_uri

    cfg = get_config()
    uri = get_mongo_uri()
    db_name = get_mongo_db()

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    col = client[db_name]["arena_signals"]
    cursor = col.find({"trade_date": date_str})

    docs = {}
    for doc in cursor:
        docs[doc["provider"]] = doc

    return docs


def _provider_stats(doc: dict[str, Any]) -> dict[str, Any]:
    """单个 provider 的统计信息。"""
    raw_picks = doc.get("raw_picks", [])
    if isinstance(raw_picks, dict):
        picks = raw_picks.get("picks", [])
    elif isinstance(raw_picks, list):
        picks = raw_picks
    else:
        picks = []

    if not picks:
        return {
            "pick_count": 0,
            "avg_confidence": 0.0,
            "max_confidence": 0.0,
            "top5": [],
        }

    confidences = [float(p.get("confidence", 0)) for p in picks]
    sorted_picks = sorted(picks, key=lambda p: float(p.get("confidence", 0)), reverse=True)
    top5 = [
        f"{p.get('stock_code', '?')}({float(p.get('confidence', 0)):.2f})"
        for p in sorted_picks[:5]
    ]

    return {
        "pick_count": len(picks),
        "avg_confidence": sum(confidences) / len(confidences),
        "max_confidence": max(confidences),
        "top5": top5,
    }


def _compute_overlap(
    docs: dict[str, dict[str, Any]],
    provider_names: list[str],
) -> dict[str, Any]:
    """计算多个 provider 之间的选股重合。"""
    provider_picks: dict[str, set[str]] = {}
    for pname in provider_names:
        raw_picks = docs[pname].get("raw_picks", [])
        if isinstance(raw_picks, dict):
            picks = raw_picks.get("picks", [])
        elif isinstance(raw_picks, list):
            picks = raw_picks
        else:
            picks = []
        codes = {p.get("stock_code", "") for p in picks if p.get("stock_code")}
        provider_picks[pname] = codes

    # 所有 provider 都选的
    common_all = set.intersection(*provider_picks.values()) if provider_picks else set()

    # 至少两个 provider 选的
    code_counter = Counter()
    for codes in provider_picks.values():
        code_counter.update(codes)

    common_multi = {code for code, count in code_counter.items() if count >= 2}

    # 仅被单一 provider 选择的
    unique_items = []
    for pname, codes in provider_picks.items():
        for code in codes:
            if code_counter[code] == 1:
                unique_items.append({"provider": pname, "stock_code": code})

    return {
        "common_all": sorted(common_all),
        "common_multi_count": len(common_multi),
        "unique_picks": sorted(unique_items, key=lambda x: x["stock_code"]),
    }


def _compute_divergence(
    docs: dict[str, dict[str, Any]],
    provider_names: list[str],
) -> dict[str, Any]:
    """找出不同 provider confidence 差异最大的股票。"""
    stock_providers: dict[str, dict[str, dict[str, Any]]] = {}
    for pname in provider_names:
        raw_picks = docs[pname].get("raw_picks", [])
        if isinstance(raw_picks, dict):
            picks = raw_picks.get("picks", [])
        elif isinstance(raw_picks, list):
            picks = raw_picks
        else:
            picks = []
        for p in picks:
            code = p.get("stock_code", "")
            if not code:
                continue
            stock_providers.setdefault(code, {})[pname] = {
                "confidence": float(p.get("confidence", 0)),
                "reason": p.get("reason", ""),
            }

    divergent = []
    for code, providers in stock_providers.items():
        if len(providers) < 2:
            continue
        confs = [d["confidence"] for d in providers.values()]
        spread = max(confs) - min(confs)
        divergent.append({
            "stock_code": code,
            "spread": round(spread, 3),
            "providers": providers,
        })

    divergent.sort(key=lambda x: x["spread"], reverse=True)
    return {"top_divergent": divergent}
