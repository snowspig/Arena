"""Arena agent pick reviewer and memory updater."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any

from loguru import logger
from pymongo import DESCENDING, MongoClient

from .arena_memory import update_agent_facts, write_agent_lesson
from .arena_portfolio import get_enabled_providers
from .config import get_config, get_mongo_db, get_mongo_uri

_HIT_RETURN_THRESHOLD = 0.0
_HIGH_CONFIDENCE = 0.8
_MID_CONFIDENCE = 0.5


def review_all_agents(trade_date: date) -> list[dict[str, Any]]:
    """Review all enabled arena providers for one date."""
    return [review_single_agent(provider, trade_date) for provider in get_enabled_providers()]


def review_single_agent(provider: str, review_date: date) -> dict[str, Any]:
    """Review one provider by comparing T-1 picks with T market returns."""
    signals_doc = _load_signal_document(provider, _get_pick_date(review_date))
    if not signals_doc:
        return _empty_review(provider, review_date)
    results = _build_pick_results(signals_doc, review_date)
    review = _build_review(provider, review_date, signals_doc, results)
    _save_review(review)
    _persist_memory(review)
    return review


def _get_market_returns(pick_date: date, review_date: date) -> dict[str, dict[str, float]]:
    """Load next-day stock returns keyed by xt stock code."""
    del pick_date
    features_col = _create_client()[get_mongo_db("qlibrd_db")]["cn_data_stock_features"]
    cursor = features_col.find(
        {"date": {"$in": _date_filters(review_date)}},
        {"_id": 0, "symbol": 1, "stock_code": 1, "close": 1, "preClose": 1},
    )
    market_returns: dict[str, dict[str, float]] = {}
    for item in cursor:
        stock_code = _to_xt_code(item)
        if not stock_code:
            continue
        close = _to_float(item.get("close"))
        pre_close = _to_float(item.get("preClose"))
        pct_change = ((close - pre_close) / pre_close * 100) if pre_close > 0 else 0.0
        market_returns[stock_code] = {
            "pct_change": pct_change,
            "close": close,
        }
    return market_returns


def _get_market_avg_return(trade_date: date) -> float:
    """Compute average market pct_change for one date."""
    features_col = _create_client()[get_mongo_db("qlibrd_db")]["cn_data_stock_features"]
    cursor = features_col.find(
        {"date": {"$in": _date_filters(trade_date)}},
        {"_id": 0, "close": 1, "preClose": 1},
    )
    returns = []
    for item in cursor:
        close = _to_float(item.get("close"))
        pre_close = _to_float(item.get("preClose"))
        if pre_close > 0:
            returns.append((close - pre_close) / pre_close * 100)
    return mean(returns) if returns else 0.0


def _compute_confidence_accuracy(results: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """Compute hit rate by confidence tier."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in results:
        grouped[_confidence_tier(_to_float(item.get("confidence")))].append(item)
    return {
        tier: {
            "count": len(items),
            "hit_rate": _safe_ratio(sum(1 for item in items if item["is_hit"]), len(items)),
        }
        for tier, items in grouped.items()
    }


def _extract_lessons(
    results: list[dict[str, Any]],
    conf_accuracy: dict[str, dict[str, float]],
    hit_rate: float,
    avg_market: float,
) -> list[str]:
    """Extract actionable lessons from review metrics."""
    avg_pick = mean(_to_float(item.get("pct_change")) for item in results) if results else 0.0
    lessons = [
        _hit_rate_lesson(hit_rate),
        _market_lesson(avg_pick, avg_market),
        _confidence_lesson(conf_accuracy),
        _extreme_pick_lesson(results, best=True),
        _extreme_pick_lesson(results, best=False),
    ]
    return [lesson for lesson in lessons if lesson]


def _update_cumulative_facts(provider: str, reviews: list[dict[str, Any]]) -> None:
    """Recompute cumulative valid review stats and write facts."""
    valid_reviews = [item for item in reviews if item.get("valid")]
    if not valid_reviews:
        return
    stats = {
        "有效复盘数": len(valid_reviews),
        "累计命中率": f"{mean(item.get('hit_rate', 0.0) for item in valid_reviews):.1%}",
        "累计平均超额": f"{mean(item.get('excess_return', 0.0) for item in valid_reviews):.2f}%",
        "最近复盘日": valid_reviews[0].get("review_date", ""),
    }
    update_agent_facts(provider, stats)


def _empty_review(provider: str, review_date: date) -> dict[str, Any]:
    """Build an empty review result payload."""
    return {
        "provider": provider,
        "review_date": review_date.isoformat(),
        "pick_date": _get_pick_date(review_date).isoformat(),
        "valid": False,
        "status": "missing_signals",
        "fallback_detected": False,
        "pick_count": 0,
        "hit_count": 0,
        "hit_rate": 0.0,
        "avg_pick_return": 0.0,
        "avg_market_return": 0.0,
        "excess_return": 0.0,
        "confidence_accuracy": {},
        "best_pick": None,
        "worst_pick": None,
        "lessons": [],
    }


def _build_pick_results(signals_doc: dict[str, Any], review_date: date) -> list[dict[str, Any]]:
    pick_date = date.fromisoformat(str(signals_doc["trade_date"]))
    market_returns = _get_market_returns(pick_date, review_date)
    picks = _normalize_picks(signals_doc.get("raw_picks", []))
    results = []
    for pick in picks:
        stock_code = str(pick.get("stock_code", ""))
        market = market_returns.get(stock_code)
        if not stock_code or market is None:
            continue
        pct_change = _to_float(market.get("pct_change"))
        results.append({
            "stock_code": stock_code,
            "confidence": _to_float(pick.get("confidence")),
            "reason": str(pick.get("reason", "")),
            "pct_change": pct_change,
            "close": _to_float(market.get("close")),
            "is_hit": pct_change > _HIT_RETURN_THRESHOLD,
        })
    return results


def _build_review(
    provider: str,
    review_date: date,
    signals_doc: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    if not results:
        review = _empty_review(provider, review_date)
        review.update(_review_meta(signals_doc))
        return review
    metrics = _review_metrics(results, review_date)
    review = {
        "provider": provider,
        "review_date": review_date.isoformat(),
        "pick_date": signals_doc.get("trade_date", ""),
        **_review_meta(signals_doc),
        **metrics,
        "best_pick": max(results, key=lambda item: item["pct_change"]),
        "worst_pick": min(results, key=lambda item: item["pct_change"]),
        "pick_details": results,  # 每只 pick 的 confidence + pct_change
        "reviewed_at": datetime.now().isoformat(),
    }
    review["lessons"] = _extract_lessons(
        results,
        review["confidence_accuracy"],
        review["hit_rate"],
        review["avg_market_return"],
    )
    return review


def _persist_memory(review: dict[str, Any]) -> None:
    if review.get("fallback_detected"):
        logger.warning(f"[{review['provider']}] 检测到 fallback，跳过 memory 写入")
        return
    if review.get("status") != "success":
        return
    write_agent_lesson(review["provider"], review["review_date"], review.get("lessons", []))
    _update_cumulative_facts(review["provider"], _load_valid_reviews(review["provider"]))


def _load_signal_document(provider: str, pick_date: date) -> dict[str, Any] | None:
    collection = _create_client()[get_mongo_db()]["arena_signals"]
    return collection.find_one({"trade_date": pick_date.isoformat(), "provider": provider})


def _save_review(review: dict[str, Any]) -> None:
    collection = _create_client()[get_mongo_db()]["arena_reviews"]
    collection.replace_one(
        {"provider": review["provider"], "review_date": review["review_date"]},
        review,
        upsert=True,
    )


def _load_valid_reviews(provider: str) -> list[dict[str, Any]]:
    collection = _create_client()[get_mongo_db()]["arena_reviews"]
    cursor = collection.find({"provider": provider, "valid": True}).sort("review_date", DESCENDING)
    return list(cursor)


def _review_metrics(results: list[dict[str, Any]], review_date: date) -> dict[str, Any]:
    avg_pick_return = mean(item["pct_change"] for item in results)
    avg_market_return = _get_market_avg_return(review_date)
    hit_count = sum(1 for item in results if item["is_hit"])
    hit_rate = _safe_ratio(hit_count, len(results))
    return {
        "valid": True,
        "pick_count": len(results),
        "hit_count": hit_count,
        "hit_rate": hit_rate,
        "avg_pick_return": avg_pick_return,
        "avg_market_return": avg_market_return,
        "excess_return": avg_pick_return - avg_market_return,
        "confidence_accuracy": _compute_confidence_accuracy(results),
    }


def _review_meta(signals_doc: dict[str, Any]) -> dict[str, Any]:
    fallback_detected = bool(signals_doc.get("fallback_detected", False))
    status = str(signals_doc.get("status", "unknown"))
    return {
        "status": status,
        "fallback_detected": fallback_detected,
        "valid": not fallback_detected and status == "success",
    }


def _normalize_picks(raw_picks: Any) -> list[dict[str, Any]]:
    if isinstance(raw_picks, dict):
        raw_picks = raw_picks.get("picks", [])
    return list(raw_picks) if isinstance(raw_picks, list) else []


def _market_payload(item: dict[str, Any]) -> dict[str, float]:
    return {
        "pct_change": _to_float(item.get("pct_change")),
        "close": _to_float(item.get("close")),
    }


def _date_filters(trade_date: date) -> list[Any]:
    return [trade_date.isoformat(), datetime.combine(trade_date, datetime.min.time())]


def _create_client() -> MongoClient:
    uri = get_mongo_uri()
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def _get_pick_date(review_date: date) -> date:
    pick_date = review_date - timedelta(days=1)
    while pick_date.weekday() >= 5:
        pick_date -= timedelta(days=1)
    return pick_date


def _to_xt_code(item: dict[str, Any]) -> str:
    """Convert qlibrd symbol (e.g. 300260_SZ) to xt stock_code (e.g. 300260.SZ)."""
    symbol = str(item.get("symbol", ""))
    if "_" in symbol:
        code, exchange = symbol.rsplit("_", 1)
        return f"{code}.{exchange}"
    stock_code = str(item.get("stock_code", ""))
    if stock_code.startswith(("SH", "SZ")):
        return stock_code[2:] + f".{stock_code[:2]}"
    return stock_code


def _confidence_tier(confidence: float) -> str:
    if confidence > _HIGH_CONFIDENCE:
        return "high"
    if confidence >= _MID_CONFIDENCE:
        return "mid"
    return "low"


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _hit_rate_lesson(hit_rate: float) -> str:
    if hit_rate >= 0.6:
        return f"命中率 {hit_rate:.0%} 较好，可以保留当前选股框架。"
    return f"命中率仅 {hit_rate:.0%}，需收紧选股条件或降低出手频率。"


def _market_lesson(avg_pick: float, avg_market: float) -> str:
    excess = avg_pick - avg_market
    if excess >= 0:
        return f"平均超额收益 {excess:.2f}%，说明 picks 相对大盘有优势。"
    return f"平均超额收益 {excess:.2f}%，跑输市场，需要复盘因子失效点。"


def _confidence_lesson(conf_accuracy: dict[str, dict[str, float]]) -> str:
    high = conf_accuracy.get("high")
    low = conf_accuracy.get("low")
    if high and low and high.get("hit_rate", 0.0) < low.get("hit_rate", 0.0):
        return "高置信度命中率低于低置信度，confidence 校准明显失真。"
    if high and high.get("count", 0) > 0:
        return f"高置信度分层命中率 {high.get('hit_rate', 0.0):.0%}，可继续观察校准稳定性。"
    return "高置信度样本不足，暂时无法评估 confidence 校准。"


def _extreme_pick_lesson(results: list[dict[str, Any]], best: bool) -> str:
    if not results:
        return ""
    key = max if best else min
    label = "最佳" if best else "最差"
    item = key(results, key=lambda payload: payload["pct_change"])
    return f"{label}个股 {item['stock_code']} 当日涨跌幅 {item['pct_change']:.2f}%。"
