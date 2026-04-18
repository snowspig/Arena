from __future__ import annotations

from datetime import date
import json
import math
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pymongo import DESCENDING, MongoClient

from .arena_comparison import compare_providers

router = APIRouter(prefix="/api/arena", tags=["arena"])

_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates" / "arena"
_MEMORY_ROOT = Path(__file__).resolve().parent.parent / "memory" / "arena"
_DEFAULT_RISK_FREE_RATE = 0.0


def _get_db():
    from .config import get_mongo_db, get_mongo_uri

    uri = get_mongo_uri()
    db_name = get_mongo_db()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client[db_name]


def _parse_trade_date(trade_date: str) -> date:
    return date.fromisoformat(trade_date)


def _normalize_raw_picks(raw_picks: Any) -> list[dict[str, Any]]:
    if isinstance(raw_picks, dict):
        raw_picks = raw_picks.get("picks", [])
    return list(raw_picks) if isinstance(raw_picks, list) else []


def _load_signal_docs(trade_date: str) -> list[dict[str, Any]]:
    cursor = _get_db()["arena_signals"].find({"trade_date": trade_date}, {"_id": 0})
    return list(cursor)


def _build_consensus_payload(trade_date: str) -> dict[str, Any]:
    report = compare_providers(_parse_trade_date(trade_date))
    overlap = report.get("overlap", {})
    provider_docs = {doc["provider"]: doc for doc in _load_signal_docs(trade_date)}
    consensus = []
    for stock_code in overlap.get("common_all", []):
        providers = []
        for provider, doc in provider_docs.items():
            pick = next(
                (
                    item
                    for item in _normalize_raw_picks(doc.get("raw_picks"))
                    if item.get("stock_code") == stock_code
                ),
                {},
            )
            providers.append(
                {
                    "provider": provider,
                    "confidence": float(pick.get("confidence", 0.0) or 0.0),
                    "reason": str(pick.get("reason", "")),
                }
            )
        consensus.append({"stock_code": stock_code, "providers": providers})
    return {
        "trade_date": trade_date,
        "provider_count": len(report.get("providers", [])),
        "common_all": consensus,
        "common_multi_count": overlap.get("common_multi_count", 0),
        "unique_picks": overlap.get("unique_picks", []),
    }


def _json_file(page_name: str) -> FileResponse:
    return FileResponse(_TEMPLATE_DIR / page_name, media_type="text/html")


def _read_memory_file(provider: str, filename: str) -> str:
    file_path = _MEMORY_ROOT / provider / filename
    if not file_path.exists():
        return ""
    return file_path.read_text(encoding="utf-8")


def _serialize_doc(doc: dict[str, Any] | None) -> dict[str, Any]:
    if not doc:
        return {}
    payload = dict(doc)
    payload.pop("_id", None)
    return json.loads(json.dumps(payload, default=str))


def _overview_rankings(trade_date: str) -> list[dict[str, Any]]:
    cursor = _get_db()["arena_daily_snapshots"].find(
        {"trade_date": trade_date},
        {"_id": 0},
    ).sort("cumulative_return_pct", DESCENDING)
    rankings = list(cursor)
    for index, item in enumerate(rankings, start=1):
        item["rank"] = index
    return rankings


def _signal_summary(trade_date: str) -> dict[str, Any]:
    docs = _load_signal_docs(trade_date)
    total_signals = sum(int(doc.get("signal_count", 0) or 0) for doc in docs)
    providers = []
    for doc in docs:
        providers.append(
            {
                "provider": doc.get("provider", ""),
                "signal_count": int(doc.get("signal_count", 0) or 0),
                "status": doc.get("status", "unknown"),
                "fallback_detected": bool(doc.get("fallback_detected", False)),
            }
        )
    return {
        "provider_count": len(docs),
        "total_signal_count": total_signals,
        "providers": providers,
    }


def _equity_curve(provider: str, days: int) -> list[dict[str, Any]]:
    cursor = _get_db()["arena_daily_snapshots"].find(
        {"provider": provider},
        {"_id": 0},
    ).sort("trade_date", DESCENDING)
    rows = list(cursor.limit(days)) if days > 0 else list(cursor)
    rows.reverse()
    return rows


def _latest_snapshot(provider: str) -> dict[str, Any]:
    return _serialize_doc(
        _get_db()["arena_daily_snapshots"].find_one(
            {"provider": provider},
            sort=[("trade_date", DESCENDING)],
        )
    )


def _to_decimal_return(value: Any) -> float:
    try:
        return float(value or 0.0) / 100.0
    except (TypeError, ValueError):
        return 0.0


def _max_drawdown(total_assets: list[float]) -> float:
    peak = 0.0
    max_drawdown = 0.0
    for asset in total_assets:
        asset_value = float(asset or 0.0)
        if asset_value > peak:
            peak = asset_value
        if peak <= 0.0:
            continue
        drawdown = (peak - asset_value) / peak
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _compute_strategy_metrics_from_rows(
    rows: list[dict[str, Any]],
    benchmark_returns: list[float],
    risk_free_rate: float = _DEFAULT_RISK_FREE_RATE,
) -> dict[str, float]:
    portfolio_returns = [_to_decimal_return(row.get("daily_return_pct")) for row in rows]
    if not portfolio_returns:
        return {
            "sharpe_ratio": 0.0,
            "alpha_pct": 0.0,
            "beta": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate": 0.0,
            "calmar_ratio": 0.0,
            "annual_return_pct": 0.0,
            "volatility_pct": 0.0,
        }

    benchmark = benchmark_returns[: len(portfolio_returns)]
    pair_count = min(len(portfolio_returns), len(benchmark))
    portfolio_for_beta = portfolio_returns[:pair_count]
    benchmark_for_beta = benchmark[:pair_count]

    mean_daily = sum(portfolio_returns) / len(portfolio_returns)
    variance = sum((value - mean_daily) ** 2 for value in portfolio_returns) / len(portfolio_returns)
    volatility = math.sqrt(variance) * math.sqrt(252)
    sharpe_ratio = (mean_daily / math.sqrt(variance) * math.sqrt(252)) if variance > 0 else 0.0

    benchmark_mean = (
        sum(benchmark_for_beta) / pair_count if pair_count else 0.0
    )
    benchmark_variance = (
        sum((value - benchmark_mean) ** 2 for value in benchmark_for_beta) / pair_count
        if pair_count
        else 0.0
    )
    covariance = (
        sum(
            (portfolio_for_beta[index] - mean_daily)
            * (benchmark_for_beta[index] - benchmark_mean)
            for index in range(pair_count)
        )
        / pair_count
        if pair_count
        else 0.0
    )
    beta = covariance / benchmark_variance if benchmark_variance > 0 else 0.0

    annual_return = (1 + mean_daily) ** 252 - 1
    benchmark_annual_return = (1 + benchmark_mean) ** 252 - 1
    alpha = annual_return - risk_free_rate - beta * (benchmark_annual_return - risk_free_rate)
    total_assets = [float(row.get("total_asset", 0.0) or 0.0) for row in rows]
    max_drawdown = _max_drawdown(total_assets)
    win_rate = sum(1 for value in portfolio_returns if value > 0) / len(portfolio_returns)
    calmar_ratio = annual_return / max_drawdown if max_drawdown > 0 else 0.0
    return {
        "sharpe_ratio": sharpe_ratio,
        "alpha_pct": alpha * 100,
        "beta": beta,
        "max_drawdown_pct": max_drawdown * 100,
        "win_rate": win_rate,
        "calmar_ratio": calmar_ratio,
        "annual_return_pct": annual_return * 100,
        "volatility_pct": volatility * 100,
    }


def compute_strategy_metrics(provider: str, days: int = 30) -> dict[str, float]:
    rows = _equity_curve(provider, days)
    return _compute_strategy_metrics_from_rows(rows, _benchmark_returns(days))


def _benchmark_returns(days: int) -> list[float]:
    provider_curves = _all_equity_curve(days)
    if not provider_curves:
        return []
    max_length = max(len(item.get("curve", [])) for item in provider_curves)
    returns: list[float] = []
    for index in range(max_length):
        values = []
        for provider_curve in provider_curves:
            curve = provider_curve.get("curve", [])
            if index >= len(curve):
                continue
            values.append(_to_decimal_return(curve[index].get("daily_return_pct")))
        returns.append(sum(values) / len(values) if values else 0.0)
    return returns


def _metrics_payload(provider: str, days: int) -> dict[str, Any]:
    rows = _equity_curve(provider, days)
    if not rows:
        raise HTTPException(status_code=404, detail="未找到净值快照")
    return {
        "provider": provider,
        "days": days,
        "metrics": compute_strategy_metrics(provider, days),
    }


def _positions_payload(provider: str) -> dict[str, Any]:
    account = _serialize_doc(_get_db()["arena_accounts"].find_one({"provider": provider}))
    if account and account.get("positions") is not None:
        return account
    snapshot = _latest_snapshot(provider)
    if account and snapshot and snapshot.get("positions") is not None:
        account["positions"] = snapshot.get("positions", {})
        return account
    if not account and snapshot:
        from .arena_portfolio import get_capital_pool
        initial_capital = get_capital_pool(provider)
        return {
            "provider": provider,
            "initial_capital": initial_capital,
            "cash": float(snapshot.get("cash", initial_capital) or initial_capital),
            "positions": snapshot.get("positions", {}),
            "total_asset": float(snapshot.get("total_asset", initial_capital) or initial_capital),
            "daily_return_pct": float(snapshot.get("daily_return_pct", 0.0) or 0.0),
            "cumulative_return_pct": float(snapshot.get("cumulative_return_pct", 0.0) or 0.0),
        }
    if not account:
        raise HTTPException(status_code=404, detail="未找到 provider 账户")
    return account


def _daily_trade_payload(provider: str, trade_date: str) -> dict[str, Any]:
    doc = _serialize_doc(
        _get_db()["arena_trades"].find_one(
            {"provider": provider, "trade_date": trade_date}
        )
    )
    if not doc:
        raise HTTPException(status_code=404, detail="未找到交易记录")
    return doc


def _review_payload(provider: str, trade_date: str) -> dict[str, Any]:
    doc = _serialize_doc(
        _get_db()["arena_reviews"].find_one(
            {"provider": provider, "review_date": trade_date}
        )
    )
    if not doc:
        raise HTTPException(status_code=404, detail="未找到复盘记录")
    return doc


def _signals_payload(provider: str, trade_date: str) -> dict[str, Any]:
    doc = _serialize_doc(
        _get_db()["arena_signals"].find_one(
            {"provider": provider, "trade_date": trade_date}
        )
    )
    if not doc:
        raise HTTPException(status_code=404, detail="未找到信号记录")
    doc["identity"] = {
        "provider": provider,
        "requested_model": doc.get("requested_model"),
        "actual_model": doc.get("actual_model"),
        "base_url": doc.get("base_url"),
        "attempts": doc.get("attempts", 0),
        "status": doc.get("status", "unknown"),
        "fallback_detected": bool(doc.get("fallback_detected", False)),
        "error_message": doc.get("error_message", ""),
        "parse_mode": doc.get("parse_mode", ""),
        "response_truncated": bool(doc.get("response_truncated", False)),
    }
    return doc


def _timeline_payload(trade_date: str) -> dict[str, Any]:
    db = _get_db()
    return {
        "trade_date": trade_date,
        "signals": list(db["arena_signals"].find({"trade_date": trade_date}, {"_id": 0})),
        "trades": list(db["arena_trades"].find({"trade_date": trade_date}, {"_id": 0})),
        "snapshots": list(
            db["arena_daily_snapshots"].find({"trade_date": trade_date}, {"_id": 0})
        ),
        "reviews": list(db["arena_reviews"].find({"review_date": trade_date}, {"_id": 0})),
    }


def _all_equity_curve(days: int) -> list[dict[str, Any]]:
    """Build equity curves for all enabled providers."""
    from .arena_portfolio import get_enabled_providers

    providers = []
    for provider in get_enabled_providers():
        curve = _equity_curve(provider, days)
        providers.append({
            "provider": provider,
            "curve": curve,
        })
    return providers


def _health_summary() -> dict[str, Any]:
    pipeline = [
        {
            "$group": {
                "_id": "$provider",
                "total": {"$sum": 1},
                "success_count": {
                    "$sum": {
                        "$cond": [{"$eq": ["$status", "success"]}, 1, 0]
                    }
                },
                "fallback_count": {
                    "$sum": {
                        "$cond": [{"$eq": ["$fallback_detected", True]}, 1, 0]
                    }
                },
                "last_generated_at": {"$max": "$generated_at"},
            }
        }
    ]
    providers = []
    for item in _get_db()["arena_signals"].aggregate(pipeline):
        total = int(item.get("total", 0) or 0)
        success_count = int(item.get("success_count", 0) or 0)
        providers.append(
            {
                "provider": item.get("_id", ""),
                "total_runs": total,
                "success_count": success_count,
                "fallback_count": int(item.get("fallback_count", 0) or 0),
                "failure_count": max(total - success_count, 0),
                "success_rate": success_count / total if total else 0.0,
                "last_generated_at": item.get("last_generated_at"),
            }
        )
    providers.sort(key=lambda item: item["provider"])
    return {"providers": providers}


@router.get("/overview")
async def get_overview(
    trade_date: str = Query(default_factory=lambda: date.today().isoformat()),
) -> dict[str, Any]:
    return {
        "trade_date": trade_date,
        "rankings": _overview_rankings(trade_date),
        "signals": _signal_summary(trade_date),
        "consensus": _build_consensus_payload(trade_date),
    }


@router.get("/rankings")
async def get_rankings(
    trade_date: str = Query(default_factory=lambda: date.today().isoformat()),
) -> dict[str, Any]:
    return {"trade_date": trade_date, "rankings": _overview_rankings(trade_date)}


@router.get("/all-equity-curve")
async def get_all_equity_curve(
    days: int = Query(30, ge=1, le=3650),
) -> dict[str, Any]:
    return {"days": days, "providers": _all_equity_curve(days)}


@router.get("/settings")
async def get_settings() -> dict[str, Any]:
    """Return current config for the settings page."""
    from .config import get_config
    cfg = get_config()
    return {
        "signal_source": cfg.get("signal_source", {}),
        "arena": cfg.get("arena", {}),
        "mongodb": cfg.get("mongodb", {}),
        "accounts": cfg.get("accounts", {}),
    }


@router.post("/settings")
async def update_settings(request: Request) -> dict[str, Any]:
    """Write updated settings back to settings.yaml."""
    from .config import update_settings as _update_settings
    body = await request.json()
    _update_settings(body)
    return {"status": "ok"}


@router.get("/connectivity-check")
async def connectivity_check() -> dict[str, Any]:
    """Check connectivity of MongoDB, QMT proxies, and AI model providers."""
    from .config import get_config, get_mongo_db, get_mongo_uri
    import httpx
    checks = []

    # MongoDB
    try:
        uri = get_mongo_uri()
        if uri:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db_name = get_mongo_db()
            checks.append({
                "name": "MongoDB", "status": "ok",
                "detail": f"{uri} / {db_name}",
            })
            client.close()
        else:
            checks.append({"name": "MongoDB", "status": "warn", "detail": "URI not configured"})
    except Exception as e:
        checks.append({"name": "MongoDB", "status": "err", "detail": str(e)})

    # QMT proxies
    cfg = get_config()
    for acc_type, acc in cfg.get("accounts", {}).items():
        if not acc.get("enabled", False):
            checks.append({"name": f"QMT {acc_type}", "status": "warn", "detail": "disabled"})
            continue
        port = acc.get("proxy_port", 8011)
        try:
            resp = httpx.get(f"http://127.0.0.1:{port}/api/health", timeout=5)
            data = resp.json()
            if data.get("connected"):
                checks.append({
                    "name": f"QMT {acc_type}", "status": "ok",
                    "detail": f"port={port}, connected",
                })
            else:
                checks.append({
                    "name": f"QMT {acc_type}", "status": "warn",
                    "detail": f"port={port}, not connected",
                })
        except Exception as e:
            checks.append({
                "name": f"QMT {acc_type}", "status": "err",
                "detail": f"port={port}: {e}",
            })

    # AI providers
    for name, pcfg in cfg.get("arena", {}).get("providers", {}).items():
        if not pcfg.get("enabled", False):
            checks.append({"name": name, "status": "warn", "detail": "disabled"})
            continue
        base_url = pcfg.get("base_url", "").rstrip("/")
        ptype = pcfg.get("type", "nadirclaw")
        try:
            if ptype == "anthropic":
                url = base_url + "/v1/messages"
            else:
                url = base_url + "/models"
            resp = httpx.get(url, timeout=5, headers={
                "Authorization": f"Bearer {pcfg.get('api_key', 'none')}",
                "x-api-key": pcfg.get("api_key", "none"),
            })
            checks.append({
                "name": name, "status": "ok",
                "detail": f"{resp.status_code} from {base_url}",
            })
        except Exception as e:
            checks.append({
                "name": name, "status": "err",
                "detail": f"{base_url}: {e}",
            })

    return {"checks": checks}


@router.get("/{provider}/confidence-deciles")
async def get_confidence_deciles(provider: str) -> dict[str, Any]:
    """返回 provider 的 confidence 十分位累计收益曲线。"""
    cursor = _get_db()["arena_reviews"].find(
        {"provider": provider, "valid": True},
        {"_id": 0, "review_date": 1, "pick_details": 1},
    ).sort("review_date", 1)

    review_rows = list(cursor)
    if not review_rows:
        return {"provider": provider, "dates": [], "series": []}

    all_picks: list[dict[str, Any]] = []
    for review in review_rows:
        for pick in review.get("pick_details", []):
            all_picks.append({
                "confidence": float(pick.get("confidence", 0)),
                "pct_change": float(pick.get("pct_change", 0)),
                "review_date": review["review_date"],
            })

    if not all_picks:
        return {"provider": provider, "dates": [], "series": []}

    all_picks.sort(key=lambda x: x["confidence"])
    n = len(all_picks)
    decile_size = max(1, n // 10)

    decile_map: list[set[tuple[str, float, float]]] = []
    for i in range(10):
        start = i * decile_size
        end = start + decile_size if i < 9 else n
        group = all_picks[start:end]
        decile_map.append({
            (item["review_date"], item["confidence"], item["pct_change"])
            for item in group
        })

    dates = sorted({row["review_date"] for row in all_picks})
    series = []
    for i in range(10):
        cumulative = 1.0
        points = []
        for review_date in dates:
            day_returns = [
                pct_change for d, _conf, pct_change in decile_map[i]
                if d == review_date
            ]
            avg_return = sum(day_returns) / len(day_returns) if day_returns else 0.0
            cumulative *= (1 + avg_return / 100.0)
            points.append(round((cumulative - 1) * 100, 3))
        series.append({
            "decile": i + 1,
            "name": f"D{i+1}",
            "values": points,
        })

    return {"provider": provider, "dates": dates, "series": series, "total_picks": n}


@router.get("/{provider}/hit-rate-curve")
async def get_hit_rate_curve(provider: str) -> dict[str, Any]:
    """返回 provider 历史每日命中率序列。"""
    cursor = _get_db()["arena_reviews"].find(
        {"provider": provider, "valid": True},
        {"_id": 0, "review_date": 1, "hit_rate": 1, "pick_count": 1},
    ).sort("review_date", 1)
    rows = list(cursor)
    dates = [r["review_date"] for r in rows]
    hit_rates = [round(float(r.get("hit_rate", 0)) * 100, 1) for r in rows]
    pick_counts = [int(r.get("pick_count", 0) or 0) for r in rows]
    return {"provider": provider, "dates": dates, "hit_rates": hit_rates, "pick_counts": pick_counts}


@router.get("/{provider}/equity-curve")
async def get_equity_curve(
    provider: str,
    days: int = Query(30, ge=1, le=3650),
) -> dict[str, Any]:
    return {"provider": provider, "days": days, "curve": _equity_curve(provider, days)}


@router.get("/{provider}/metrics")
async def get_metrics(
    provider: str,
    days: int = Query(30, ge=1, le=3650),
) -> dict[str, Any]:
    return _metrics_payload(provider, days)


@router.get("/{provider}/daily/{trade_date}")
async def get_daily(provider: str, trade_date: str) -> dict[str, Any]:
    return _daily_trade_payload(provider, trade_date)


@router.get("/{provider}/positions")
async def get_positions(provider: str) -> dict[str, Any]:
    return _positions_payload(provider)


@router.get("/{provider}/memory")
async def get_memory(provider: str) -> dict[str, Any]:
    return {
        "provider": provider,
        "facts": _read_memory_file(provider, "facts.md"),
        "lessons": _read_memory_file(provider, "lessons.md"),
    }


@router.get("/{provider}/prompt/{trade_date}")
async def get_prompt(provider: str, trade_date: str) -> dict[str, Any]:
    """获取指定 provider 和日期的 prompt 及模型原始响应。"""
    col = _get_db()["arena_signals"]
    doc = col.find_one(
        {"trade_date": trade_date, "provider": provider},
        {"system_prompt": 1, "user_prompt": 1, "raw_response": 1,
         "requested_model": 1, "actual_model": 1, "_id": 0},
    )
    if not doc:
        return {"provider": provider, "trade_date": trade_date,
                "system_prompt": "", "user_prompt": "",
                "raw_response": "", "requested_model": "", "actual_model": ""}
    return {
        "provider": provider,
        "trade_date": trade_date,
        "system_prompt": doc.get("system_prompt", ""),
        "user_prompt": doc.get("user_prompt", ""),
        "raw_response": doc.get("raw_response", ""),
        "requested_model": doc.get("requested_model", ""),
        "actual_model": doc.get("actual_model", ""),
    }


@router.get("/{provider}/prompt-history")
async def get_prompt_history(provider: str, limit: int = 10) -> list[dict[str, Any]]:
    """获取 provider 最近的 prompt/响应记录，按日期倒序。"""
    col = _get_db()["arena_signals"]
    docs = col.find(
        {"provider": provider, "raw_response": {"$ne": ""}},
        {"trade_date": 1, "requested_model": 1, "actual_model": 1,
         "signal_count": 1, "status": 1, "parse_mode": 1,
         "response_truncated": 1, "error_message": 1, "_id": 0},
        sort=[("trade_date", DESCENDING)],
        limit=limit,
    )
    return list(docs)


@router.delete("/signals/{trade_date}")
async def clear_arena_signals(trade_date: str) -> dict[str, Any]:
    """清空指定日期的所有 arena_signals 记录。"""
    from datetime import date as date_type

    col = _get_db()["arena_signals"]
    result = col.delete_many({"trade_date": trade_date})
    return {
        "message": f"已清空 {trade_date} 的 {result.deleted_count} 条 arena_signals",
        "deleted": result.deleted_count,
    }


@router.post("/signals/regenerate/{trade_date}")
async def regenerate_arena_signals(trade_date: str) -> dict[str, Any]:
    """重新生成指定日期的 arena 信号（跳过已成功的 provider）。"""
    from datetime import date as date_type
    from loguru import logger

    target = date_type.fromisoformat(trade_date)
    try:
        from .arena_runner import generate_daily_arena_signals
        batch = generate_daily_arena_signals(target_date=target)
        return {
            "message": f"信号重新生成完成: {len(batch.signals)} 条",
            "signal_count": len(batch.signals),
            "batch_id": batch.batch_id,
        }
    except Exception as e:
        logger.error(f"重新生成 arena 信号失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/comparison/{trade_date}")
async def get_comparison(trade_date: str) -> dict[str, Any]:
    return compare_providers(_parse_trade_date(trade_date))


@router.get("/{provider}/review/{trade_date}")
async def get_review(provider: str, trade_date: str) -> dict[str, Any]:
    return _review_payload(provider, trade_date)


@router.get("/consensus/{trade_date}")
async def get_consensus(trade_date: str) -> dict[str, Any]:
    return _build_consensus_payload(trade_date)


@router.get("/timeline/{trade_date}")
async def get_timeline(trade_date: str) -> dict[str, Any]:
    return _timeline_payload(trade_date)


@router.get("/signals/{provider}/{trade_date}")
async def get_signals(provider: str, trade_date: str) -> dict[str, Any]:
    return _signals_payload(provider, trade_date)


@router.get("/trades/{provider}/{trade_date}")
async def get_trades(provider: str, trade_date: str) -> dict[str, Any]:
    doc = _serialize_doc(
        _get_db()["arena_trades"].find_one(
            {"provider": provider, "trade_date": trade_date}
        )
    )
    if not doc:
        return {"provider": provider, "trade_date": trade_date, "trades": []}
    return doc


@router.get("/health")
async def get_health() -> dict[str, Any]:
    return _health_summary()


@router.get("/page")
async def arena_index_page() -> FileResponse:
    return _json_file("index.html")


@router.get("/page/agent")
async def arena_agent_page() -> FileResponse:
    return _json_file("agent.html")


@router.get("/page/compare")
async def arena_compare_page() -> FileResponse:
    return _json_file("compare.html")


@router.get("/page/review")
async def arena_review_page() -> FileResponse:
    return _json_file("review.html")


@router.get("/page/timeline")
async def arena_timeline_page() -> FileResponse:
    return _json_file("timeline.html")


@router.get("/page/health")
async def arena_health_page() -> FileResponse:
    return _json_file("health.html")


@router.get("/page/settings")
async def arena_settings_page() -> FileResponse:
    return _json_file("settings.html")
