from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from pymongo.errors import PyMongoError

from .config import get_config
from .settlement import get_asset_history, get_settlement, get_settlement_dates

router = APIRouter()
templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent.parent / "templates")
)


def _is_weekday(date_str: str) -> bool:
    """Return True if date_str (YYYY-MM-DD) is a weekday (Mon-Fri)."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").weekday() < 5
    except (ValueError, TypeError):
        return True


def _get_runtime_engine(account_type: str) -> Any | None:
    """Get registered runtime engine from global config."""
    return get_config().get("_runtime_engines", {}).get(account_type)


def _register_runtime_engine(engine: Any, account_type: str = "simulation") -> None:
    """Register runtime engine into global config dict."""
    cfg = get_config()
    if "_runtime_engines" not in cfg:
        cfg["_runtime_engines"] = {}
    cfg["_runtime_engines"][account_type] = engine


def set_runtime_engine(engine: Any, account_type: str = "simulation") -> None:
    """注册内嵌 QMT 引擎，供 serve 模式下的 dashboard 使用。"""
    _register_runtime_engine(engine, account_type)


def _get_proxy_port(account_type: str) -> int:
    """获取指定账户的 QMT 代理端口。"""
    cfg = get_config()
    acc = cfg.get("accounts", {}).get(account_type, {})
    return acc.get("proxy_port", 8011 if account_type == "simulation" else 8012)


def _safe_float(value: Any) -> float:
    return float(value or 0.0)


def _build_runtime_realtime(account_type: str) -> dict[str, Any] | None:
    """直接查询内嵌 QMT 引擎（serve 模式）并补充 daily_pnl。"""
    engine = _get_runtime_engine(account_type)
    if engine is None or not getattr(engine, "connected", False):
        return None

    try:
        asset_obj = engine.query_asset()
        positions_obj = engine.query_positions() or []
        positions = []
        for position in positions_obj:
            volume = int(getattr(position, "volume", 0) or 0)
            cost_price = _safe_float(getattr(position, "open_price", 0.0))
            market_value = _safe_float(getattr(position, "market_value", 0.0))
            positions.append(
                {
                    "stock_code": str(getattr(position, "stock_code", "")),
                    "volume": volume,
                    "can_use_volume": int(getattr(position, "can_use_volume", 0) or 0),
                    "cost_price": cost_price,
                    "market_value": market_value,
                    "unrealized_pnl": market_value - volume * cost_price,
                }
            )

        return {
            "connected": True,
            "account_type": account_type,
            "asset": {
                "total_asset": _safe_float(getattr(asset_obj, "total_asset", 0.0)),
                "cash": _safe_float(getattr(asset_obj, "cash", 0.0)),
                "market_value": _safe_float(getattr(asset_obj, "market_value", 0.0)),
                "frozen_cash": _safe_float(getattr(asset_obj, "frozen_cash", 0.0)),
            },
            "positions": positions,
            "daily_pnl": _get_latest_daily_pnl(account_type),
        }
    except Exception as e:
        logger.warning(f"内嵌引擎查询失败: {e}")
        return None


async def _fetch_from_proxy(account_type: str, path: str = "/api/realtime") -> dict | None:
    """通过 HTTP 从 QMT 代理获取数据。"""
    port = _get_proxy_port(account_type)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"http://127.0.0.1:{port}{path}",
                timeout=5.0,
            )
            if resp.status_code == 200:
                return resp.json()
    except (httpx.ConnectError, httpx.TimeoutException):
        logger.debug(f"QMT 代理 ({account_type}:{port}) 不可用")
    return None


def _get_latest_daily_pnl(account_type: str) -> float:
    try:
        history = get_asset_history(days=1, account_type=account_type)
        return history[-1]["daily_pnl"] if history else 0.0
    except PyMongoError as exc:
        logger.warning(f"读取结算历史失败: {exc}")
        return 0.0


def _load_settlements(account_type: str) -> list[dict[str, Any]]:
    try:
        dates = get_settlement_dates(account_type)
        settlements = [get_settlement(item, account_type) for item in dates]
        results = [item for item in settlements if item is not None]
        return [s for s in results if _is_weekday(s.get("date", s.get("trade_date", "")))]
    except PyMongoError as exc:
        logger.warning(f"读取结算列表失败: {exc}")
        return []


def _build_empty_realtime(account_type: str) -> dict[str, Any]:
    return {
        "enabled": True,
        "connected": False,
        "account_type": account_type,
        "asset": {
            "total_asset": 0.0,
            "cash": 0.0,
            "market_value": 0.0,
            "frozen_cash": 0.0,
        },
        "positions": [],
        "daily_pnl": 0.0,
        "snapshot_date": None,
        "snapshot_mode": True,
    }


def _build_snapshot_realtime(account_type: str) -> dict[str, Any]:
    settlements = _load_settlements(account_type)
    latest = settlements[0] if settlements else None
    if not latest:
        return _build_empty_realtime(account_type)
    positions = latest.get("positions", [])
    asset = latest.get("asset", {})
    # Fallback to top-level fields if asset subdocument is empty
    total_asset = _safe_float(asset.get("total_asset", 0.0))
    if total_asset == 0.0:
        total_asset = _safe_float(latest.get("total_asset", 0.0))
    cash = _safe_float(asset.get("cash", 0.0))
    if cash == 0.0:
        cash = _safe_float(latest.get("cash", 0.0))
    market_value = _safe_float(asset.get("market_value", 0.0))
    if market_value == 0.0:
        market_value = _safe_float(latest.get("market_value", 0.0))
    return {
        "enabled": True,
        "connected": False,
        "account_type": account_type,
        "asset": {
            "total_asset": total_asset,
            "cash": cash,
            "market_value": market_value,
            "frozen_cash": 0.0,
        },
        "positions": positions,
        "daily_pnl": _safe_float(latest.get("daily_pnl", 0.0)),
        "snapshot_date": latest.get("date"),
        "snapshot_mode": True,
    }


@router.get("/dashboard")
async def dashboard_page(request: Request, account: str = Query("simulation")):
    cfg = get_config()
    account_cfg = cfg.get("accounts", {}).get(account, {})
    enabled = account_cfg.get("enabled", False)

    realtime = _build_snapshot_realtime(account)
    realtime["enabled"] = enabled
    if not enabled:
        realtime = _build_empty_realtime(account)
        realtime["enabled"] = False

    live_enabled = cfg.get("accounts", {}).get("live", {}).get("enabled", False)

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "account": account,
            "realtime": realtime,
            "settlements": _load_settlements(account),
            "live_enabled": live_enabled,
        },
    )


@router.get("/dashboard/{date}")
def settlement_page(
    request: Request,
    date: str,
    account: str = Query("simulation"),
):
    settlement = get_settlement(date, account)
    if settlement is None:
        raise HTTPException(status_code=404, detail="未找到结算记录")
    return templates.TemplateResponse(
        request,
        "settlement.html",
        {
            "account": account,
            "settlement": settlement,
            "raw_json": json.dumps(settlement, ensure_ascii=False, indent=2),
        },
    )


@router.get("/api/dashboard/asset-history")
def asset_history_api(
    days: int = Query(7, ge=0),
    account: str = Query("simulation"),
):
    query_days = None if days == 0 else days
    history = get_asset_history(query_days, account)
    return JSONResponse([h for h in history if _is_weekday(h.get("date", ""))])


@router.get("/api/dashboard/settlements")
def settlements_api(account: str = Query("simulation")):
    return JSONResponse(_load_settlements(account))


@router.get("/api/dashboard/settlement/{date}")
def settlement_api(date: str, account: str = Query("simulation")):
    settlement = get_settlement(date, account)
    if settlement is None:
        raise HTTPException(status_code=404, detail="未找到结算记录")
    return JSONResponse(settlement)


def _persist_realtime_as_settlement(account_type: str, data: dict[str, Any]) -> None:
    """将实时数据保存为结算快照，以便下次页面加载时直接读取。
    不覆盖已有正式结算文档（含 orders/trades）。"""
    from datetime import date
    from .settlement import save_settlement_document, get_settlement
    from .config import get_account_config

    today = date.today().isoformat()
    existing = get_settlement(today, account_type)
    if existing and (existing.get("orders") or existing.get("trades")):
        # 已有正式结算数据，不覆盖
        return

    asset = data.get("asset", {})
    positions = data.get("positions", [])
    total_asset = _safe_float(asset.get("total_asset", 0.0))

    # 正确计算 daily_pnl：今天总资产 - 昨天总资产
    # 不依赖传入 data 中的 daily_pnl，避免 mock/残留值污染
    daily_pnl = 0.0
    if existing:
        prev_total = _safe_float(existing.get("asset", {}).get("total_asset", 0.0))
        if prev_total > 0:
            daily_pnl = total_asset - prev_total

    document = {
        "date": today,
        "account_type": account_type,
        "account_id": str(get_account_config(account_type).get("account_id", "")),
        "asset": {
            "total_asset": total_asset,
            "cash": _safe_float(asset.get("cash", 0.0)),
            "market_value": _safe_float(asset.get("market_value", 0.0)),
            "frozen_cash": _safe_float(asset.get("frozen_cash", 0.0)),
        },
        "positions": positions,
        "orders": [],
        "trades": [],
        "daily_pnl": _safe_float(daily_pnl),
        "daily_return_pct": 0.0,
    }
    try:
        save_settlement_document(document)
    except Exception as exc:
        logger.warning(f"保存实时快照失败: {exc}")


@router.get("/api/dashboard/realtime")
async def realtime_api(account: str = Query("simulation")):
    cfg = get_config()
    account_cfg = cfg.get("accounts", {}).get(account, {})
    if not account_cfg.get("enabled", False):
        return JSONResponse({"error": "账户未启用"}, status_code=403)

    runtime_data = _build_runtime_realtime(account)
    if runtime_data is not None:
        _persist_realtime_as_settlement(account, runtime_data)
        return JSONResponse(runtime_data)

    proxy_data = await _fetch_from_proxy(account)
    if proxy_data is None:
        return JSONResponse(
            {"connected": False, "error": "QMT 代理未启动"},
            status_code=503,
        )
    if "daily_pnl" not in proxy_data:
        proxy_data["daily_pnl"] = _get_latest_daily_pnl(account)
    _persist_realtime_as_settlement(account, proxy_data)
    return JSONResponse(proxy_data)


@router.get("/api/dashboard/signals")
def signals_api(signal_date: str = Query(default=None)):
    """获取指定日期的交易信号（优先远程，失败则 mock）"""
    from datetime import date as date_type, timedelta
    from .signal_client import fetch_signals

    target = signal_date or (date_type.today() + timedelta(days=1)).isoformat()
    try:
        cfg = get_config()
        old_mode = cfg["signal_source"].get("mode", "mock")
        cfg["signal_source"]["mode"] = "remote"
        batch = fetch_signals(date_type.fromisoformat(target))
        cfg["signal_source"]["mode"] = old_mode
        return JSONResponse(batch.model_dump(mode="json"))
    except Exception as exc:
        logger.warning(f"获取远程信号失败，尝试 mock: {exc}")
        try:
            batch = fetch_signals(date_type.fromisoformat(target))
            return JSONResponse(batch.model_dump(mode="json"))
        except Exception as exc2:
            return JSONResponse({"error": str(exc2)}, status_code=502)


def _get_account_settings_map() -> dict[str, dict]:
    """构建账户设置摘要（用于 API 响应）。"""
    cfg = get_config()
    result = {}
    for acc_type in ("simulation", "live"):
        if "accounts" in cfg and acc_type in cfg["accounts"]:
            acc = cfg["accounts"][acc_type]
        else:
            acc = {}
        result[acc_type] = {
            "enabled": acc.get("enabled", False),
            "account_id": acc.get("account_id", ""),
            "proxy_port": acc.get("proxy_port", 8011 if acc_type == "simulation" else 8012),
            "auto_reverse_repo_enabled": acc.get("auto_reverse_repo_enabled", False),
            "reverse_repo_min_amount": acc.get("reverse_repo_min_amount", 1000),
        }
    return result


@router.get("/api/dashboard/account-settings")
def get_account_settings_api():
    """获取两个账户的设置状态。"""
    return JSONResponse(_get_account_settings_map())


@router.post("/api/dashboard/account-settings")
async def update_account_settings_api(request: Request):
    """更新指定账户的设置，写回 settings.yaml。"""
    from .config import update_account_settings

    body = await request.json()
    account_type = body.pop("account_type", None)
    if not account_type:
        return JSONResponse({"error": "缺少 account_type"}, status_code=400)

    logger.info(f"更新账户设置: {account_type} -> {body}")
    updated = update_account_settings(account_type, body)
    return JSONResponse({account_type: updated})
