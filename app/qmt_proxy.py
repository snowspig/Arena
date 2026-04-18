"""
QMT 独立代理服务 —— 每个账户一个进程，保持长连接。

用法:
    python main.py proxy --account simulation   (port 8011)
    python main.py proxy --account live         (port 8012)
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from loguru import logger

from .config import get_account_config
from .qmt_engine import QmtEngine

app = FastAPI(title="QMT Proxy")

# 模块级状态：每个进程只有一个 engine
_engine: QmtEngine | None = None
_account_type: str = ""
_max_reconnect_attempts = 3
_reconnect_interval = 5


def _safe_float(value: Any) -> float:
    return float(value or 0.0)


def _build_position_response(position: Any) -> dict[str, Any]:
    volume = int(getattr(position, "volume", 0) or 0)
    cost_price = _safe_float(getattr(position, "open_price", 0.0))
    market_value = _safe_float(getattr(position, "market_value", 0.0))
    return {
        "stock_code": str(getattr(position, "stock_code", "")),
        "volume": volume,
        "can_use_volume": int(getattr(position, "can_use_volume", 0) or 0),
        "cost_price": cost_price,
        "market_value": market_value,
        "unrealized_pnl": market_value - volume * cost_price,
    }


def init_engine(account_type: str) -> None:
    """启动时初始化 QMT 连接。"""
    global _engine, _account_type
    _account_type = account_type
    _engine = QmtEngine(account_type=account_type)
    _try_connect()


def _try_connect() -> bool:
    """尝试连接 QMT，支持自动发现账号。"""
    if _engine is None:
        return False
    if _engine.connect():
        logger.info(f"QMT Proxy [{_account_type}] 连接成功")
        return True
    logger.warning(f"QMT Proxy [{_account_type}] 连接失败（QMT 可能未启动或非交易日）")
    return False


def _ensure_connected() -> bool:
    """检查连接状态，断线则尝试重连。"""
    if _engine is None:
        return False
    if _engine.connected:
        return True
    logger.warning(f"QMT Proxy [{_account_type}] 连接已断开，尝试重连...")
    return _try_connect()


@app.get("/api/health")
def health():
    """代理健康检查。"""
    return JSONResponse({
        "connected": _engine.connected if _engine else False,
        "account": _account_type,
    })


@app.get("/api/realtime")
def realtime():
    """实时资产 + 持仓。"""
    if not _ensure_connected():
        return JSONResponse({"connected": False, "error": "QMT 未连接"}, status_code=503)

    try:
        asset_obj = _engine.query_asset()
        positions_obj = _engine.query_positions() or []
        positions = [_build_position_response(p) for p in positions_obj]
        return JSONResponse({
            "connected": True,
            "account_type": _account_type,
            "asset": {
                "total_asset": _safe_float(getattr(asset_obj, "total_asset", 0.0)),
                "cash": _safe_float(getattr(asset_obj, "cash", 0.0)),
                "market_value": _safe_float(getattr(asset_obj, "market_value", 0.0)),
                "frozen_cash": _safe_float(getattr(asset_obj, "frozen_cash", 0.0)),
            },
            "positions": positions,
        })
    except Exception as e:
        logger.error(f"查询实时数据失败: {e}")
        return JSONResponse({"connected": False, "error": str(e)}, status_code=503)


@app.get("/api/orders")
def orders():
    """当日委托。"""
    if not _ensure_connected():
        return JSONResponse({"connected": False}, status_code=503)

    try:
        raw_orders = _engine.query_orders() or []
        result = []
        for o in raw_orders:
            result.append({
                "order_id": getattr(o, "order_id", ""),
                "stock_code": getattr(o, "stock_code", ""),
                "order_type": getattr(o, "order_type", ""),
                "order_volume": getattr(o, "order_volume", 0),
                "traded_volume": getattr(o, "traded_volume", 0),
                "order_status": getattr(o, "status_msg", ""),
                "order_remark": getattr(o, "order_remark", ""),
            })
        return JSONResponse({"connected": True, "orders": result})
    except Exception as e:
        logger.error(f"查询委托失败: {e}")
        return JSONResponse({"connected": False, "error": str(e)}, status_code=503)


@app.get("/api/trades")
def trades():
    """当日成交。"""
    if not _ensure_connected():
        return JSONResponse({"connected": False}, status_code=503)

    try:
        raw_trades = _engine.query_trades() or []
        result = []
        for t in raw_trades:
            result.append({
                "trade_id": getattr(t, "traded_id", ""),
                "stock_code": getattr(t, "stock_code", ""),
                "traded_price": _safe_float(getattr(t, "traded_price", 0.0)),
                "traded_volume": getattr(t, "traded_volume", 0),
                "traded_time": str(getattr(t, "traded_time", "")),
            })
        return JSONResponse({"connected": True, "trades": result})
    except Exception as e:
        logger.error(f"查询成交失败: {e}")
        return JSONResponse({"connected": False, "error": str(e)}, status_code=503)
