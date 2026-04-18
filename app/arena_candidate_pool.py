"""
AI Signal Arena —— 候选池提取

从 MongoDB 提取每日可交易候选股票池。
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from loguru import logger
from pymongo import MongoClient


def _get_qlibrd_db():
    """连接 qlibrd 数据库。"""
    from .config import get_mongo_db, get_mongo_uri
    uri = get_mongo_uri()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client[get_mongo_db("qlibrd_db")]


def build_candidate_pool(
    target_date: date | None = None,
    pool_size: int = 250,
    min_amount: float = 50_000_000,
) -> list[dict[str, Any]]:
    """构建每日候选股票池。

    从 qlibrd.cn_data_stock_features + cn_data_stock_instruments 提取，
    硬筛选后按综合得分排序，取 TOP pool_size 只。

    Args:
        target_date: 目标交易日，默认取最新有数据的那天
        pool_size: 返回的候选数量
        min_amount: 最低成交额（元），默认 5000 万

    Returns:
        候选股票列表，每只包含 stock_code, name, close, pct_change 等
    """
    db = _get_qlibrd_db()
    features_col = db["cn_data_stock_features"]
    instruments_col = db["cn_data_stock_instruments"]

    # 1. 确定最新交易日
    if target_date is None:
        latest = features_col.find_one(sort=[("date", -1)])
        if not latest:
            logger.warning("cn_data_stock_features 无数据")
            return []
        target_date = _normalize_trade_date(latest["date"])

    date_str = target_date.isoformat()
    logger.info(f"构建候选池: date={date_str}, pool_size={pool_size}")

    # 2. 取当日所有股票 features；若无数据则 fallback 到最近交易日
    query_dates = [date_str, datetime.combine(target_date, datetime.min.time())]
    features = list(features_col.find({"date": {"$in": query_dates}}))
    if not features:
        logger.warning(f"无 {date_str} 行情数据，尝试 fallback 到最近交易日")
        latest = features_col.find_one(sort=[("date", -1)])
        if not latest:
            logger.warning("cn_data_stock_features 无可用数据")
            return []
        target_date = _normalize_trade_date(latest["date"])
        date_str = target_date.isoformat()
        query_dates = [date_str, datetime.combine(target_date, datetime.min.time())]
        features = list(features_col.find({"date": {"$in": query_dates}}))
        if not features:
            logger.warning(f"最近交易日 {date_str} 也无数据")
            return []
        logger.info(f"使用最近交易日 {date_str} 构建候选池")

    # 3. 取 instruments 映射 (symbol -> info)
    instruments = {}
    for inst in instruments_col.find():
        sym = inst.get("symbol", "")
        instruments[sym] = inst

    # 4. 硬筛选 + 评分
    candidates = []
    for f in features:
        symbol = f.get("symbol", "")
        close = _safe_float(f.get("close", 0))
        volume = _safe_float(f.get("volume", 0))
        amount = _safe_float(f.get("amount", 0))
        pct_change = _safe_float(f.get("pct_change", 0))
        pre_close = _safe_float(f.get("preClose", 0))

        if close <= 0 or volume <= 0 or amount < min_amount:
            continue

        # 从 instruments 获取额外信息
        inst = instruments.get(symbol, {})
        name = inst.get("name", symbol)
        up_stop = _safe_float(inst.get("up_stop_price", 0))
        down_stop = _safe_float(inst.get("down_stop_price", 0))

        # 剔除 ST
        if "ST" in name or "st" in name:
            continue

        # 剔除停牌 (成交量为 0 或 suspendFlag)
        if f.get("suspendFlag") or volume == 0:
            continue

        # 剔除一字涨停：开=收=高=低 且涨幅>9.5%（买不进去）
        open_price = _safe_float(f.get("open", 0))
        high_price = _safe_float(f.get("high", 0))
        low_price = _safe_float(f.get("low", 0))
        if (close > 0 and open_price > 0 and high_price > 0 and low_price > 0
                and abs(open_price - close) / close < 0.001
                and abs(high_price - close) / close < 0.001
                and abs(low_price - close) / close < 0.001
                and pct_change > 9.5):
            continue

        # 简单评分：成交额归一化 + 涨幅 + 流动性
        amount_score = min(amount / 1e9, 1.0)  # 10 亿封顶
        momentum_score = max(min(pct_change / 10.0, 1.0), -1.0)  # ±10% 封顶
        composite = amount_score * 0.5 + (momentum_score + 1) / 2 * 0.3 + 0.2  # 基础分

        candidates.append({
            "stock_code": _symbol_to_xt(symbol),
            "name": name,
            "trade_date": date_str,
            "close": round(close, 2),
            "pct_change": round(pct_change, 2),
            "volume": int(volume),
            "amount": round(amount, 0),
            "vwap": round(_safe_float(f.get("vwap", close)), 2),
            "up_stop_price": round(up_stop, 2) if up_stop > 0 else None,
            "down_stop_price": round(down_stop, 2) if down_stop > 0 else None,
            "composite_score": round(composite, 3),
        })

    # 5. 按综合分排序，取 TOP
    candidates.sort(key=lambda x: x["composite_score"], reverse=True)
    result = candidates[:pool_size]

    logger.info(f"候选池: {len(features)} 只原始 → {len(candidates)} 只筛选后 → {len(result)} 只候选")
    return result


def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _normalize_trade_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _symbol_to_xt(symbol: str) -> str:
    """qlibrd symbol → xtquant 格式 (如 600519.SH)。

    支持三种输入格式：
      - SH600519 / sh600519  (前缀)
      - 600519_SH / 600519_sh (后缀下划线)
      - 600519.SH             (已正确格式)
    """
    if "." in symbol:
        return symbol
    if symbol.startswith("SH") or symbol.startswith("sh"):
        return symbol[2:] + ".SH"
    if symbol.startswith("SZ") or symbol.startswith("sz"):
        return symbol[2:] + ".SZ"
    # 后缀下划线格式: 600519_SH
    if "_" in symbol:
        code, suffix = symbol.rsplit("_", 1)
        return f"{code}.{suffix.upper()}"
    return symbol
