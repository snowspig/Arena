"""Arena portfolio order placement for simulation accounts."""

from __future__ import annotations

from contextlib import suppress
from datetime import date, datetime, timedelta
from typing import Any

from loguru import logger
from pymongo import MongoClient

from .config import get_config, get_mongo_db, get_mongo_uri
from .models import SignalDirection, TimingType, TradeSignal
from .qmt_engine import _ensure_qmt_path

_DEFAULT_CAPITAL_POOL = 5_000_000
_SELL_DISCOUNT = 0.005
_FEATURES_COLLECTION = "cn_data_stock_features"


def get_limit_rate(stock_code: str) -> float:
    """Return the board-specific daily price limit rate."""
    if stock_code.startswith(("300", "688")):
        return 0.20
    if stock_code.endswith(".BJ"):
        return 0.30
    return 0.10


def calc_limit_up_price(close: float, stock_code: str) -> float:
    """Calculate the rounded limit-up price."""
    return round(close * (1 + get_limit_rate(stock_code)), 2)


def calc_limit_down_price(close: float, stock_code: str) -> float:
    """Calculate the rounded limit-down price."""
    return round(close * (1 - get_limit_rate(stock_code)), 2)


def calc_sell_price(current_price: float) -> float:
    """Calculate the discounted sell price."""
    return round(current_price * (1 - _SELL_DISCOUNT), 2)


def get_enabled_providers() -> list[str]:
    """Read enabled arena providers from config."""
    cfg = get_config()
    providers_cfg = cfg.get("arena", {}).get("providers", {})
    return [
        name
        for name, provider_cfg in providers_cfg.items()
        if provider_cfg.get("enabled", True)
    ]


def get_capital_pool(provider: str) -> float:
    """Read provider capital pool from config."""
    cfg = get_config()
    provider_cfg = cfg.get("arena", {}).get("providers", {}).get(provider, {})
    return float(provider_cfg.get("capital_pool", _DEFAULT_CAPITAL_POOL))


def _create_qlibrd_client() -> MongoClient:
    """Create a MongoDB client for qlibrd market data."""
    uri = get_mongo_uri()
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def _to_qlibrd_code(stock_code: str) -> str:
    """Convert xt-style stock code to qlibrd stock code."""
    code, exchange = stock_code.split(".")
    return f"{exchange}{code}"


def _date_filters(target_date: date) -> list[Any]:
    """Build compatible date filters for Mongo queries."""
    return [
        target_date.isoformat(),
        target_date,
        datetime.combine(target_date, datetime.min.time()),
    ]


def _batch_get_prev_close_from_mongo(
    stock_codes: list[str],
    trade_date: date,
) -> dict[str, float]:
    """Read previous closes from MongoDB market features."""
    if not stock_codes:
        return {}

    target_date = trade_date - timedelta(days=1)
    try:
        client = _create_qlibrd_client()
        collection = client[get_mongo_db("qlibrd_db")][_FEATURES_COLLECTION]
        docs = collection.find(
            {
                "stock_code": {"$in": [_to_qlibrd_code(code) for code in stock_codes]},
                "date": {"$in": _date_filters(target_date)},
            },
            {"stock_code": 1, "close": 1, "date": 1},
        )
        result: dict[str, float] = {}
        for doc in docs:
            qlibrd_code = str(doc.get("stock_code", ""))
            close = float(doc.get("close", 0.0) or 0.0)
            if len(qlibrd_code) < 3 or close <= 0:
                continue
            result[f"{qlibrd_code[2:]}.{qlibrd_code[:2]}"] = close
        return result
    except Exception as exc:
        logger.warning(f"批量从 MongoDB 获取昨收价失败: {exc}")
        return {}


def _batch_get_prev_close_from_xtdata(stock_codes: list[str]) -> dict[str, float]:
    """Read previous closes from xtdata when Mongo data is missing."""
    del stock_codes
    return {}


def _get_prev_close_from_xtdata(stock_code: str, trade_date: date | None = None) -> float:
    """Read previous close from xtdata when Mongo data is missing."""
    del stock_code, trade_date
    return 0.0


def load_arena_account(provider: str) -> dict[str, Any] | None:
    """Load a provider arena account document."""
    collection = _get_mongo_collection("arena_accounts")
    return collection.find_one({"provider": provider})


def ensure_arena_accounts() -> None:
    """Ensure all enabled providers have arena_accounts documents."""
    collection = _get_mongo_collection("arena_accounts")
    for provider in get_enabled_providers():
        existing = collection.find_one({"provider": provider})
        if existing:
            continue
        initial_capital = get_capital_pool(provider)
        collection.insert_one(
            {
                "provider": provider,
                "initial_capital": initial_capital,
                "cash": initial_capital,
                "positions": {},
                "total_asset": initial_capital,
                "daily_return_pct": 0.0,
                "cumulative_return_pct": 0.0,
                "updated_at": datetime.now().isoformat(),
            }
        )
        logger.info(
            f"[{provider}] 创建初始 arena_account: capital={initial_capital:,.0f}"
        )


def load_today_signals(provider: str, trade_date: date) -> list[dict[str, Any]]:
    """Load a provider's signals for the trade date."""
    collection = _get_mongo_collection("arena_signals")
    document = collection.find_one({
        "trade_date": trade_date.isoformat(),
        "provider": provider,
    })
    if not document:
        return []
    return list(document.get("signals", []))


def place_buy_orders(engine: Any, trade_date: date) -> dict[str, int]:
    """Place limit-up buy orders for all enabled providers."""
    ensure_arena_accounts()
    results: dict[str, int] = {}
    for provider in get_enabled_providers():
        signals = load_today_signals(provider, trade_date)
        account = load_arena_account(provider)
        if not signals or not account:
            results[provider] = 0
            continue
        results[provider] = _place_provider_buy_orders(engine, provider, trade_date, signals)
    return results


def place_sell_orders(engine: Any, trade_date: date) -> dict[str, int]:
    """Place discounted sell orders for all enabled providers."""
    ensure_arena_accounts()
    results: dict[str, int] = {}
    for provider in get_enabled_providers():
        account = load_arena_account(provider)
        positions = (account or {}).get("positions", {})
        if not positions:
            results[provider] = 0
            continue
        results[provider] = _place_provider_sell_orders(engine, provider, trade_date, positions)
    return results


def _place_provider_buy_orders(
    engine: Any,
    provider: str,
    trade_date: date,
    signals: list[dict[str, Any]],
) -> int:
    """Place buy orders for one provider."""
    order_count = 0
    logger.info(f"[{provider}] 开始买入, 信号数={len(signals)}")
    for idx, signal_data in enumerate(signals, start=1):
        stock_code = signal_data.get("stock_code", "?")
        close_price = float(signal_data.get("price", 0.0))
        volume = signal_data.get("volume", 0)
        if close_price <= 0:
            logger.warning(f"[{provider}] {idx}/{len(signals)} 跳过 {stock_code}: 价格无效 (price={close_price})")
            continue
        order_signal = _build_buy_signal(provider, trade_date, signal_data, close_price)
        logger.info(
            f"[{provider}] {idx}/{len(signals)} 买入 {stock_code} "
            f"volume={volume} price={close_price:.2f}"
        )
        record = engine.place_order(order_signal)
        status = getattr(record, "status", "unknown")
        order_id = getattr(record, "order_id", None)
        error = getattr(record, "error_msg", "")
        if status == "submitted":
            order_count += 1
            logger.info(f"[{provider}] {stock_code} 下单成功 order_id={order_id}")
        else:
            logger.error(f"[{provider}] {stock_code} 下单失败 status={status} error={error}")
    logger.info(f"[{provider}] 买入完成: 成功={order_count}/{len(signals)}")
    return order_count


def _place_provider_sell_orders(
    engine: Any,
    provider: str,
    trade_date: date,
    positions: dict[str, Any],
) -> int:
    """Place sell orders for one provider."""
    order_count = 0
    total_positions = len(positions)
    logger.info(f"[{provider}] 开始卖出, 持仓数={total_positions}")
    for stock_code, position_data in positions.items():
        volume = _extract_position_volume(position_data)
        if volume <= 0:
            logger.info(f"[{provider}] {stock_code} 持仓为0, 跳过")
            continue
        current_price = _get_current_price(stock_code)
        if current_price <= 0:
            logger.warning(f"[{provider}] {stock_code} 获取现价失败, 跳过卖单")
            continue
        logger.info(
            f"[{provider}] 卖出 {stock_code} "
            f"volume={volume} price={current_price:.2f}"
        )
        order_signal = _build_sell_signal(
            provider,
            trade_date,
            stock_code,
            volume,
            current_price,
        )
        record = engine.place_order(order_signal)
        status = getattr(record, "status", "unknown")
        order_id = getattr(record, "order_id", None)
        error = getattr(record, "error_msg", "")
        if status == "submitted":
            order_count += 1
            logger.info(f"[{provider}] {stock_code} 卖单成功 order_id={order_id}")
        else:
            logger.error(f"[{provider}] {stock_code} 卖单失败 status={status} error={error}")
    logger.info(f"[{provider}] 卖出完成: 成功={order_count}/{total_positions}")
    return order_count


def _build_buy_signal(
    provider: str,
    trade_date: date,
    signal_data: dict[str, Any],
    close_price: float,
) -> TradeSignal:
    """Build a buy order signal from stored arena signal data."""
    stock_code = str(signal_data["stock_code"])
    return TradeSignal(
        signal_id=f"arena-buy-{provider}-{trade_date.isoformat()}-{stock_code}",
        stock_code=stock_code,
        direction=SignalDirection.BUY,
        volume=int(signal_data["volume"]),
        price=calc_limit_up_price(close_price, stock_code),
        timing=TimingType.OPEN_AUCTION,
        signal_date=trade_date,
        reason=str(signal_data.get("reason", "arena_buy")),
    )


def _build_sell_signal(
    provider: str,
    trade_date: date,
    stock_code: str,
    volume: int,
    current_price: float,
) -> TradeSignal:
    """Build a sell order signal from current positions."""
    return TradeSignal(
        signal_id=f"arena-sell-{provider}-{trade_date.isoformat()}-{stock_code}",
        stock_code=stock_code,
        direction=SignalDirection.SELL,
        volume=volume,
        price=calc_sell_price(current_price),
        timing=TimingType.CLOSE,
        signal_date=trade_date,
        reason="arena_sell",
    )


def _extract_position_volume(position_data: Any) -> int:
    """Extract position volume from stored position payload."""
    if isinstance(position_data, dict):
        for key in ("volume", "can_use_volume", "available_volume"):
            if key in position_data:
                return int(position_data[key])
        return 0
    with suppress(TypeError, ValueError):
        return int(position_data)
    return 0


def _get_current_price(stock_code: str) -> float:
    """Try reading the latest price from xtdata."""
    try:
        _ensure_qmt_path("simulation")
        from xtquant import xtdata

        latest = xtdata.get_full_tick([stock_code])
        if not latest:
            return 0.0
        stock_data = latest.get(stock_code, {})
        for key in ("lastPrice", "last_price", "price"):
            price = stock_data.get(key)
            if price:
                return float(price)
    except Exception as exc:
        logger.warning(f"读取最新价失败: {stock_code} error={exc}")
    return 0.0


def _get_prev_close(stock_code: str, trade_date: date | None = None) -> float:
    """Read previous close price with Mongo-first fallback order."""
    if trade_date is None:
        trade_date = date.today()

    target_date = trade_date - timedelta(days=1)
    mongo_result = _batch_get_prev_close_from_mongo([stock_code], trade_date)
    if stock_code in mongo_result:
        return mongo_result[stock_code]

    xt_price = _get_prev_close_from_xtdata(stock_code, trade_date)
    if xt_price > 0:
        return xt_price

    try:
        client = _create_qlibrd_client()
        collection = client[get_mongo_db("qlibrd_db")][_FEATURES_COLLECTION]
        doc = collection.find_one(
            {
                "stock_code": _to_qlibrd_code(stock_code),
                "date": {"$in": _date_filters(target_date)},
            },
            {"close": 1},
            sort=[("date", -1)],
        )
        if doc and doc.get("close"):
            return float(doc["close"])
    except Exception as exc:
        logger.debug(f"MongoDB prev_close 读取失败: {exc}")

    return 0.0


def _batch_get_prev_close(
    stock_codes: list[str],
    trade_date: date | None = None,
) -> dict[str, float]:
    """Batch-read previous closes with Mongo-first fallback order."""
    if not stock_codes:
        return {}
    if trade_date is None:
        trade_date = date.today()

    mongo_prices = _batch_get_prev_close_from_mongo(stock_codes, trade_date)
    missing_codes = [code for code in stock_codes if code not in mongo_prices]
    xt_prices = _batch_get_prev_close_from_xtdata(missing_codes)
    merged_prices = dict(mongo_prices)
    merged_prices.update(xt_prices)
    return merged_prices


def _batch_get_current_price(stock_codes: list[str]) -> dict[str, float]:
    """批量获取多只股票的最新价。"""
    if not stock_codes:
        return {}
    result: dict[str, float] = {}
    try:
        _ensure_qmt_path("simulation")
        from xtquant import xtdata

        latest = xtdata.get_full_tick(stock_codes)
        if not latest:
            return {}
        for stock_code in stock_codes:
            stock_data = latest.get(stock_code, {})
            for key in ("lastPrice", "last_price", "price"):
                price = stock_data.get(key)
                if price:
                    result[stock_code] = float(price)
                    break
    except Exception as exc:
        logger.warning(f"批量获取最新价失败: {exc}")
    return result


def _is_at_limit_up_cached(
    stock_code: str,
    latest_price: float,
    prev_close: float,
) -> bool:
    """Use pre-fetched market data to detect limit-up quickly."""
    if latest_price <= 0:
        logger.warning(f"涨停检测: {stock_code} 无法获取最新价，默认不卖")
        return False
    if prev_close <= 0:
        logger.warning(f"涨停检测: {stock_code} 无法获取昨收价，默认不卖")
        return False
    limit_up = calc_limit_up_price(prev_close, stock_code)
    result = latest_price >= limit_up - 0.01
    logger.info(
        f"涨停检测: {stock_code} latest={latest_price:.2f} "
        f"limit_up={limit_up:.2f} at_limit={result}"
    )
    return result


def is_at_limit_up(
    stock_code: str,
    latest_price: float = 0.0,
    trade_date: date | None = None,
) -> bool:
    """Check if stock is at or above limit-up price."""
    if latest_price <= 0:
        latest_price = _get_current_price(stock_code)
    if latest_price <= 0:
        logger.warning(f"涨停检测: {stock_code} 无法获取最新价，默认不卖")
        return False
    prev_close = _get_prev_close(stock_code, trade_date)
    if prev_close <= 0:
        logger.warning(f"涨停检测: {stock_code} 无法获取昨收价，默认不卖")
        return False
    limit_up = calc_limit_up_price(prev_close, stock_code)
    result = latest_price >= limit_up - 0.01
    logger.info(f"涨停检测: {stock_code} latest={latest_price:.2f} limit_up={limit_up:.2f} at_limit={result}")
    return result


def continuous_auction_sell(engine: Any, trade_date: date) -> int:
    """Phase 1: Sell all non-limit-up positions at latest price."""
    positions = engine.query_positions() or []
    order_count = 0
    logger.info(f"Phase 1 查到 {len(positions)} 个持仓")

    # Batch-fetch prev_close for all positions upfront to avoid per-stock delay
    stock_codes = [str(getattr(p, "stock_code", "")) for p in positions]
    prev_close_map = _batch_get_prev_close(stock_codes, trade_date)
    latest_price_map = _batch_get_current_price(stock_codes)
    logger.info(
        f"Phase 1 批量行情完成: prev_close={len(prev_close_map)}/{len(stock_codes)}, "
        f"latest={len(latest_price_map)}/{len(stock_codes)}"
    )

    for pos in positions:
        volume = int(getattr(pos, "can_use_volume", 0) or 0)
        total_vol = int(getattr(pos, "volume", 0) or 0)
        stock_code = str(getattr(pos, "stock_code", ""))
        logger.info(f"Phase 1 检查 {stock_code}: can_use={volume}, total={total_vol}")
        if volume <= 0:
            continue
        latest_price = latest_price_map.get(stock_code, 0.0)
        prev_close = prev_close_map.get(stock_code, 0.0)
        if _is_at_limit_up_cached(stock_code, latest_price, prev_close):
            logger.info(f"涨停不卖: {stock_code}")
            continue
        signal = TradeSignal(
            signal_id=f"sell-{trade_date.isoformat()}-{stock_code}",
            stock_code=stock_code,
            direction=SignalDirection.SELL,
            volume=volume,
            price=0.0,
            timing=TimingType.CLOSE,
            signal_date=trade_date,
            reason="arena_continuous_sell",
        )
        record = engine.place_order(signal)
        if getattr(record, "order_id", 0) > 0:
            order_count += 1
            logger.info(f"连续竞价卖出: {stock_code} volume={volume}")
    logger.info(f"Phase 1 连续竞价卖出完成: {order_count} 笔")
    return order_count


def cancel_unfilled_sells(engine: Any) -> int:
    """Phase 2: Cancel unfilled sell orders."""
    orders = engine.query_orders() or []
    cancelled = 0
    for order in orders:
        order_type = int(getattr(order, "order_type", 0))
        if order_type != 24:
            continue
        traded = int(getattr(order, "traded_volume", 0) or 0)
        total = int(getattr(order, "order_volume", 0) or 0)
        if traded >= total:
            continue
        order_id = int(getattr(order, "order_id", 0))
        if order_id > 0:
            engine.cancel_order(order_id)
            cancelled += 1
            logger.info(f"撤单: order_id={order_id}")
    logger.info(f"Phase 2 撤单完成: {cancelled} 笔")
    return cancelled


def closing_auction_sell(engine: Any, trade_date: date) -> int:
    """Phase 3: Sell remaining positions at limit-down price."""
    positions = engine.query_positions() or []
    order_count = 0
    logger.info(f"Phase 3 查到 {len(positions)} 个持仓")

    stock_codes = [str(getattr(p, "stock_code", "")) for p in positions]
    prev_close_map = _batch_get_prev_close(stock_codes, trade_date)
    latest_price_map = _batch_get_current_price(stock_codes)
    logger.info(
        f"Phase 3 批量行情完成: prev_close={len(prev_close_map)}/{len(stock_codes)}, "
        f"latest={len(latest_price_map)}/{len(stock_codes)}"
    )

    for pos in positions:
        volume = int(getattr(pos, "can_use_volume", 0) or 0)
        total_vol = int(getattr(pos, "volume", 0) or 0)
        stock_code = str(getattr(pos, "stock_code", ""))
        logger.info(f"Phase 3 检查 {stock_code}: can_use={volume}, total={total_vol}")
        if volume <= 0:
            continue
        latest_price = latest_price_map.get(stock_code, 0.0)
        prev_close = prev_close_map.get(stock_code, 0.0)
        if _is_at_limit_up_cached(stock_code, latest_price, prev_close):
            logger.info(f"涨停不卖: {stock_code}")
            continue
        if prev_close <= 0:
            logger.warning(
                f"收盘竞价卖出跳过 {stock_code}: 无法获取昨收价 "
                f"(latest={latest_price:.2f})"
            )
            continue
        limit_down = calc_limit_down_price(prev_close, stock_code)
        signal = TradeSignal(
            signal_id=f"sell-auction-{trade_date.isoformat()}-{stock_code}",
            stock_code=stock_code,
            direction=SignalDirection.SELL,
            volume=volume,
            price=limit_down,
            timing=TimingType.CLOSE,
            signal_date=trade_date,
            reason="arena_closing_auction_sell",
        )
        record = engine.place_order(signal)
        if getattr(record, "order_id", 0) > 0:
            order_count += 1
            logger.info(f"收盘竞价卖出: {stock_code} volume={volume} price={limit_down}")
    logger.info(f"Phase 3 收盘竞价卖出完成: {order_count} 笔")
    return order_count


def _get_mongo_collection(name: str):
    """Create a MongoDB collection handle from config."""
    uri = get_mongo_uri()
    db_name = get_mongo_db()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client[db_name][name]
