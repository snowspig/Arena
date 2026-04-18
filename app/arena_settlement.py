"""Arena settlement using QMT fills and MongoDB snapshots."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from loguru import logger
from pymongo import DESCENDING, MongoClient

from .arena_portfolio import get_enabled_providers
from .config import get_config, get_mongo_db, get_mongo_uri
from .models import SignalDirection




def settle_all_agents(trade_date: date, engine: Any = None) -> dict[str, Any]:
    """Settle all enabled arena providers for one trade date."""
    providers = get_enabled_providers()
    all_signals = _load_all_provider_signals(trade_date, providers)
    per_provider_signal_map = {
        p: _build_provider_signal_map(all_signals.get(p, []))
        for p in providers
    }
    aggregate_signal_map = _build_aggregate_signal_map_from_maps(per_provider_signal_map)
    # Use real QMT fills as the source of truth
    real_buy_fills = _get_real_fills_by_stock(engine)
    real_sell_fills = _aggregate_sell_fills_by_stock(engine) if engine else {}
    logger.info(f"QMT 买入汇总: {len(real_buy_fills)} 只股票")
    logger.info(f"QMT 卖出汇总: {len(real_sell_fills)} 只股票")
    buy_allocation = _allocate_all_from_real_fills(
        real_buy_fills, per_provider_signal_map, aggregate_signal_map,
    )
    # Load current holdings for sell allocation
    provider_holdings = _load_provider_holdings(providers)
    sell_allocation = _allocate_sell_fills_by_holdings(real_sell_fills, provider_holdings)
    # Merge real fills for market_value calculation
    real_fills_by_stock = {**real_buy_fills}
    for code, fill in real_sell_fills.items():
        if code not in real_fills_by_stock:
            real_fills_by_stock[code] = fill
    results = [
        settle_single_agent(
            provider, trade_date, engine,
            buy_allocation.get(provider, {}),
            all_signals.get(provider, []),
            sell_allocation.get(provider, {}),
            real_fills_by_stock,
        )
        for provider in providers
    ]
    rankings = sorted(
        results,
        key=lambda item: item["cumulative_return_pct"],
        reverse=True,
    )
    for index, item in enumerate(rankings, start=1):
        item["rank"] = index
    return {"trade_date": trade_date.isoformat(), "rankings": rankings}


def settle_single_agent(
    provider: str,
    trade_date: date,
    engine: Any = None,
    allocated_buy_positions: dict[str, dict[str, Any]] | None = None,
    signals: list[dict[str, Any]] | None = None,
    allocated_sell_positions: dict[str, dict[str, Any]] | None = None,
    real_fills_by_stock: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Settle one arena provider using incremental state updates."""
    collections = _get_collections()
    account = _load_or_restore_account(collections, provider)
    if signals is None:
        signals = _load_signals(collections["signals"], provider, trade_date)
    if allocated_buy_positions is None:
        allocated_buy_positions = _allocate_provider_positions(
            trade_date, provider, signals, engine,
        )
    if allocated_sell_positions is None:
        allocated_sell_positions = {}
    buy_fills = _build_allocated_buy_fills(provider, signals, allocated_buy_positions)
    sell_fills = _build_allocated_sell_fills(provider, allocated_sell_positions)
    cash, positions, trade_stats = _apply_fills_incremental(account, buy_fills, sell_fills)
    market_value = _calc_allocated_market_value(positions, real_fills_by_stock or {})
    metrics = _calculate_metrics(account, cash, market_value, trade_date, provider)
    all_fills = buy_fills + sell_fills
    _persist_settlement(
        collections,
        provider,
        trade_date,
        cash,
        positions,
        all_fills,
        trade_stats,
        metrics,
    )
    result = _build_result(provider, trade_date, all_fills, positions, cash, market_value, metrics)
    logger.info(
        f"Arena 结算: {provider} "
        f"cash={cash:,.0f} mv={market_value:,.0f} asset={metrics['total_asset']:,.0f} "
        f"日收益={metrics['daily_return_pct']:+.2f}% "
        f"累计={metrics['cumulative_return_pct']:+.2f}%"
    )
    return result


def _get_real_fills_by_stock(engine: Any) -> dict[str, dict[str, Any]]:
    """Query QMT trades (or positions as fallback) and aggregate by stock code."""
    if engine is None:
        return {}
    # Try trades first (available during trading hours)
    result = _aggregate_from_trades(engine)
    if result:
        logger.info(f"从 QMT 成交记录获取: {len(result)} 只股票")
        return result
    # Fallback: use positions (available after market close)
    result = _aggregate_from_positions(engine)
    if result:
        logger.info(f"从 QMT 持仓获取: {len(result)} 只股票")
    return result


def _aggregate_from_trades(engine: Any) -> dict[str, dict[str, Any]]:
    """Aggregate QMT trades by stock code (buy only, VWAP price)."""
    try:
        trades = engine.query_trades() or []
    except Exception as exc:
        logger.warning(f"查询 QMT 成交失败: {exc}")
        return {}
    if not trades:
        return {}
    buy_fills: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        direction = _normalize_direction(getattr(trade, "traded_type", ""))
        if direction != SignalDirection.BUY.value:
            continue
        stock_code = str(getattr(trade, "stock_code", ""))
        price = float(getattr(trade, "traded_price", 0.0) or 0.0)
        volume = int(getattr(trade, "traded_volume", 0) or 0)
        if not stock_code or volume <= 0 or price <= 0:
            continue
        buy_fills.setdefault(stock_code, []).append({"price": price, "volume": volume})
    result: dict[str, dict[str, Any]] = {}
    for stock_code, fills in buy_fills.items():
        total_volume = sum(f["volume"] for f in fills)
        total_amount = sum(f["price"] * f["volume"] for f in fills)
        avg_price = total_amount / total_volume if total_volume > 0 else 0.0
        result[stock_code] = {"volume": total_volume, "avg_price": avg_price}
    return result


def _aggregate_sell_fills_by_stock(engine: Any) -> dict[str, dict[str, Any]]:
    """Aggregate QMT sell trades by stock code (VWAP price)."""
    try:
        trades = engine.query_trades() or []
    except Exception as exc:
        logger.warning(f"查询 QMT 成交失败: {exc}")
        return {}
    if not trades:
        return {}
    sell_fills: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        direction = _normalize_direction(getattr(trade, "traded_type", ""))
        if direction != SignalDirection.SELL.value:
            continue
        stock_code = str(getattr(trade, "stock_code", ""))
        price = float(getattr(trade, "traded_price", 0.0) or 0.0)
        volume = int(getattr(trade, "traded_volume", 0) or 0)
        if not stock_code or volume <= 0 or price <= 0:
            continue
        sell_fills.setdefault(stock_code, []).append({"price": price, "volume": volume})
    result: dict[str, dict[str, Any]] = {}
    for stock_code, fills in sell_fills.items():
        total_volume = sum(f["volume"] for f in fills)
        total_amount = sum(f["price"] * f["volume"] for f in fills)
        avg_price = total_amount / total_volume if total_volume > 0 else 0.0
        result[stock_code] = {"volume": total_volume, "avg_price": avg_price}
    return result


def _allocate_sell_fills_by_holdings(
    sell_fills_by_stock: dict[str, dict[str, Any]],
    provider_holdings: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Allocate real QMT sell fills across providers by current holding proportion."""
    allocation: dict[str, dict[str, dict[str, Any]]] = {
        provider: {} for provider in provider_holdings
    }
    for stock_code, sell_fill in sell_fills_by_stock.items():
        sell_volume = int(sell_fill["volume"])
        sell_price = float(sell_fill["avg_price"])
        # Gather current holdings for this stock
        holdings: list[tuple[str, int]] = []
        for provider, positions in provider_holdings.items():
            held = int(positions.get(stock_code, {}).get("volume", 0) or 0)
            if held > 0:
                holdings.append((provider, held))
        if not holdings or sell_volume <= 0:
            continue
        total_held = sum(held for _, held in holdings)
        allocatable = min(sell_volume, total_held)
        # Allocate by holding proportion using largest remainders
        provider_map = {provider: {stock_code: held} for provider, held in holdings}
        stock_allocations = _allocate_stock_volume(
            stock_code, allocatable, total_held, provider_map,
        )
        for provider, volume in stock_allocations.items():
            if volume <= 0:
                continue
            allocation[provider][stock_code] = {
                "volume": volume,
                "avg_price": sell_price,
            }
    return allocation


def _aggregate_from_positions(engine: Any) -> dict[str, dict[str, Any]]:
    """Use QMT positions as fallback when trades are unavailable."""
    try:
        positions = engine.query_positions() or []
    except Exception as exc:
        logger.warning(f"查询 QMT 持仓失败: {exc}")
        return {}
    result: dict[str, dict[str, Any]] = {}
    for pos in positions:
        stock_code = str(getattr(pos, "stock_code", ""))
        volume = int(getattr(pos, "volume", 0) or 0)
        if not stock_code or volume <= 0:
            continue
        open_price = float(getattr(pos, "open_price", 0.0) or 0.0)
        market_value = float(getattr(pos, "market_value", 0.0) or 0.0)
        result[stock_code] = {
            "volume": volume,
            "avg_price": open_price,
            "market_value": market_value,
        }
    return result


def _allocate_all_from_real_fills(
    real_fills_by_stock: dict[str, dict[str, Any]],
    per_provider_signal_map: dict[str, dict[str, int]],
    aggregate_signal_map: dict[str, int],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Allocate real QMT fills across providers by signal proportion."""
    allocation: dict[str, dict[str, dict[str, Any]]] = {
        provider: {} for provider in per_provider_signal_map
    }
    for stock_code, real_fill in real_fills_by_stock.items():
        real_volume = int(real_fill["volume"])
        real_price = float(real_fill["avg_price"])
        total_signal_volume = int(aggregate_signal_map.get(stock_code, 0) or 0)
        if real_volume <= 0 or total_signal_volume <= 0:
            continue
        allocatable_volume = min(real_volume, total_signal_volume)
        stock_allocations = _allocate_stock_volume(
            stock_code,
            allocatable_volume,
            total_signal_volume,
            per_provider_signal_map,
        )
        for provider, volume in stock_allocations.items():
            if volume <= 0:
                continue
            allocation[provider][stock_code] = {
                "volume": volume,
                "avg_price": real_price,
            }
    return allocation


def _load_all_provider_signals(
    trade_date: date,
    providers: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Load all provider signals for one trade date."""
    collections = _get_collections()
    return {
        provider: _load_signals(collections["signals"], provider, trade_date)
        for provider in providers
    }


def _build_aggregate_signal_map_from_maps(
    per_provider_signal_map: dict[str, dict[str, int]],
) -> dict[str, int]:
    """Aggregate all providers' signal volume by stock code."""
    aggregate: dict[str, int] = {}
    for provider_map in per_provider_signal_map.values():
        for stock_code, volume in provider_map.items():
            aggregate[stock_code] = aggregate.get(stock_code, 0) + volume
    return aggregate


def _allocate_all_positions(
    shared_positions: dict[str, dict[str, Any]],
    per_provider_signal_map: dict[str, dict[str, int]],
    aggregate_signal_map: dict[str, int],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Allocate shared positions across all providers, including remainder shares."""
    allocation: dict[str, dict[str, dict[str, Any]]] = {
        provider: {} for provider in per_provider_signal_map
    }
    for stock_code, total_signal_volume in aggregate_signal_map.items():
        shared_volume = int(shared_positions.get(stock_code, {}).get("volume", 0) or 0)
        if shared_volume <= 0 or total_signal_volume <= 0:
            continue
        allocatable_volume = min(shared_volume, total_signal_volume)
        stock_allocations = _allocate_stock_volume(
            stock_code,
            allocatable_volume,
            total_signal_volume,
            per_provider_signal_map,
        )
        for provider, volume in stock_allocations.items():
            if volume <= 0:
                continue
            real_avg_price = float(
                shared_positions.get(stock_code, {}).get("avg_price", 0.0) or 0.0
            )
            allocation[provider][stock_code] = {
                "volume": volume,
                "avg_price": real_avg_price,
            }
    return allocation


def _allocate_stock_volume(
    stock_code: str,
    allocatable_volume: int,
    total_signal_volume: int,
    per_provider_signal_map: dict[str, dict[str, int]],
) -> dict[str, int]:
    """Allocate one stock's filled volume using largest remainders.

    Never allocates more than a provider's signal volume for that stock.
    """
    if total_signal_volume <= 0 or allocatable_volume <= 0:
        return {provider: 0 for provider in per_provider_signal_map}
    result: dict[str, int] = {}
    raw_allocations: list[tuple[str, int, float]] = []
    allocated_total = 0
    for provider, provider_map in per_provider_signal_map.items():
        signal_volume = int(provider_map.get(stock_code, 0) or 0)
        if signal_volume <= 0:
            continue
        raw_volume = allocatable_volume * signal_volume / total_signal_volume
        base_volume = int(raw_volume)
        raw_allocations.append((provider, base_volume, raw_volume - base_volume))
        allocated_total += base_volume
    result = {provider: volume for provider, volume, _ in raw_allocations}
    remaining = allocatable_volume - allocated_total
    for provider, _, _ in sorted(raw_allocations, key=lambda item: item[2], reverse=True):
        if remaining <= 0:
            break
        result[provider] = result.get(provider, 0) + 1
        remaining -= 1
    # Cap: no provider gets more than their signal volume
    for provider, provider_map in per_provider_signal_map.items():
        signal_volume = int(provider_map.get(stock_code, 0) or 0)
        if signal_volume > 0 and result.get(provider, 0) > signal_volume:
            result[provider] = signal_volume
    logger.debug(
        f"分摊 {stock_code}: 可分={allocatable_volume} 信号={total_signal_volume} "
        f"结果={result} 总分配={sum(result.values())}"
    )
    return result


def _calc_allocated_market_value(
    positions: dict[str, dict[str, Any]],
    real_fills_by_stock: dict[str, dict[str, Any]],
) -> float:
    """Calculate market value for allocated positions using QMT real market_value.

    Proportion: agent's allocated volume / total real volume * total market_value.
    """
    total_mv = 0.0
    for stock_code, position in positions.items():
        volume = int(position.get("volume", 0) or 0)
        if volume <= 0:
            continue
        real_fill = real_fills_by_stock.get(stock_code, {})
        real_volume = int(real_fill.get("volume", 0) or 0)
        real_mv = float(real_fill.get("market_value", 0.0) or 0.0)
        if real_volume > 0 and real_mv > 0:
            total_mv += real_mv * volume / real_volume
    return total_mv


def _build_allocated_buy_fills(
    provider: str,
    signals: list[dict[str, Any]],
    allocated_positions: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build buy fills based on allocated real positions for one provider."""
    fills: list[dict[str, Any]] = []
    for signal in signals:
        stock_code = str(signal.get("stock_code", ""))
        allocated = allocated_positions.get(stock_code, {})
        volume = int(allocated.get("volume", 0) or 0)
        if volume <= 0:
            continue
        avg_price = float(allocated.get("avg_price", 0.0) or 0.0)
        price = avg_price if avg_price > 0 else float(signal.get("price", 0.0) or 0.0)
        if price <= 0:
            continue
        fills.append({
            "stock_code": stock_code,
            "direction": SignalDirection.BUY.value,
            "price": price,
            "volume": volume,
            "provider": provider,
            "fill_source": "position_allocated",
            "signal_id": str(signal.get("signal_id", "")),
            "reason": str(signal.get("reason", "")),
        })
    return fills


def _apply_allocated_state(
    account: dict[str, Any],
    fills: list[dict[str, Any]],
    allocated_positions: dict[str, dict[str, Any]],
) -> tuple[float, dict[str, dict[str, Any]], dict[str, float]]:
    """Rebuild provider state from initial capital and allocated end-of-day positions."""
    initial_capital = float(account.get("initial_capital", 0.0) or 0.0)
    positions = _copy_positions(allocated_positions)
    trade_stats = _empty_trade_stats()
    cash = initial_capital
    for fill in fills:
        amount = float(fill["price"]) * int(fill["volume"])
        trade_stats["buy_count"] += 1
        trade_stats["total_buy_amount"] += amount
        cash -= amount
    return cash, positions, trade_stats


def _allocate_provider_positions(
    trade_date: date,
    provider: str,
    signals: list[dict[str, Any]],
    engine: Any,
) -> dict[str, dict[str, Any]]:
    """Allocate shared QMT positions back to one provider based on same-stock signal totals."""
    providers = get_enabled_providers()
    all_signals = _load_all_provider_signals(trade_date, providers)
    all_signals[provider] = signals
    per_provider_signal_map = {
        provider_name: _build_provider_signal_map(provider_signals)
        for provider_name, provider_signals in all_signals.items()
    }
    aggregate_signal_map = _build_aggregate_signal_map_from_maps(per_provider_signal_map)
    allocation_map = _allocate_all_positions(
        _get_shared_positions(engine),
        per_provider_signal_map,
        aggregate_signal_map,
    )
    allocated = allocation_map.get(provider, {})
    # Keep real avg_price from QMT open_price, only fill signal price as fallback
    for stock_code, position in allocated.items():
        if not position.get("avg_price") or float(position["avg_price"]) <= 0:
            position["avg_price"] = _resolve_signal_price(signals, stock_code)
    logger.info(f"[{provider}] 持仓分摊完成: {len(allocated)} 只")
    return allocated


def _build_provider_signal_map(signals: list[dict[str, Any]]) -> dict[str, int]:
    """Sum one provider's signaled volume by stock code."""
    signal_map: dict[str, int] = {}
    for signal in signals:
        if _normalize_direction(signal.get("direction")) != SignalDirection.BUY.value:
            continue
        stock_code = str(signal.get("stock_code", ""))
        volume = int(signal.get("volume", 0) or 0)
        if stock_code and volume > 0:
            signal_map[stock_code] = signal_map.get(stock_code, 0) + volume
    return signal_map


def _build_aggregate_signal_map(trade_date: date) -> dict[str, int]:
    """Sum all enabled providers' buy signal volume by stock code."""
    providers = get_enabled_providers()
    all_signals = _load_all_provider_signals(trade_date, providers)
    per_provider_signal_map = {
        provider: _build_provider_signal_map(signals)
        for provider, signals in all_signals.items()
    }
    return _build_aggregate_signal_map_from_maps(per_provider_signal_map)


def _get_shared_positions(engine: Any) -> dict[str, dict[str, Any]]:
    """Load actual shared QMT end-of-day positions."""
    if engine is None:
        return {}
    try:
        positions = engine.query_positions() or []
    except Exception as exc:
        logger.warning(f"读取 QMT 持仓失败: {exc}")
        return {}
    shared: dict[str, dict[str, Any]] = {}
    for pos in positions:
        stock_code = str(getattr(pos, "stock_code", ""))
        volume = int(getattr(pos, "volume", 0) or 0)
        if not stock_code or volume <= 0:
            continue
        shared[stock_code] = {
            "volume": volume,
            "avg_price": float(getattr(pos, "open_price", 0.0) or 0.0),
        }
    return shared



def _resolve_signal_price(signals: list[dict[str, Any]], stock_code: str) -> float:
    """Return the first signaled price for one stock code."""
    for signal in signals:
        if str(signal.get("stock_code", "")) == stock_code:
            return float(signal.get("price", 0.0) or 0.0)
    return 0.0


def _signals_to_fills(signals: list[dict[str, Any]], provider: str) -> list[dict[str, Any]]:
    """Convert stored arena signals into approximate fill records."""
    fills: list[dict[str, Any]] = []
    for signal in signals:
        price = float(signal.get("price", 0.0) or 0.0)
        volume = int(signal.get("volume", 0) or 0)
        if price <= 0 or volume <= 0:
            if price <= 0:
                logger.warning(f"[{provider}] 信号缺少有效价格，跳过近似成交: {signal}")
            continue
        fills.append({
            "stock_code": str(signal.get("stock_code", "")),
            "direction": _normalize_direction(signal.get("direction")),
            "price": price,
            "volume": volume,
            "provider": provider,
            "fill_source": "signal_approx",
            "signal_id": str(signal.get("signal_id", "")),
            "reason": str(signal.get("reason", "")),
        })
    return fills


def _get_close_prices(stock_codes: list[str], trade_date: date) -> dict[str, float]:
    """Load close prices from qlibrd.cn_data_stock_features."""
    prices = _get_market_prices(stock_codes, trade_date)
    return {code: data["close"] for code, data in prices.items()}


def _get_market_prices(
    stock_codes: list[str], trade_date: date,
) -> dict[str, dict[str, float]]:
    """Load open and close prices from qlibrd.cn_data_stock_features.

    Returns: {stock_code: {"open": float, "close": float}}
    """
    if not stock_codes:
        return {}
    client = _create_client()
    features_col = client[get_mongo_db("qlibrd_db")]["cn_data_stock_features"]
    qlibrd_codes = [_to_qlibrd_code(code) for code in stock_codes]
    cursor = features_col.find(
        {"date": trade_date.isoformat(), "stock_code": {"$in": qlibrd_codes}},
        {"_id": 0, "stock_code": 1, "open": 1, "close": 1},
    )
    price_by_qlibrd_code: dict[str, dict[str, float]] = {}
    for item in cursor:
        code = str(item.get("stock_code", ""))
        price_by_qlibrd_code[code] = {
            "open": float(item.get("open", 0.0) or 0.0),
            "close": float(item.get("close", 0.0) or 0.0),
        }
    # Fallback: try xtdata if MongoDB has no data for today
    missing = [
        code for code in stock_codes
        if not price_by_qlibrd_code.get(_to_qlibrd_code(code), {}).get("close")
    ]
    if missing:
        xt_prices = _get_prices_from_xtdata(missing, trade_date)
        for code, prices in xt_prices.items():
            qlibrd_code = _to_qlibrd_code(code)
            price_by_qlibrd_code[qlibrd_code] = prices
    return _map_market_prices(stock_codes, price_by_qlibrd_code)


def _get_prices_from_xtdata(
    stock_codes: list[str], trade_date: date,
) -> dict[str, dict[str, float]]:
    """Fallback: read open/close from xtdata kline."""
    try:
        from app.arena_portfolio import _ensure_qmt_path
        _ensure_qmt_path("simulation")
        from xtquant import xtdata

        df = xtdata.get_kline_data(
            stock_list=stock_codes, period="1d",
            start_time=trade_date.isoformat(),
            end_time=trade_date.isoformat(),
            count=1,
        )
        if df is None or df.empty:
            return {}
        result: dict[str, dict[str, float]] = {}
        for stock_code in stock_codes:
            row = df[df["stock_code"] == stock_code] if "stock_code" in df.columns else df
            if row.empty:
                continue
            result[stock_code] = {
                "open": float(row["open"].iloc[-1]) if "open" in row.columns else 0.0,
                "close": float(row["close"].iloc[-1]) if "close" in row.columns else 0.0,
            }
        return result
    except Exception as exc:
        logger.warning(f"xtdata 行情读取失败: {exc}")
        return {}


def _map_market_prices(
    stock_codes: list[str],
    price_by_qlibrd_code: dict[str, dict[str, float]],
) -> dict[str, dict[str, float]]:
    return {
        stock_code: price_by_qlibrd_code.get(_to_qlibrd_code(stock_code), {"open": 0.0, "close": 0.0})
        for stock_code in stock_codes
    }


def _map_close_prices(
    stock_codes: list[str],
    price_by_qlibrd_code: dict[str, float],
) -> dict[str, float]:
    return {
        stock_code: price_by_qlibrd_code.get(_to_qlibrd_code(stock_code), 0.0)
        for stock_code in stock_codes
    }


def _get_collections() -> dict[str, Any]:
    client = _create_client()
    database = client[_get_db_name()]
    return {
        "accounts": database["arena_accounts"],
        "signals": database["arena_signals"],
        "trades": database["arena_trades"],
        "snapshots": database["arena_daily_snapshots"],
    }


def _create_client() -> MongoClient:
    cfg = get_config()
    uri = get_mongo_uri()
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def _get_db_name() -> str:
    cfg = get_config()
    return get_mongo_db()


def _load_account(collection: Any, provider: str) -> dict[str, Any]:
    account = collection.find_one({"provider": provider})
    if account:
        return account
    raise ValueError(f"未找到 Arena 账户: {provider}")


def _restore_account_from_snapshot(
    provider: str,
    snapshot: dict[str, Any] | None,
    initial_capital: float,
) -> dict[str, Any]:
    """Restore account state from latest snapshot, or create initial account."""
    if snapshot:
        logger.warning(f"[{provider}] arena_accounts 缺失，从 snapshot 恢复")
        return {
            "provider": provider,
            "initial_capital": initial_capital,
            "cash": float(snapshot.get("cash", initial_capital) or initial_capital),
            "positions": snapshot.get("positions", {}),
            "total_asset": float(snapshot.get("total_asset", initial_capital) or initial_capital),
            "daily_return_pct": float(snapshot.get("daily_return_pct", 0.0) or 0.0),
            "cumulative_return_pct": float(snapshot.get("cumulative_return_pct", 0.0) or 0.0),
        }
    logger.warning(f"[{provider}] arena_accounts 与 snapshot 均缺失，创建初始账户")
    return {
        "provider": provider,
        "initial_capital": initial_capital,
        "cash": initial_capital,
        "positions": {},
        "total_asset": initial_capital,
        "daily_return_pct": 0.0,
        "cumulative_return_pct": 0.0,
    }


def _load_provider_holdings(
    providers: list[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Load current positions from arena_accounts for all providers."""
    collections = _get_collections()
    holdings: dict[str, dict[str, dict[str, Any]]] = {}
    for provider in providers:
        account = collections["accounts"].find_one({"provider": provider})
        if account and account.get("positions"):
            holdings[provider] = account["positions"]
        else:
            holdings[provider] = {}
    return holdings


def _load_or_restore_account(
    collections: dict[str, Any],
    provider: str,
) -> dict[str, Any]:
    """Load account from arena_accounts, or restore from snapshot if missing."""
    account = collections["accounts"].find_one({"provider": provider})
    if account:
        account.pop("_id", None)
        return account
    # Try to restore from latest snapshot
    snapshot = collections["snapshots"].find_one(
        {"provider": provider},
        sort=[("trade_date", DESCENDING)],
    )
    from .arena_portfolio import get_capital_pool
    initial_capital = get_capital_pool(provider)
    restored = _restore_account_from_snapshot(provider, snapshot, initial_capital)
    # Persist restored account so next run finds it
    collections["accounts"].update_one(
        {"provider": provider},
        {"$set": restored},
        upsert=True,
    )
    return restored


def _build_allocated_sell_fills(
    provider: str,
    allocated_sell_positions: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build sell fills from allocated sell positions for one provider."""
    fills: list[dict[str, Any]] = []
    for stock_code, position in allocated_sell_positions.items():
        volume = int(position.get("volume", 0) or 0)
        avg_price = float(position.get("avg_price", 0.0) or 0.0)
        if volume <= 0 or avg_price <= 0:
            continue
        fills.append({
            "stock_code": stock_code,
            "direction": SignalDirection.SELL.value,
            "price": avg_price,
            "volume": volume,
            "provider": provider,
            "fill_source": "sell_allocated",
        })
    return fills


def _load_signals(
    collection: Any,
    provider: str,
    trade_date: date,
) -> list[dict[str, Any]]:
    document = collection.find_one({
        "trade_date": trade_date.isoformat(),
        "provider": provider,
    })
    return list((document or {}).get("signals", []))


def _load_fills(
    provider: str,
    signals: list[dict[str, Any]],
    engine: Any,
) -> list[dict[str, Any]]:
    qmt_fills = _query_qmt_fills(provider, engine)
    if qmt_fills:
        logger.info(f"[{provider}] 使用 QMT 成交数据结算，共 {len(qmt_fills)} 笔")
        return qmt_fills
    approx_fills = _signals_to_fills(signals, provider)
    logger.info(f"[{provider}] 未读取到 QMT 成交，使用信号近似成交，共 {len(approx_fills)} 笔")
    return approx_fills


def _query_qmt_fills(provider: str, engine: Any) -> list[dict[str, Any]]:
    if engine is None:
        return []
    try:
        trades = engine.query_trades() or []
    except Exception as exc:
        logger.warning(f"[{provider}] 查询 QMT 成交失败: {exc}")
        return []
    fills = [_trade_to_fill(item, provider) for item in trades]
    return [fill for fill in fills if fill["volume"] > 0 and fill["price"] > 0]


def _trade_to_fill(trade: Any, provider: str) -> dict[str, Any]:
    return {
        "stock_code": str(getattr(trade, "stock_code", "")),
        "direction": _normalize_direction(getattr(trade, "traded_type", "")),
        "price": float(getattr(trade, "traded_price", 0.0) or 0.0),
        "volume": int(getattr(trade, "traded_volume", 0) or 0),
        "provider": provider,
        "fill_source": "qmt",
        "trade_id": int(getattr(trade, "traded_id", 0) or 0),
        "traded_at": str(getattr(trade, "traded_time", "")),
    }


def _normalize_direction(value: Any) -> str:
    if isinstance(value, SignalDirection):
        return value.value
    text = str(value or "").lower()
    if text in {"23", "buy"}:
        return SignalDirection.BUY.value
    if text in {"24", "sell"}:
        return SignalDirection.SELL.value
    return text


def _apply_fills(
    account: dict[str, Any],
    fills: list[dict[str, Any]],
) -> tuple[float, dict[str, dict[str, Any]], dict[str, float]]:
    cash = float(account.get("cash", account.get("initial_capital", 0.0)) or 0.0)
    positions = _copy_positions(account.get("positions", {}))
    trade_stats = _empty_trade_stats()
    for fill in fills:
        _apply_single_fill(fill, positions, trade_stats)
        cash = _update_cash(cash, fill)
    _cleanup_positions(positions)
    return cash, positions, trade_stats


def _apply_fills_incremental(
    account: dict[str, Any],
    buy_fills: list[dict[str, Any]],
    sell_fills: list[dict[str, Any]],
) -> tuple[float, dict[str, dict[str, Any]], dict[str, float]]:
    """Apply buy and sell fills incrementally to current account state."""
    cash = float(account.get("cash", account.get("initial_capital", 0.0)) or 0.0)
    positions = _copy_positions(account.get("positions", {}))
    trade_stats = _empty_trade_stats()
    for fill in buy_fills:
        _apply_single_fill(fill, positions, trade_stats)
        cash = _update_cash(cash, fill)
    for fill in sell_fills:
        _apply_single_fill(fill, positions, trade_stats)
        cash = _update_cash(cash, fill)
    _cleanup_positions(positions)
    return cash, positions, trade_stats


def _copy_positions(raw_positions: dict[str, Any]) -> dict[str, dict[str, Any]]:
    copied: dict[str, dict[str, Any]] = {}
    for stock_code, payload in (raw_positions or {}).items():
        copied[stock_code] = _normalize_position(payload)
    return copied


def _normalize_position(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        volume = int(payload.get("volume", 0) or 0)
        avg_price = float(payload.get("avg_price", payload.get("cost_price", 0.0)) or 0.0)
        return {"volume": volume, "avg_price": avg_price}
    return {"volume": int(payload or 0), "avg_price": 0.0}


def _empty_trade_stats() -> dict[str, float]:
    return {
        "buy_count": 0,
        "sell_count": 0,
        "total_buy_amount": 0.0,
        "total_sell_amount": 0.0,
    }


def _apply_single_fill(
    fill: dict[str, Any],
    positions: dict[str, dict[str, Any]],
    trade_stats: dict[str, float],
) -> None:
    stock_code = str(fill["stock_code"])
    amount = float(fill["price"]) * int(fill["volume"])
    if fill["direction"] == SignalDirection.BUY.value:
        _apply_buy_fill(positions, stock_code, int(fill["volume"]), float(fill["price"]))
        trade_stats["buy_count"] += 1
        trade_stats["total_buy_amount"] += amount
        return
    _apply_sell_fill(positions, stock_code, int(fill["volume"]))
    trade_stats["sell_count"] += 1
    trade_stats["total_sell_amount"] += amount


def _apply_buy_fill(
    positions: dict[str, dict[str, Any]],
    stock_code: str,
    volume: int,
    price: float,
) -> None:
    current = positions.get(stock_code, {"volume": 0, "avg_price": 0.0})
    old_volume = int(current.get("volume", 0) or 0)
    old_avg_price = float(current.get("avg_price", 0.0) or 0.0)
    new_volume = old_volume + volume
    new_avg_price = _weighted_avg_price(old_volume, old_avg_price, volume, price)
    positions[stock_code] = {"volume": new_volume, "avg_price": new_avg_price}


def _weighted_avg_price(
    old_volume: int,
    old_avg_price: float,
    new_volume: int,
    new_price: float,
) -> float:
    total_volume = old_volume + new_volume
    if total_volume <= 0:
        return 0.0
    total_cost = old_volume * old_avg_price + new_volume * new_price
    return total_cost / total_volume


def _apply_sell_fill(
    positions: dict[str, dict[str, Any]],
    stock_code: str,
    volume: int,
) -> None:
    current = positions.get(stock_code, {"volume": 0, "avg_price": 0.0})
    remaining = int(current.get("volume", 0) or 0) - volume
    positions[stock_code] = {
        "volume": max(remaining, 0),
        "avg_price": float(current.get("avg_price", 0.0) or 0.0),
    }


def _update_cash(cash: float, fill: dict[str, Any]) -> float:
    amount = float(fill["price"]) * int(fill["volume"])
    if fill["direction"] == SignalDirection.BUY.value:
        return cash - amount
    return cash + amount


def _cleanup_positions(positions: dict[str, dict[str, Any]]) -> None:
    empty_codes = [code for code, item in positions.items() if int(item.get("volume", 0)) <= 0]
    for stock_code in empty_codes:
        positions.pop(stock_code, None)


def _calculate_market_value(
    positions: dict[str, dict[str, Any]],
    trade_date: date,
) -> float:
    close_prices = _get_close_prices(list(positions.keys()), trade_date)
    total_value = 0.0
    for stock_code, position in positions.items():
        volume = int(position.get("volume", 0) or 0)
        total_value += volume * float(close_prices.get(stock_code, 0.0) or 0.0)
    return total_value


def _calculate_metrics(
    account: dict[str, Any],
    cash: float,
    market_value: float,
    trade_date: date,
    provider: str,
) -> dict[str, float]:
    total_asset = cash + market_value
    previous_snapshot = _get_previous_snapshot(trade_date, provider)
    previous_total_asset = _resolve_previous_total_asset(account, previous_snapshot)
    initial_capital = float(account.get("initial_capital", total_asset) or total_asset)
    daily_pnl = total_asset - previous_total_asset
    daily_return_pct = _safe_pct(daily_pnl, previous_total_asset)
    cumulative_pnl = total_asset - initial_capital
    cumulative_return_pct = _safe_pct(cumulative_pnl, initial_capital)
    return {
        "market_value": market_value,
        "total_asset": total_asset,
        "daily_pnl": daily_pnl,
        "daily_return_pct": daily_return_pct,
        "cumulative_return_pct": cumulative_return_pct,
    }


def _get_previous_snapshot(trade_date: date, provider: str) -> dict[str, Any] | None:
    collections = _get_collections()
    return collections["snapshots"].find_one(
        {"provider": provider, "trade_date": {"$lt": trade_date.isoformat()}},
        sort=[("trade_date", DESCENDING)],
    )


def _resolve_previous_total_asset(
    account: dict[str, Any],
    previous_snapshot: dict[str, Any] | None,
) -> float:
    if previous_snapshot:
        return float(previous_snapshot.get("total_asset", 0.0) or 0.0)
    return float(account.get("total_asset", account.get("initial_capital", 0.0)) or 0.0)


def _safe_pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator * 100


def _persist_settlement(
    collections: dict[str, Any],
    provider: str,
    trade_date: date,
    cash: float,
    positions: dict[str, dict[str, Any]],
    fills: list[dict[str, Any]],
    trade_stats: dict[str, float],
    metrics: dict[str, float],
) -> None:
    _update_account(collections["accounts"], provider, cash, positions, metrics)
    _write_trade_doc(collections["trades"], provider, trade_date, fills, trade_stats)
    _write_snapshot_doc(
        collections["snapshots"],
        provider,
        trade_date,
        cash,
        metrics["market_value"],
        positions,
        metrics,
    )


def _update_account(
    collection: Any,
    provider: str,
    cash: float,
    positions: dict[str, dict[str, Any]],
    metrics: dict[str, float],
) -> None:
    collection.update_one(
        {"provider": provider},
        {
            "$set": {
                "cash": cash,
                "positions": positions,
                "total_asset": metrics["total_asset"],
                "daily_return_pct": metrics["daily_return_pct"],
                "cumulative_return_pct": metrics["cumulative_return_pct"],
                "updated_at": datetime.now().isoformat(),
            },
        },
        upsert=False,
    )


def _write_trade_doc(
    collection: Any,
    provider: str,
    trade_date: date,
    fills: list[dict[str, Any]],
    trade_stats: dict[str, float],
) -> None:
    collection.replace_one(
        {"trade_date": trade_date.isoformat(), "provider": provider},
        {
            "trade_date": trade_date.isoformat(),
            "provider": provider,
            "trades": fills,
            "buy_count": int(trade_stats["buy_count"]),
            "sell_count": int(trade_stats["sell_count"]),
            "total_buy_amount": float(trade_stats["total_buy_amount"]),
            "total_sell_amount": float(trade_stats["total_sell_amount"]),
            "updated_at": datetime.now().isoformat(),
        },
        upsert=True,
    )


def _write_snapshot_doc(
    collection: Any,
    provider: str,
    trade_date: date,
    cash: float,
    market_value: float,
    positions: dict[str, dict[str, Any]],
    metrics: dict[str, float],
) -> None:
    total_asset = float(metrics["total_asset"])
    collection.replace_one(
        {"trade_date": trade_date.isoformat(), "provider": provider},
        {
            "trade_date": trade_date.isoformat(),
            "provider": provider,
            "cash": cash,
            "market_value": market_value,
            "total_asset": total_asset,
            "daily_pnl": metrics["daily_pnl"],
            "daily_return_pct": metrics["daily_return_pct"],
            "cumulative_return_pct": metrics["cumulative_return_pct"],
            "position_count": len(positions),
            "cash_ratio": _safe_pct(cash, total_asset),
            "positions": positions,
            "updated_at": datetime.now().isoformat(),
        },
        upsert=True,
    )


def _build_result(
    provider: str,
    trade_date: date,
    fills: list[dict[str, Any]],
    positions: dict[str, dict[str, Any]],
    cash: float,
    market_value: float,
    metrics: dict[str, float],
) -> dict[str, Any]:
    return {
        "provider": provider,
        "trade_date": trade_date.isoformat(),
        "trade_count": len(fills),
        "position_count": len(positions),
        "cash": cash,
        "market_value": market_value,
        "total_asset": metrics["total_asset"],
        "daily_return_pct": metrics["daily_return_pct"],
        "cumulative_return_pct": metrics["cumulative_return_pct"],
    }


def _to_qlibrd_code(stock_code: str) -> str:
    code, exchange = stock_code.split(".", maxsplit=1)
    return f"{exchange.upper()}{code}"
