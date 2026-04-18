from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger
from pymongo import ASCENDING, DESCENDING, MongoClient

from .config import get_account_config, get_config
from .models import (
    AssetSnapshot,
    DailySnapshot,
    OrderSnapshot,
    PositionSnapshot,
    TradeSnapshot,
)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "daily"
_MONGO_CLIENT: MongoClient | None = None
_COLLECTION = None


def _get_collection():
    global _MONGO_CLIENT, _COLLECTION
    if _COLLECTION is not None:
        return _COLLECTION

    cfg = get_config()["mongodb"]
    _MONGO_CLIENT = MongoClient(cfg["uri"])
    database = _MONGO_CLIENT[cfg["database"]]
    _COLLECTION = database[cfg["settlement_collection"]]
    _COLLECTION.create_index(
        [("date", ASCENDING), ("account_type", ASCENDING)],
        unique=True,
    )
    return _COLLECTION


def save_settlement_document(document: dict[str, Any]) -> None:
    """Upsert a settlement document by date and account_type."""
    date = document.get("date")
    account_type = document.get("account_type")
    if not date or not account_type:
        raise ValueError("document must contain 'date' and 'account_type'")
    _get_collection().update_one(
        {"date": date, "account_type": account_type},
        {"$set": document},
        upsert=True,
    )


def _safe_float(value: Any) -> float:
    return float(value or 0.0)


def _safe_int(value: Any) -> int:
    return int(value or 0)


def _resolve_direction(value: Any) -> str:
    if value == 23:
        return "buy"
    if value == 24:
        return "sell"
    return str(value or "")


def _build_asset_snapshot(asset: Any) -> AssetSnapshot:
    return AssetSnapshot(
        total_asset=_safe_float(getattr(asset, "total_asset", 0.0)),
        cash=_safe_float(getattr(asset, "cash", 0.0)),
        market_value=_safe_float(getattr(asset, "market_value", 0.0)),
        frozen_cash=_safe_float(getattr(asset, "frozen_cash", 0.0)),
    )


def _build_position_snapshots(positions: list[Any]) -> list[PositionSnapshot]:
    snapshots: list[PositionSnapshot] = []
    for position in positions:
        volume = _safe_int(getattr(position, "volume", 0))
        cost_price = _safe_float(getattr(position, "open_price", 0.0))
        market_value = _safe_float(getattr(position, "market_value", 0.0))
        snapshots.append(
            PositionSnapshot(
                stock_code=str(getattr(position, "stock_code", "")),
                volume=volume,
                can_use_volume=_safe_int(getattr(position, "can_use_volume", 0)),
                cost_price=cost_price,
                market_value=market_value,
                unrealized_pnl=market_value - volume * cost_price,
            )
        )
    return snapshots


def _build_order_snapshots(orders: list[Any]) -> list[OrderSnapshot]:
    snapshots: list[OrderSnapshot] = []
    for order in orders:
        snapshots.append(
            OrderSnapshot(
                order_id=_safe_int(getattr(order, "order_id", 0)),
                stock_code=str(getattr(order, "stock_code", "")),
                direction=_resolve_direction(getattr(order, "order_type", "")),
                order_volume=_safe_int(getattr(order, "order_volume", 0)),
                traded_volume=_safe_int(getattr(order, "traded_volume", 0)),
                traded_price=_safe_float(getattr(order, "traded_price", 0.0)),
                price_type=_safe_int(getattr(order, "price_type", 0)),
                order_status=str(getattr(order, "order_status", "")),
            )
        )
    return snapshots


def _build_trade_snapshots(trades: list[Any]) -> list[TradeSnapshot]:
    snapshots: list[TradeSnapshot] = []
    for trade in trades:
        snapshots.append(
            TradeSnapshot(
                trade_id=_safe_int(getattr(trade, "traded_id", 0)),
                stock_code=str(getattr(trade, "stock_code", "")),
                direction=_resolve_direction(getattr(trade, "traded_type", "")),
                traded_price=_safe_float(getattr(trade, "traded_price", 0.0)),
                traded_volume=_safe_int(getattr(trade, "traded_volume", 0)),
            )
        )
    return snapshots


def _get_previous_snapshot(date_str: str, account_type: str) -> dict[str, Any] | None:
    return _get_collection().find_one(
        {
            "account_type": account_type,
            "date": {"$lt": date_str},
        },
        sort=[("date", DESCENDING)],
    )


def _calculate_daily_metrics(
    total_asset: float,
    previous: dict[str, Any] | None,
) -> tuple[float, float]:
    if not previous:
        return 0.0, 0.0

    previous_asset = _safe_float(previous.get("asset", {}).get("total_asset", 0.0))
    if previous_asset <= 0:
        return 0.0, 0.0

    daily_pnl = total_asset - previous_asset
    daily_return_pct = daily_pnl / previous_asset * 100
    return daily_pnl, daily_return_pct


def _export_json(snapshot: DailySnapshot) -> Path:
    output_dir = _DATA_DIR / snapshot.account_type
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{snapshot.date}.json"
    output_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    return output_path


def _format_signed(value: float) -> str:
    return f"{value:+,.2f}"


def _build_markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "暂无数据"
    header_row = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header_row, separator, *body])


def _export_markdown(snapshot: DailySnapshot) -> Path:
    output_dir = _DATA_DIR / snapshot.account_type
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{snapshot.date}.md"

    position_rows = [
        [
            item.stock_code,
            str(item.volume),
            str(item.can_use_volume),
            f"{item.cost_price:,.3f}",
            f"{item.market_value:,.2f}",
            _format_signed(item.unrealized_pnl),
        ]
        for item in snapshot.positions
    ]
    order_rows = [
        [
            str(item.order_id),
            item.stock_code,
            item.direction,
            str(item.order_volume),
            str(item.traded_volume),
            f"{item.traded_price:,.3f}",
            str(item.price_type),
            item.order_status,
        ]
        for item in snapshot.orders
    ]
    trade_rows = [
        [
            str(item.trade_id),
            item.stock_code,
            item.direction,
            str(item.traded_volume),
            f"{item.traded_price:,.3f}",
        ]
        for item in snapshot.trades
    ]

    lines = [
        f"# {snapshot.date} 账户结算报告",
        "",
        f"- 账户类型: {snapshot.account_type}",
        f"- 账户编号: {snapshot.account_id}",
        f"- 导出时间: {snapshot.created_at:%Y-%m-%d %H:%M:%S}",
        "",
        "## 资产概览",
        "",
        f"- 总资产: {snapshot.asset.total_asset:,.2f}",
        f"- 可用资金: {snapshot.asset.cash:,.2f}",
        f"- 持仓市值: {snapshot.asset.market_value:,.2f}",
        f"- 冻结资金: {snapshot.asset.frozen_cash:,.2f}",
        f"- 今日盈亏: {_format_signed(snapshot.daily_pnl)}",
        f"- 当日收益率: {_format_signed(snapshot.daily_return_pct)}%",
        "",
        "## 持仓",
        "",
        _build_markdown_table(
            ["证券代码", "持仓数量", "可用数量", "成本价", "市值", "浮动盈亏"],
            position_rows,
        ),
        "",
        "## 委托",
        "",
        _build_markdown_table(
            ["委托号", "证券代码", "方向", "委托数量", "成交数量", "成交均价", "报价类型", "状态"],
            order_rows,
        ),
        "",
        "## 成交",
        "",
        _build_markdown_table(
            ["成交号", "证券代码", "方向", "成交数量", "成交价格"],
            trade_rows,
        ),
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def save_daily_snapshot(engine: Any, account_type: str) -> dict[str, Any]:
    today = datetime.now().strftime("%Y-%m-%d")
    asset = _build_asset_snapshot(engine.query_asset())
    positions = _build_position_snapshots(engine.query_positions() or [])
    orders = _build_order_snapshots(engine.query_orders() or [])
    trades = _build_trade_snapshots(engine.query_trades() or [])
    previous = _get_previous_snapshot(today, account_type)
    daily_pnl, daily_return_pct = _calculate_daily_metrics(asset.total_asset, previous)

    snapshot = DailySnapshot(
        date=today,
        account_type=account_type,
        account_id=str(get_account_config(account_type).get("account_id", "")),
        asset=asset,
        positions=positions,
        orders=orders,
        trades=trades,
        daily_pnl=daily_pnl,
        daily_return_pct=daily_return_pct,
    )
    document = snapshot.model_dump(mode="json")
    _get_collection().update_one(
        {"date": snapshot.date, "account_type": snapshot.account_type},
        {"$set": document},
        upsert=True,
    )
    json_path = _export_json(snapshot)
    markdown_path = _export_markdown(snapshot)
    logger.info(f"结算快照已保存: {snapshot.date} {account_type}")
    logger.info(f"JSON 导出: {json_path}")
    logger.info(f"Markdown 导出: {markdown_path}")
    return document


def get_asset_history(
    days: int | None = None,
    account_type: str = "simulation",
) -> list[dict[str, Any]]:
    cursor = _get_collection().find(
        {"account_type": account_type},
        {"_id": 0},
    ).sort("date", ASCENDING)
    records = list(cursor)
    if days and days > 0:
        records = records[-days:]

    history: list[dict[str, Any]] = []
    for record in records:
        asset = record.get("asset", {})
        history.append(
            {
                "date": record.get("date", ""),
                "total_asset": _safe_float(asset.get("total_asset", 0.0)),
                "cash": _safe_float(asset.get("cash", 0.0)),
                "market_value": _safe_float(asset.get("market_value", 0.0)),
                "daily_pnl": _safe_float(record.get("daily_pnl", 0.0)),
            }
        )
    return history


def get_settlement_dates(account_type: str = "simulation") -> list[str]:
    cursor = _get_collection().find(
        {"account_type": account_type},
        {"date": 1, "_id": 0},
    ).sort("date", DESCENDING)
    return [item.get("date", "") for item in cursor]


def get_settlement(
    date: str,
    account_type: str = "simulation",
) -> dict[str, Any] | None:
    return _get_collection().find_one(
        {"date": date, "account_type": account_type},
        {"_id": 0},
    )
