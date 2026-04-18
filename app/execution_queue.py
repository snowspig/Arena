"""执行队列 —— 从 arena_signals 合并信号到独立队列，供模拟盘/实盘下单使用。

arena_signals  = AI 预测结果（只读数据资产，不清除）
execution_queue = 今日委托计划（可增删改，不影响原始信号）

每个账户(simulation/live)有独立的执行队列。
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from loguru import logger
from pymongo import MongoClient

from .arena_portfolio import get_enabled_providers
from .config import get_config, get_mongo_db, get_mongo_uri
from .models import SignalBatch, SignalDirection, TimingType, TradeSignal


def _get_collection(account_type: str = "simulation"):
    uri = get_mongo_uri()
    db_name = get_mongo_db()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    # 按账户类型使用不同的 collection
    return client[db_name][f"execution_queue_{account_type}"]


def populate_from_arena(target_date: date | None = None, account_type: str = "simulation") -> int:
    """从 arena_signals 合并所有 enabled provider 的信号到指定账户的执行队列。

    Args:
        target_date: 交易日期，默认今天。
        account_type: 账户类型 ("simulation" 或 "live")。

    Returns:
        写入执行队列的信号数量。
    """
    target_date = target_date or date.today()

    # 读取 arena_signals 并合并
    uri = get_mongo_uri()
    db_name = get_mongo_db()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    arena_col = client[db_name]["arena_signals"]

    enabled = get_enabled_providers()
    cursor = arena_col.find({
        "trade_date": target_date.isoformat(),
        "provider": {"$in": enabled},
    })

    merged: dict[str, dict[str, Any]] = {}
    providers_map: dict[str, list[str]] = {}

    for doc in cursor:
        provider = doc.get("provider", "unknown")
        for s in doc.get("signals", []):
            code = s.get("stock_code", "")
            if not code:
                continue
            providers_map.setdefault(code, []).append(provider)
            if code not in merged:
                merged[code] = {
                    "stock_code": code,
                    "volume": 0,
                    "price": float(s.get("price", 0) or 0.0),
                }
            merged[code]["volume"] += int(s.get("volume", 0) or 0)

    if not merged:
        logger.warning(f"[{account_type}] arena_signals 中无可用信号，执行队列为空")
        return 0

    queue_signals = []
    for code, data in merged.items():
        if data["price"] <= 0:
            continue
        agents = providers_map[code]
        signal = TradeSignal(
            signal_id=f"eq-{account_type}-{target_date.isoformat()}-{code}",
            stock_code=code,
            direction=SignalDirection.BUY,
            volume=data["volume"],
            price=data["price"],
            timing=TimingType.OPEN_AUCTION,
            signal_date=target_date,
            reason=f"merged {len(agents)} agents: {', '.join(agents)}",
        )
        queue_signals.append(signal.model_dump(mode="json"))

    # 写入指定账户的执行队列（替换当日已有数据）
    col = _get_collection(account_type)
    col.delete_many({"trade_date": target_date.isoformat()})
    if queue_signals:
        col.insert_many([
            {**s, "trade_date": target_date.isoformat(), "account_type": account_type, "created_at": datetime.now().isoformat()}
            for s in queue_signals
        ])

    logger.info(
        f"[{account_type}] 执行队列已填充: {len(queue_signals)} 条信号 "
        f"(from {len(enabled)} providers)"
    )
    return len(queue_signals)


def load_queue(target_date: date | None = None, account_type: str = "simulation") -> SignalBatch:
    """从指定账户的执行队列读取信号。

    Args:
        target_date: 交易日期，默认今天。
        account_type: 账户类型 ("simulation" 或 "live")。

    Returns:
        SignalBatch，如无数据则 signals 为空列表。
    """
    target_date = target_date or date.today()
    col = _get_collection(account_type)

    docs = list(col.find({"trade_date": target_date.isoformat()}))
    signals = []
    for d in docs:
        try:
            signals.append(TradeSignal.model_validate(d))
        except Exception as e:
            logger.warning(f"解析执行队列信号失败: {e}")

    return SignalBatch(
        batch_id=f"execution-queue-{account_type}-{target_date.isoformat()}",
        signals=signals,
    )


def clear_queue(
    target_date: date | None = None,
    mode: str = "all",
    account_type: str = "simulation",
) -> int:
    """清除指定账户的执行队列中的信号。

    Args:
        target_date: 交易日期，默认今天。
        mode: "buy" / "sell" / "all"。
        account_type: 账户类型 ("simulation" 或 "live")。

    Returns:
        被清除的信号数量。
    """
    target_date = target_date or date.today()
    col = _get_collection(account_type)

    if mode == "all":
        result = col.delete_many({"trade_date": target_date.isoformat()})
        cleared = result.deleted_count
    else:
        # 只删除指定方向的信号
        docs = list(col.find({
            "trade_date": target_date.isoformat(),
            "direction": mode,
        }))
        if not docs:
            return 0
        ids = [d["_id"] for d in docs]
        result = col.delete_many({"_id": {"$in": ids}})
        cleared = result.deleted_count

    logger.info(f"[{account_type}] 执行队列清除: mode={mode}, cleared={cleared}")
    return cleared
