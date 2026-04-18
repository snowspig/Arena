"""交易信号获取客户端 —— 支持 mock 和 remote 两种模式"""

from __future__ import annotations

import uuid
from datetime import date, datetime

import httpx
from loguru import logger

from .config import get_config
from .models import SignalBatch, SignalDirection, TimingType, TradeSignal


def fetch_signals(signal_date: date | None = None) -> SignalBatch:
    """根据配置中的 mode 获取交易信号"""
    cfg = get_config()
    mode = cfg["signal_source"]["mode"]
    target_date = signal_date or date.today()

    if mode == "mock":
        return _fetch_from_mock_api(target_date, cfg)
    elif mode == "remote":
        return _fetch_from_remote(target_date, cfg)
    elif mode == "arena":
        return _fetch_from_arena(target_date, cfg)
    else:
        raise ValueError(f"未知的信号源模式: {mode}")


def _fetch_from_mock_api(target_date: date, cfg: dict) -> SignalBatch:
    """从本地 mock API 获取信号"""
    url = cfg.get("signal_source", {}).get("mock_url", "http://127.0.0.1:8000/api/signals")
    params = {"signal_date": target_date.isoformat()}
    try:
        resp = httpx.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return SignalBatch.model_validate(resp.json())
    except Exception as e:
        logger.warning(f"Mock API 请求失败, 使用内置模拟数据: {e}")
        return _generate_fallback_signals(target_date)


def _fetch_from_remote(target_date: date, cfg: dict) -> SignalBatch:
    """从远程信号服务器获取信号"""
    url = cfg["signal_source"]["remote_url"]
    params = {"signal_date": target_date.isoformat()}
    try:
        resp = httpx.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return SignalBatch.model_validate(resp.json())
    except Exception as e:
        logger.error(f"远程信号获取失败: {e}")
        raise


def _fetch_from_arena(target_date: date, cfg: dict) -> SignalBatch:
    """从本地 Arena 生成结果读取合并信号（所有 agent）。"""
    from .arena_runner import load_merged_signals

    batch = load_merged_signals(target_date)
    if batch is not None:
        return batch
    raise ValueError(f"未找到 Arena 信号: {target_date.isoformat()}")


def _generate_fallback_signals(target_date: date) -> SignalBatch:
    """当 mock API 不可用时的兜底信号"""
    signals = [
        TradeSignal(
            signal_id=uuid.uuid4().hex[:12],
            stock_code="600519.SH",
            direction=SignalDirection.BUY,
            volume=100,
            price=0.0,
            timing=TimingType.OPEN_AUCTION,
            signal_date=target_date,
            reason="fallback_test",
        ),
    ]
    return SignalBatch(
        batch_id=uuid.uuid4().hex[:16],
        signals=signals,
        generated_at=datetime.now(),
    )
