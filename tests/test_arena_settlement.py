"""Tests for arena_settlement helpers."""

from unittest.mock import MagicMock

import pytest

from app.arena_portfolio import ensure_arena_accounts
from app.arena_settlement import _aggregate_sell_fills_by_stock
from app.models import SignalDirection


class FakeTrade:
    def __init__(self, stock_code, traded_type, traded_price, traded_volume):
        self.stock_code = stock_code
        self.traded_type = traded_type
        self.traded_price = traded_price
        self.traded_volume = traded_volume


def test_aggregate_sell_fills_by_stock_vwap():
    engine = MagicMock()
    engine.query_trades.return_value = [
        FakeTrade("000001.SZ", "24", 10.0, 100),
        FakeTrade("000001.SZ", "24", 12.0, 200),
        FakeTrade("600000.SH", "24", 5.0, 500),
    ]
    result = _aggregate_sell_fills_by_stock(engine)
    assert result == {
        "000001.SZ": {"volume": 300, "avg_price": pytest.approx(11.333333333333334)},
        "600000.SH": {"volume": 500, "avg_price": 5.0},
    }


def test_aggregate_sell_fills_by_stock_ignores_buys():
    engine = MagicMock()
    engine.query_trades.return_value = [
        FakeTrade("000001.SZ", "23", 10.0, 100),
        FakeTrade("000001.SZ", "24", 12.0, 200),
    ]
    result = _aggregate_sell_fills_by_stock(engine)
    assert result == {"000001.SZ": {"volume": 200, "avg_price": 12.0}}


def test_aggregate_sell_fills_by_stock_empty():
    engine = MagicMock()
    engine.query_trades.return_value = []
    result = _aggregate_sell_fills_by_stock(engine)
    assert result == {}


def test_aggregate_sell_fills_by_stock_query_exception():
    engine = MagicMock()
    engine.query_trades.side_effect = RuntimeError("QMT disconnected")
    result = _aggregate_sell_fills_by_stock(engine)
    assert result == {}


def test_allocate_sell_fills_by_holdings():
    """Sell volume should be allocated by provider holding proportion."""
    from app.arena_settlement import _allocate_sell_fills_by_holdings

    sell_fills = {"600519.SH": {"volume": 300, "avg_price": 50.0}}
    provider_holdings = {
        "agent_a": {"600519.SH": {"volume": 200, "avg_price": 40.0}},
        "agent_b": {"600519.SH": {"volume": 100, "avg_price": 40.0}},
    }
    result = _allocate_sell_fills_by_holdings(sell_fills, provider_holdings)
    assert result["agent_a"]["600519.SH"]["volume"] == 200
    assert result["agent_b"]["600519.SH"]["volume"] == 100


def test_allocate_sell_fills_caps_at_holding():
    """Provider cannot sell more than they hold."""
    from app.arena_settlement import _allocate_sell_fills_by_holdings

    sell_fills = {"600519.SH": {"volume": 500, "avg_price": 50.0}}
    provider_holdings = {
        "agent_a": {"600519.SH": {"volume": 200, "avg_price": 40.0}},
        "agent_b": {"600519.SH": {"volume": 100, "avg_price": 40.0}},
    }
    result = _allocate_sell_fills_by_holdings(sell_fills, provider_holdings)
    # Total holding is 300, sell is 500 -> capped at 300
    total_allocated = sum(
        alloc.get("600519.SH", {}).get("volume", 0)
        for alloc in result.values()
    )
    assert total_allocated == 300
    assert result["agent_a"]["600519.SH"]["volume"] == 200
    assert result["agent_b"]["600519.SH"]["volume"] == 100


def test_allocate_sell_fills_skips_no_holding():
    """Stock with no provider holdings should be skipped."""
    from app.arena_settlement import _allocate_sell_fills_by_holdings

    sell_fills = {"999999.SH": {"volume": 100, "avg_price": 10.0}}
    provider_holdings = {
        "agent_a": {"600519.SH": {"volume": 200, "avg_price": 40.0}},
    }
    result = _allocate_sell_fills_by_holdings(sell_fills, provider_holdings)
    assert result["agent_a"] == {}


def test_restore_account_from_snapshot():
    """When account is missing, restore from latest snapshot."""
    from app.arena_settlement import _restore_account_from_snapshot

    snapshot = {
        "provider": "test_agent",
        "cash": 4000000.0,
        "market_value": 1200000.0,
        "total_asset": 5200000.0,
        "daily_return_pct": 4.0,
        "cumulative_return_pct": 4.0,
    }
    account = _restore_account_from_snapshot("test_agent", snapshot, initial_capital=5000000.0)
    assert account["provider"] == "test_agent"
    assert account["cash"] == 4000000.0
    assert account["total_asset"] == 5200000.0
    assert account["initial_capital"] == 5000000.0
    assert account["cumulative_return_pct"] == 4.0


def test_restore_account_no_snapshot():
    """When no snapshot exists, create initial account."""
    from app.arena_settlement import _restore_account_from_snapshot

    account = _restore_account_from_snapshot("test_agent", None, initial_capital=5000000.0)
    assert account["provider"] == "test_agent"
    assert account["cash"] == 5000000.0
    assert account["positions"] == {}
    assert account["total_asset"] == 5000000.0
    assert account["initial_capital"] == 5000000.0


def test_incremental_settlement_preserves_existing_positions():
    """Settlement should add to existing positions, not replace them."""
    from app.arena_settlement import _apply_fills_incremental

    account = {
        "provider": "agent_a",
        "initial_capital": 5000000.0,
        "cash": 4000000.0,
        "total_asset": 5000000.0,
        "positions": {
            "600519.SH": {"volume": 100, "avg_price": 40.0},
        },
    }
    buy_fills = [
        {"stock_code": "000858.SZ", "direction": "buy", "price": 30.0, "volume": 200},
    ]
    sell_fills = []
    cash, positions, trade_stats = _apply_fills_incremental(account, buy_fills, sell_fills)
    # Existing position preserved
    assert "600519.SH" in positions
    assert positions["600519.SH"]["volume"] == 100
    # New position added
    assert "000858.SZ" in positions
    assert positions["000858.SZ"]["volume"] == 200
    # Cash reduced by buy
    assert cash == 4000000.0 - 30.0 * 200


def test_incremental_settlement_applies_sells():
    """Sell fills should reduce positions and add cash."""
    from app.arena_settlement import _apply_fills_incremental

    account = {
        "provider": "agent_a",
        "initial_capital": 5000000.0,
        "cash": 4000000.0,
        "total_asset": 5000000.0,
        "positions": {
            "600519.SH": {"volume": 200, "avg_price": 40.0},
        },
    }
    buy_fills = []
    sell_fills = [
        {"stock_code": "600519.SH", "direction": "sell", "price": 50.0, "volume": 100},
    ]
    cash, positions, trade_stats = _apply_fills_incremental(account, buy_fills, sell_fills)
    assert positions["600519.SH"]["volume"] == 100
    assert cash == 4000000.0 + 50.0 * 100


def test_incremental_settlement_sell_removes_position():
    """Selling all volume should remove the position entry."""
    from app.arena_settlement import _apply_fills_incremental

    account = {
        "provider": "agent_a",
        "initial_capital": 5000000.0,
        "cash": 4000000.0,
        "total_asset": 5000000.0,
        "positions": {
            "600519.SH": {"volume": 100, "avg_price": 40.0},
        },
    }
    buy_fills = []
    sell_fills = [
        {"stock_code": "600519.SH", "direction": "sell", "price": 50.0, "volume": 100},
    ]
    cash, positions, trade_stats = _apply_fills_incremental(account, buy_fills, sell_fills)
    assert "600519.SH" not in positions
    assert cash == 4000000.0 + 50.0 * 100


def test_ensure_arena_accounts_only_creates_missing(monkeypatch):
    collection = MagicMock()
    collection.find_one.side_effect = [
        {"provider": "agent_a", "cash": 123.0},
        None,
    ]
    monkeypatch.setattr("app.arena_portfolio._get_mongo_collection", lambda name: collection)
    monkeypatch.setattr("app.arena_portfolio.get_enabled_providers", lambda: ["agent_a", "agent_b"])
    monkeypatch.setattr("app.arena_portfolio.get_capital_pool", lambda provider: 5_000_000.0)

    ensure_arena_accounts()

    collection.insert_one.assert_called_once()
    inserted = collection.insert_one.call_args.args[0]
    assert inserted["provider"] == "agent_b"
    assert inserted["cash"] == 5_000_000.0
    assert inserted["positions"] == {}
    assert inserted["total_asset"] == 5_000_000.0
