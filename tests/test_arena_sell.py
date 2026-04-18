from datetime import date
from unittest.mock import MagicMock, patch

from app.arena_portfolio import (
    cancel_unfilled_sells,
    closing_auction_sell,
    continuous_auction_sell,
    is_at_limit_up,
)


def make_position(
    stock_code: str,
    can_use_volume: int,
    market_value: float = 0.0,
) -> MagicMock:
    pos = MagicMock()
    pos.stock_code = stock_code
    pos.can_use_volume = can_use_volume
    pos.volume = can_use_volume
    pos.market_value = market_value
    return pos


@patch("app.arena_portfolio._get_current_price")
@patch("app.arena_portfolio._get_prev_close")
def test_is_at_limit_up_true(mock_prev: MagicMock, mock_price: MagicMock) -> None:
    mock_prev.return_value = 10.0
    mock_price.return_value = 11.0
    assert is_at_limit_up("000001.SZ") is True


@patch("app.arena_portfolio._get_current_price")
@patch("app.arena_portfolio._get_prev_close")
def test_is_at_limit_up_false(mock_prev: MagicMock, mock_price: MagicMock) -> None:
    mock_prev.return_value = 10.0
    mock_price.return_value = 10.5
    assert is_at_limit_up("000001.SZ") is False


@patch("app.arena_portfolio._batch_get_current_price")
@patch("app.arena_portfolio._batch_get_prev_close")
def test_continuous_sell_skips_limit_up(
    mock_prev: MagicMock,
    mock_price: MagicMock,
) -> None:
    mock_prev.return_value = {"000001.SZ": 10.0}
    mock_price.return_value = {"000001.SZ": 11.0}

    engine = MagicMock()
    positions = [make_position("000001.SZ", 1000)]
    engine.query_positions.return_value = positions

    count = continuous_auction_sell(engine, date(2026, 4, 16))
    assert count == 0
    engine.place_order.assert_not_called()


@patch("app.arena_portfolio._batch_get_current_price")
@patch("app.arena_portfolio._batch_get_prev_close")
def test_continuous_sell_places_order(
    mock_prev: MagicMock,
    mock_price: MagicMock,
) -> None:
    mock_prev.return_value = {"000001.SZ": 10.0}
    mock_price.return_value = {"000001.SZ": 10.5}

    engine = MagicMock()
    positions = [make_position("000001.SZ", 1000)]
    engine.query_positions.return_value = positions
    engine.place_order.return_value = MagicMock(order_id=123, status="submitted")

    count = continuous_auction_sell(engine, date(2026, 4, 16))
    assert count == 1
    engine.place_order.assert_called_once()
    call_args = engine.place_order.call_args
    assert call_args[0][0].direction.value == "sell"
    assert call_args[0][0].volume == 1000


def test_cancel_unfilled_sells() -> None:
    engine = MagicMock()
    filled_order = MagicMock()
    filled_order.order_type = 24
    filled_order.traded_volume = 1000
    filled_order.order_volume = 1000

    partial_order = MagicMock()
    partial_order.order_type = 24
    partial_order.traded_volume = 500
    partial_order.order_volume = 1000
    partial_order.order_id = 456

    engine.query_orders.return_value = [filled_order, partial_order]

    cancel_unfilled_sells(engine)
    engine.cancel_order.assert_called_once_with(456)


@patch("app.arena_portfolio._batch_get_current_price")
@patch("app.arena_portfolio._batch_get_prev_close")
def test_closing_auction_sell_uses_limit_down(
    mock_prev: MagicMock,
    mock_price: MagicMock,
) -> None:
    mock_prev.return_value = {"000001.SZ": 10.0}
    mock_price.return_value = {"000001.SZ": 10.5}

    engine = MagicMock()
    positions = [make_position("000001.SZ", 1000)]
    engine.query_positions.return_value = positions
    engine.place_order.return_value = MagicMock(order_id=789, status="submitted")

    count = closing_auction_sell(engine, date(2026, 4, 16))
    assert count == 1
    call_args = engine.place_order.call_args
    assert call_args[0][0].price == 9.0
