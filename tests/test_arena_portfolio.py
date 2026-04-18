from datetime import date

import pytest

from app import arena_portfolio
from app.arena_portfolio import calc_limit_down_price, get_limit_rate


class _FakeCollection:
    def __init__(self, docs):
        self._docs = docs

    def find(self, query, projection):
        del projection
        stock_codes = set(query["stock_code"]["$in"])
        target_dates = set(query["date"]["$in"])
        return [
            doc for doc in self._docs
            if doc["stock_code"] in stock_codes and doc["date"] in target_dates
        ]

    def find_one(self, query, projection, sort=None):
        del projection, sort
        stock_code = query["stock_code"]
        date_filter = query.get("date", {}).get("$in")
        rows = [doc for doc in self._docs if doc["stock_code"] == stock_code]
        if date_filter is not None:
            target_dates = set(date_filter)
            rows = [doc for doc in rows if doc["date"] in target_dates]
        rows.sort(key=lambda item: item["date"], reverse=True)
        return rows[0] if rows else None


class _FakeDatabase:
    def __init__(self, docs):
        self._docs = docs

    def __getitem__(self, name):
        assert name == "cn_data_stock_features"
        return _FakeCollection(self._docs)


class _FakeClient:
    def __init__(self, docs):
        self._docs = docs

    def __getitem__(self, name):
        assert name == "qlibrd"
        return _FakeDatabase(self._docs)


class TestGetLimitRate:
    def test_returns_main_board_sh_rate(self) -> None:
        assert get_limit_rate("600519.SH") == pytest.approx(0.10)

    def test_returns_main_board_sz_rate(self) -> None:
        assert get_limit_rate("000858.SZ") == pytest.approx(0.10)

    def test_returns_chinext_rate(self) -> None:
        assert get_limit_rate("300750.SZ") == pytest.approx(0.20)

    def test_returns_star_market_rate(self) -> None:
        assert get_limit_rate("688981.SH") == pytest.approx(0.20)

    def test_returns_bse_rate(self) -> None:
        assert get_limit_rate("830799.BJ") == pytest.approx(0.30)


class TestCalcLimitDownPrice:
    def test_returns_main_board_limit_down_price(self) -> None:
        assert calc_limit_down_price(10.0, "600519.SH") == pytest.approx(9.0)

    def test_returns_chinext_limit_down_price(self) -> None:
        assert calc_limit_down_price(10.0, "300750.SZ") == pytest.approx(8.0)

    def test_returns_star_market_limit_down_price(self) -> None:
        assert calc_limit_down_price(10.0, "688981.SH") == pytest.approx(8.0)

    def test_returns_bse_limit_down_price(self) -> None:
        assert calc_limit_down_price(10.0, "830799.BJ") == pytest.approx(7.0)


def test_batch_get_prev_close_prefers_mongo(monkeypatch) -> None:
    docs = [
        {"stock_code": "SH601138", "date": "2026-04-16", "close": 35.2},
        {"stock_code": "SZ000988", "date": "2026-04-16", "close": 118.6},
    ]
    monkeypatch.setattr(arena_portfolio, "_create_qlibrd_client", lambda: _FakeClient(docs))
    monkeypatch.setattr(arena_portfolio, "_batch_get_prev_close_from_xtdata", lambda stock_codes: {})

    result = arena_portfolio._batch_get_prev_close(
        ["601138.SH", "000988.SZ"],
        trade_date=date(2026, 4, 17),
    )

    assert result == {"601138.SH": 35.2, "000988.SZ": 118.6}


def test_get_prev_close_returns_target_previous_day_from_mongo(monkeypatch) -> None:
    docs = [
        {"stock_code": "SZ300308", "date": "2026-04-15", "close": 61.23},
        {"stock_code": "SZ300308", "date": "2026-04-14", "close": 58.11},
    ]
    monkeypatch.setattr(arena_portfolio, "_create_qlibrd_client", lambda: _FakeClient(docs))
    monkeypatch.setattr(arena_portfolio, "_get_prev_close_from_xtdata", lambda stock_code, trade_date=None: 0.0)

    result = arena_portfolio._get_prev_close("300308.SZ", date(2026, 4, 16))

    assert result == 61.23


def test_get_prev_close_returns_zero_without_target_previous_day(monkeypatch) -> None:
    docs = [
        {"stock_code": "SZ300308", "date": "2026-04-14", "close": 58.11},
    ]
    monkeypatch.setattr(arena_portfolio, "_create_qlibrd_client", lambda: _FakeClient(docs))
    monkeypatch.setattr(arena_portfolio, "_get_prev_close_from_xtdata", lambda stock_code, trade_date=None: 0.0)

    result = arena_portfolio._get_prev_close("300308.SZ", date(2026, 4, 16))

    assert result == 0.0
