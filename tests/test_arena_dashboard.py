import math

import pytest

from app import arena_dashboard


class FakeCollection:
    def __init__(self, doc=None, docs=None):
        self.doc = doc
        self.docs = docs or []

    def find_one(self, query, projection=None, sort=None):
        del query, projection
        if sort and self.docs:
            field, direction = sort[0]
            reverse = direction < 0
            docs = sorted(self.docs, key=lambda item: item.get(field, ""), reverse=reverse)
            return docs[0]
        return self.doc

    def find(self, query, projection=None):
        return FakeCursor(self.docs)


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, field, direction):
        reverse = direction < 0
        self.docs.sort(key=lambda item: item.get(field, ""), reverse=reverse)
        return self

    def limit(self, count):
        self.docs = self.docs[:count]
        return self

    def __iter__(self):
        return iter(self.docs)


class FakeDb(dict):
    def __getitem__(self, item):
        return super().__getitem__(item)


def test_positions_payload_preserves_explicit_empty_positions(monkeypatch) -> None:
    fake_db = FakeDb(
        {
            "arena_accounts": FakeCollection(
                doc={"provider": "demo", "positions": []}
            ),
            "arena_daily_snapshots": FakeCollection(
                docs=[
                    {
                        "provider": "demo",
                        "trade_date": "2026-04-16",
                        "positions": [{"stock_code": "600519.SH", "shares": 100}],
                    }
                ]
            ),
        }
    )
    monkeypatch.setattr(arena_dashboard, "_get_db", lambda: fake_db)

    payload = arena_dashboard._positions_payload("demo")

    assert payload["positions"] == []


def test_positions_payload_falls_back_when_positions_missing(monkeypatch) -> None:
    fake_db = FakeDb(
        {
            "arena_accounts": FakeCollection(
                doc={"provider": "demo"}
            ),
            "arena_daily_snapshots": FakeCollection(
                docs=[
                    {
                        "provider": "demo",
                        "trade_date": "2026-04-16",
                        "positions": [{"stock_code": "600519.SH", "shares": 100}],
                    }
                ]
            ),
        }
    )
    monkeypatch.setattr(arena_dashboard, "_get_db", lambda: fake_db)

    payload = arena_dashboard._positions_payload("demo")

    assert payload["positions"] == [{"stock_code": "600519.SH", "shares": 100}]


def test_compute_strategy_metrics_returns_expected_keys() -> None:
    rows = [
        {"daily_return_pct": 1.0, "total_asset": 101000},
        {"daily_return_pct": -0.5, "total_asset": 100500},
        {"daily_return_pct": 2.0, "total_asset": 102510},
    ]
    benchmark_returns = [0.008, -0.002, 0.01]

    metrics = arena_dashboard._compute_strategy_metrics_from_rows(rows, benchmark_returns)

    expected_keys = {
        "sharpe_ratio",
        "alpha_pct",
        "beta",
        "max_drawdown_pct",
        "win_rate",
        "calmar_ratio",
        "annual_return_pct",
        "volatility_pct",
    }
    assert expected_keys == set(metrics)
    assert metrics["win_rate"] == pytest.approx(2 / 3)
    assert not math.isnan(metrics["volatility_pct"])


def test_compute_strategy_metrics_applies_risk_free_alpha_formula() -> None:
    rows = [
        {"daily_return_pct": 1.0, "total_asset": 101000},
        {"daily_return_pct": 0.5, "total_asset": 101505},
    ]
    benchmark_returns = [0.004, 0.003]

    metrics = arena_dashboard._compute_strategy_metrics_from_rows(
        rows,
        benchmark_returns,
        risk_free_rate=0.02,
    )

    portfolio_returns = [0.01, 0.005]
    portfolio_mean = sum(portfolio_returns) / len(portfolio_returns)
    benchmark_mean = sum(benchmark_returns) / len(benchmark_returns)
    covariance = sum(
        (portfolio - portfolio_mean) * (benchmark - benchmark_mean)
        for portfolio, benchmark in zip(portfolio_returns, benchmark_returns)
    ) / len(portfolio_returns)
    variance = sum(
        (benchmark - benchmark_mean) ** 2 for benchmark in benchmark_returns
    ) / len(benchmark_returns)
    beta = covariance / variance
    annual_return = (1 + portfolio_mean) ** 252 - 1
    benchmark_annual = (1 + benchmark_mean) ** 252 - 1
    expected_alpha_pct = (annual_return - 0.02 - beta * (benchmark_annual - 0.02)) * 100
    assert metrics["alpha_pct"] == pytest.approx(expected_alpha_pct)
