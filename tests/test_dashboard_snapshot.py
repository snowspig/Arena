from __future__ import annotations

from types import SimpleNamespace

from app.dashboard import _build_runtime_realtime


class ConnectedEngine:
    connected = True

    def query_asset(self) -> SimpleNamespace:
        return SimpleNamespace(
            total_asset=1000.0,
            cash=400.0,
            market_value=600.0,
            frozen_cash=50.0,
        )

    def query_positions(self) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(
                stock_code="600519.SH",
                volume=200,
                can_use_volume=100,
                open_price=2.5,
                market_value=700.0,
            )
        ]


def test_build_runtime_realtime_includes_latest_daily_pnl(monkeypatch) -> None:
    monkeypatch.setattr("app.dashboard._get_runtime_engine", lambda account_type: ConnectedEngine())
    monkeypatch.setattr("app.dashboard._get_latest_daily_pnl", lambda account_type: 123.45)

    realtime = _build_runtime_realtime("simulation")

    assert realtime is not None
    assert realtime["daily_pnl"] == 123.45
    assert realtime["asset"] == {
        "total_asset": 1000.0,
        "cash": 400.0,
        "market_value": 600.0,
        "frozen_cash": 50.0,
    }
    assert realtime["positions"] == [
        {
            "stock_code": "600519.SH",
            "volume": 200,
            "can_use_volume": 100,
            "cost_price": 2.5,
            "market_value": 700.0,
            "unrealized_pnl": 200.0,
        }
    ]


def test_realtime_api_injects_daily_pnl_into_proxy_response(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.dashboard import router

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    monkeypatch.setattr(
        "app.dashboard.get_config",
        lambda: {"accounts": {"simulation": {"enabled": True}}},
    )
    monkeypatch.setattr("app.dashboard._build_runtime_realtime", lambda account_type: None)
    import asyncio
    async def _mock_proxy(account_type, skip_runtime=True):
        return {
            "connected": True,
            "account_type": account_type,
            "asset": {"total_asset": 1000.0},
            "positions": [],
        }
    monkeypatch.setattr("app.dashboard._fetch_from_proxy", _mock_proxy)
    monkeypatch.setattr("app.dashboard._get_latest_daily_pnl", lambda account_type: 88.8)

    response = client.get("/api/dashboard/realtime?account=simulation")

    assert response.status_code == 200
    assert response.json()["daily_pnl"] == 88.8
