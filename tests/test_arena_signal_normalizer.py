from datetime import date

from app.arena_signal_normalizer import normalize_ai_picks


def test_normalize_ai_picks_uses_estimated_order_price_for_volume() -> None:
    picks_result = {
        "provider": "test-provider",
        "picks": [{"stock_code": "300750.SZ", "confidence": 0.9}],
    }
    candidate_pool = [{"stock_code": "300750.SZ", "close": 100.0}]

    signals = normalize_ai_picks(
        picks_result=picks_result,
        candidate_pool=candidate_pool,
        total_capital=1_000_000,
        trade_date=date(2026, 4, 16),
    )

    assert len(signals) == 1
    assert signals[0].estimated_order_price == 120.0
    assert signals[0].volume == 8300


def test_normalize_ai_picks_bse_uses_30pct_limit() -> None:
    """BSE stocks should use 30% limit-up for volume sizing."""
    picks_result = {
        "provider": "test-provider",
        "picks": [{"stock_code": "830799.BJ", "confidence": 0.9}],
    }
    candidate_pool = [{"stock_code": "830799.BJ", "close": 10.0}]

    signals = normalize_ai_picks(
        picks_result=picks_result,
        candidate_pool=candidate_pool,
        total_capital=1_000_000,
        trade_date=date(2026, 4, 16),
    )

    assert len(signals) == 1
    assert signals[0].estimated_order_price == 13.0  # 10 * 1.30
