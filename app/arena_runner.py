"""AI Signal Arena —— 每日信号生成编排。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from loguru import logger
from pymongo import MongoClient

from .arena_ai_providers import AISignalProvider, create_provider_from_config
from .arena_market_brief import build_market_brief
from .arena_prompt_builder import build_pro_trader_prompt
from .arena_signal_normalizer import normalize_ai_picks
from .config import get_config, get_mongo_db, get_mongo_uri
from .models import SignalBatch, SignalDirection, TimingType, TradeSignal

_DEFAULT_POOL_SIZE = 250
_DEFAULT_CAPITAL_PER_AGENT = 5_000_000


def generate_daily_arena_signals(
    target_date: date | None = None,
    provider_name: str | None = None,
    total_capital: float | None = None,
) -> SignalBatch:
    """编排: briefing → prompt → provider → normalize → persist → return。

    当 provider_name 为 None 时，遍历所有 enabled 的 provider，
    共享同一份 briefing，各自独立生成信号。

    Args:
        target_date: 交易日期，默认今天。
        provider_name: 指定单个 provider，None 表示全部 enabled 的。
        total_capital: 分配给该 agent 的资金（元），None 从配置读。

    Returns:
        仅 execution_provider 的信号作为 SignalBatch 返回（用于下单）。
    """
    cfg = get_config()
    arena_cfg = cfg.get("arena", {})
    execution_provider = arena_cfg.get("execution_provider", "vllm_trader_pro")

    # 1. briefing — 所有 agent 共享同一份
    brief = build_market_brief(target_date=target_date, pool_size=_DEFAULT_POOL_SIZE)
    candidate_pool = brief.get("candidate_pool", [])
    if not candidate_pool:
        logger.warning("候选池为空，跳过信号生成")
        return _empty_batch(date.today(), execution_provider)

    # 优先使用传入的 target_date，brief 里的 trade_date 仅作 fallback
    trade_date = target_date or date.today()
    trade_date_str = trade_date.isoformat()

    # 2. prompt — 默认基于完整候选池
    default_system_prompt, default_user_prompt = build_pro_trader_prompt(brief)

    # 3. 确定要运行的 providers
    if provider_name is not None:
        providers_to_run = [provider_name]
    else:
        providers_to_run = _get_enabled_providers(arena_cfg)

    execution_signals = []

    # 预查：哪些 provider 今天已经有有效信号（跳过）
    already_done = _get_persisted_providers(trade_date)

    for pname in providers_to_run:
        if pname in already_done:
            logger.info(f"[{pname}] 已有当日有效信号 ({already_done[pname]} 条)，跳过")
            if pname == execution_provider:
                # 从已有记录中恢复 execution_provider 的信号
                execution_signals = _load_provider_signals(trade_date, pname)
            continue

        capital = total_capital or _get_dynamic_capital(provider_name=pname)
        provider_pool_size = _get_pool_size(arena_cfg, pname)

        # 如果 provider 有自定义 pool_size，截取候选池并重建 prompt
        if provider_pool_size < len(candidate_pool):
            pool_for_agent = candidate_pool[:provider_pool_size]
            agent_brief = dict(brief, candidate_pool=pool_for_agent)
            sys_prompt, usr_prompt = build_pro_trader_prompt(
                agent_brief, provider_name=pname,
            )
        else:
            pool_for_agent = candidate_pool
            sys_prompt, usr_prompt = build_pro_trader_prompt(
                brief, provider_name=pname,
            )

        logger.info(
            f"=== Arena Agent: {pname} | capital={capital:,.0f} | pool={len(pool_for_agent)} ==="
        )

        signals = _run_single_agent(
            pname=pname,
            capital=capital,
            target_date=trade_date,
            candidate_pool=pool_for_agent,
            system_prompt=sys_prompt,
            user_prompt=usr_prompt,
        )

        if pname == execution_provider:
            execution_signals = signals

    batch = SignalBatch(
        batch_id=f"arena-{execution_provider}-{trade_date.isoformat()}",
        signals=execution_signals,
    )
    logger.info(
        f"=== Arena 完成: trade_date={trade_date.isoformat()}, "
        f"execution_provider={execution_provider}, "
        f"{len(execution_signals)} 条信号 (batch={batch.batch_id}) ==="
    )
    return batch


def _run_single_agent(
    pname: str,
    capital: float,
    target_date: date,
    candidate_pool: list[dict[str, Any]],
    system_prompt: str,
    user_prompt: str,
) -> list[Any]:
    """运行单个 agent: provider → normalize → persist。"""
    try:
        provider = create_provider_from_config(pname)
        picks_result = provider.generate_picks(system_prompt, user_prompt, target_date)

        if picks_result.get("fallback_detected", False):
            logger.warning(
                f"[{pname}] 模型身份不一致: "
                f"请求={picks_result.get('requested_model')}, "
                f"实际={picks_result.get('actual_model')}, "
                f"但信号已解析 ({len(picks_result.get('picks', []))} 条)，保留信号"
            )

        signals = normalize_ai_picks(
            picks_result=picks_result,
            candidate_pool=candidate_pool,
            total_capital=capital,
            trade_date=target_date,
        )

        _persist_signals(
            target_date, pname, signals, picks_result,
            system_prompt=system_prompt, user_prompt=user_prompt,
        )
        return signals

    except Exception as e:
        logger.error(f"[{pname}] agent 运行失败: {e}")
        return []


def _get_enabled_providers(arena_cfg: dict[str, Any]) -> list[str]:
    """从配置中获取所有 enabled 的 provider 名称。"""
    providers_cfg = arena_cfg.get("providers", {})
    return [
        name
        for name, pcfg in providers_cfg.items()
        if pcfg.get("enabled", True)
    ]


def _get_capital_pool(arena_cfg: dict[str, Any], provider_name: str) -> float:
    """从配置中读取 provider 的 capital_pool。"""
    providers_cfg = arena_cfg.get("providers", {})
    pcfg = providers_cfg.get(provider_name, {})
    return float(pcfg.get("capital_pool", _DEFAULT_CAPITAL_PER_AGENT))


def _get_pool_size(arena_cfg: dict[str, Any], provider_name: str) -> int:
    """从配置中读取 provider 的 pool_size，默认用全局 DEFAULT_POOL_SIZE。"""
    providers_cfg = arena_cfg.get("providers", {})
    pcfg = providers_cfg.get(provider_name, {})
    return int(pcfg.get("pool_size", _DEFAULT_POOL_SIZE))


def _persist_signals(
    trade_date: date,
    provider_name: str,
    signals: list[Any],
    picks_result: dict[str, Any],
    system_prompt: str = "",
    user_prompt: str = "",
) -> None:
    """持久化 arena 信号到 MongoDB，包含完整的 prompt 和模型响应。"""
    cfg = get_config()
    uri = get_mongo_uri()
    db_name = get_mongo_db()

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["arena_signals"]
        # 读取虚拟账户资金用于展示
        capital_info = {}
        try:
            account_col = client[db_name]["arena_accounts"]
            account_doc = account_col.find_one({"provider": provider_name})
            if account_doc:
                capital_info = {
                    "total_asset": float(account_doc.get("total_asset", 0.0) or 0.0),
                    "cash": float(account_doc.get("cash", 0.0) or 0.0),
                    "market_value": float(account_doc.get("market_value", 0.0) or 0.0),
                    "deployable": min(
                        float(account_doc.get("total_asset", 0.0) or 0.0) / 2,
                        float(account_doc.get("cash", 0.0) or 0.0),
                    ),
                }
        except Exception:
            pass

        doc = {
            "trade_date": trade_date.isoformat(),
            "provider": provider_name,
            "signal_count": len(signals),
            "signals": [s.model_dump(mode="json") for s in signals],
            "raw_picks": picks_result.get("picks", []),
            "raw_response": picks_result.get("raw_response", ""),
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "requested_model": picks_result.get("requested_model"),
            "actual_model": picks_result.get("actual_model"),
            "base_url": picks_result.get("base_url"),
            "attempts": picks_result.get("attempts", 0),
            "status": picks_result.get("status", "unknown"),
            "fallback_detected": picks_result.get("fallback_detected", False),
            "error_message": picks_result.get("error_message", ""),
            "parse_mode": picks_result.get("parse_mode", ""),
            "response_truncated": picks_result.get("response_truncated", False),
            "generated_at": datetime.now().isoformat(),
            "capital": capital_info,
        }
        col.replace_one(
            {"trade_date": trade_date.isoformat(), "provider": provider_name},
            doc,
            upsert=True,
        )
        logger.info(f"arena_signals 已持久化: {provider_name} / {trade_date}")
    except Exception as e:
        logger.error(f"arena_signals 持久化失败: {e}")


def load_execution_signals(target_date: date | None = None) -> SignalBatch | None:
    """从 MongoDB 读取已生成的 execution_provider 信号。

    Args:
        target_date: 交易日期，默认今天。

    Returns:
        SignalBatch 或 None（如无记录）。
    """
    cfg = get_config()
    arena_cfg = cfg.get("arena", {})
    execution_provider = arena_cfg.get("execution_provider", "vllm_trader_pro")
    target_date = target_date or date.today()

    uri = get_mongo_uri()
    db_name = get_mongo_db()

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["arena_signals"]
        doc = col.find_one({
            "trade_date": target_date.isoformat(),
            "provider": execution_provider,
        })
    except Exception as e:
        logger.error(f"读取 arena_signals 失败: {e}")
        return None

    if not doc:
        return None

    signals = []
    for s in doc.get("signals", []):
        try:
            signals.append(TradeSignal.model_validate(s))
        except Exception as e:
            logger.warning(f"解析信号失败: {e}")

    return SignalBatch(
        batch_id=f"arena-{execution_provider}-{target_date.isoformat()}",
        signals=signals,
    )


def load_merged_signals(target_date: date | None = None) -> SignalBatch | None:
    """从 MongoDB 读取所有 enabled provider 的信号并合并。同股数量叠加。"""
    from pymongo import MongoClient
    from .arena_portfolio import get_enabled_providers

    cfg = get_config()
    uri = get_mongo_uri()
    db_name = get_mongo_db()
    target_date = target_date or date.today()

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["arena_signals"]
        cursor = col.find({
            "trade_date": target_date.isoformat(),
            "provider": {"$in": get_enabled_providers()},
        })
    except Exception as e:
        logger.error(f"读取 arena_signals 失败: {e}")
        return None

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
        return None

    signals = []
    for code, data in merged.items():
        if data["price"] <= 0:
            continue
        agents = providers_map[code]
        signals.append(TradeSignal(
            signal_id=f"arena-merged-{target_date.isoformat()}-{code}",
            stock_code=code,
            direction=SignalDirection.BUY,
            volume=data["volume"],
            price=data["price"],
            timing=TimingType.OPEN_AUCTION,
            signal_date=target_date,
            reason=f"merged {len(agents)} agents: {', '.join(agents)}",
        ))

    if not signals:
        return None

    return SignalBatch(
        batch_id=f"arena-merged-{target_date.isoformat()}",
        signals=signals,
    )


def _get_configured_avg_capital() -> float:
    """从配置中计算 enabled provider 的 capital_pool 平均值。"""
    providers_cfg = get_config().get("arena", {}).get("providers", {})
    pools = [float(p.get("capital_pool", _DEFAULT_CAPITAL_PER_AGENT))
             for p in providers_cfg.values() if p.get("enabled", True)]
    return sum(pools) / max(len(pools), 1)


def _get_dynamic_capital(provider_name: str | None = None) -> float:
    """读取 provider 虚拟账户可用资金。"""
    try:
        from pymongo import MongoClient
        uri = get_mongo_uri()
        db_name = get_mongo_db()
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["arena_accounts"]

        if provider_name:
            doc = col.find_one({"provider": provider_name})
            if doc:
                cash = float(doc.get("cash", 0.0) or 0.0)
                total_asset = float(doc.get("total_asset", 0.0) or 0.0)
                deployable = min(total_asset / 2, cash)
                logger.info(
                    f"[{provider_name}] 虚拟账户: "
                    f"total_asset={total_asset:,.0f} cash={cash:,.0f} "
                    f"deployable={deployable:,.0f}"
                )
                return deployable
            logger.warning(
                f"[{provider_name}] 未找到虚拟账户，回退到配置 capital_pool"
            )
            return _get_capital_pool(cfg.get("arena", {}), provider_name)

        # 无 provider 时回退到配置平均值（兼容旧调用）
        return _get_configured_avg_capital()
    except Exception as e:
        logger.warning(f"动态资金计算失败，回退到配置: {e}")
        if provider_name:
            return _get_capital_pool(get_config().get("arena", {}), provider_name)
        return _get_configured_avg_capital()


def _empty_batch(trade_date: date, provider_name: str) -> SignalBatch:
    return SignalBatch(
        batch_id=f"arena-{provider_name}-{trade_date.isoformat()}",
        signals=[],
    )


def _get_persisted_providers(
    trade_date: date,
) -> dict[str, int]:
    """查询当天已有有效信号的 provider。

    Returns:
        {provider_name: signal_count}，仅包含 signal_count > 0 的。
    """
    cfg = get_config()
    uri = get_mongo_uri()
    db_name = get_mongo_db()

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["arena_signals"]
        docs = col.find(
            {"trade_date": trade_date.isoformat(), "signal_count": {"$gt": 0}},
            {"provider": 1, "signal_count": 1, "_id": 0},
        )
        return {d["provider"]: d["signal_count"] for d in docs}
    except Exception as e:
        logger.warning(f"查询已持久化 provider 失败: {e}")
        return {}


def _load_provider_signals(
    trade_date: date,
    provider_name: str,
) -> list[TradeSignal]:
    """从 MongoDB 加载指定 provider 当天的信号。"""
    cfg = get_config()
    uri = get_mongo_uri()
    db_name = get_mongo_db()

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["arena_signals"]
        doc = col.find_one({
            "trade_date": trade_date.isoformat(),
            "provider": provider_name,
        })
    except Exception as e:
        logger.warning(f"加载 {provider_name} 信号失败: {e}")
        return []

    if not doc:
        return []

    signals = []
    for s in doc.get("signals", []):
        try:
            signals.append(TradeSignal.model_validate(s))
        except Exception as e:
            logger.warning(f"解析信号失败: {e}")
    return signals
