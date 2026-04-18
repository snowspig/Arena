"""
QLiBRD-QMT 模拟交易系统 —— 主入口

用法:
    实盘模拟:  python main.py trade
    手动测试:  python main.py test
    回测:      python main.py backtest [--file data/history_signals.csv] [--mode live_replay|paper_calc]
    生成示例:  python main.py generate-sample
    启动API:   python main.py api
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path

from loguru import logger


def setup_logging():
    from app.config import get_config
    cfg = get_config()
    log_cfg = cfg["logging"]

    log_dir = Path(log_cfg["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.remove()
    logger.add(sys.stderr, level=log_cfg["level"], format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ))
    logger.add(
        str(log_dir / "qlibrd_{time:YYYY-MM-DD}.log"),
        level=log_cfg["level"],
        rotation=log_cfg["rotation"],
        retention=log_cfg["retention"],
        encoding="utf-8",
    )


def cmd_trade(args):
    """启动实盘模拟交易"""
    from app.qmt_engine import QmtEngine
    from app.scheduler import TradingScheduler

    account_type = args.account or "simulation"
    engine = QmtEngine(account_type)
    if not engine.connect():
        logger.error("QMT 连接失败，请检查客户端是否启动并确认 account_id 配置")
        sys.exit(1)

    scheduler = TradingScheduler(engine, account_type)

    def graceful_exit(signum, frame):
        logger.info("收到退出信号，正在停止...")
        scheduler.stop()
        engine.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    scheduler.start()
    logger.info("系统已启动, 等待定时任务触发... (Ctrl+C 退出)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        graceful_exit(None, None)


def cmd_test(args):
    """手动执行一轮测试（立即获取信号并下单）"""
    from app.config import get_account_config, get_config
    from app.models import TimingType
    from app.qmt_engine import QmtEngine
    from app.scheduler import TradingScheduler

    account_type = args.account or "simulation"
    engine = QmtEngine(account_type)
    if not engine.connect():
        cfg = get_config()
        qmt_cfg = get_account_config(account_type)
        logger.error(
            f"QMT 连接失败: path={qmt_cfg['qmt_path']}, "
            f"account_id={'已配置' if qmt_cfg.get('account_id') else '未配置'}"
        )
        sys.exit(1)

    try:
        scheduler = TradingScheduler(engine, account_type)
        timing = TimingType(args.timing) if args.timing else None
        scheduler.run_now(timing)

        logger.info(f"\n执行完成, 共 {len(scheduler.order_records)} 笔委托:")
        for r in scheduler.order_records:
            logger.info(
                f"  {r.stock_code} {r.direction.value} "
                f"量={r.order_volume} order_id={r.order_id} "
                f"status={r.status} {r.error_msg}"
            )
    finally:
        engine.disconnect()


def cmd_backtest(args):
    """执行回测"""
    from app.backtest import Backtester
    from app.qmt_engine import QmtEngine

    mode = args.mode or "paper_calc"
    engine = None

    if mode == "live_replay":
        engine = QmtEngine()
        if not engine.connect():
            logger.error("QMT 连接失败")
            sys.exit(1)

    try:
        bt = Backtester(engine)
        bt.load_signals(args.file)
        bt.run(start_date=args.start, end_date=args.end, mode=mode)
        bt.report()

        if args.output:
            bt.export_records(args.output)
    finally:
        if engine:
            engine.disconnect()


def cmd_generate_sample(args):
    """生成示例历史信号文件"""
    from app.backtest import generate_sample_history
    output = args.output or "data/history_signals.csv"
    generate_sample_history(output)


def cmd_proxy(args):
    """启动 QMT 代理服务（独立进程）"""
    import uvicorn
    from app.config import get_account_config

    account_type = args.account or "simulation"
    acc_cfg = get_account_config(account_type)
    proxy_port = acc_cfg.get("proxy_port", 8011)

    def on_startup():
        from app.qmt_proxy import init_engine
        init_engine(account_type)

    from app.qmt_proxy import app as proxy_app
    proxy_app.add_event_handler("startup", on_startup)

    uvicorn.run(
        proxy_app,
        host="127.0.0.1",
        port=proxy_port,
        log_level="info",
    )


def cmd_proxies(args):
    """启动已启用账户的 QMT 代理和 dashboard 服务"""
    import subprocess
    from app.config import get_config

    cfg = get_config()
    procs = []

    def spawn(label: str, cmd_args: list[str]):
        print(f"启动 {label}...")
        p = subprocess.Popen(cmd_args)
        procs.append((label, p))

    for acc_type in ("simulation", "live"):
        acc_cfg = cfg.get("accounts", {}).get(acc_type, {})
        if acc_cfg.get("enabled", False):
            spawn(f"QMT代理-{acc_type}", [sys.executable, "main.py", "proxy", "--account", acc_type])

    spawn("Dashboard", [sys.executable, "main.py", "dashboard"])

    print("所有服务已启动，Ctrl+C 停止全部")
    try:
        for _, p in procs:
            p.wait()
    except KeyboardInterrupt:
        print("\n停止所有服务...")
        for _, p in procs:
            p.terminate()
        for _, p in procs:
            p.wait()


def cmd_api(args):
    """启动模拟信号 API 服务"""
    import uvicorn
    uvicorn.run(
        "app.signal_api:app",
        host=args.host or "0.0.0.0",
        port=args.port or 8000,
        reload=True,
    )


def cmd_serve(args):
    """启动完整服务：Dashboard + QMT代理 + 交易调度（单进程）"""
    import threading
    import uvicorn
    from app.config import get_config, get_account_config

    cfg = get_config()

    # 1. 启动 QMT 交易调度器
    def run_trade():
        from app.dashboard import set_runtime_engine
        from app.qmt_engine import QmtEngine
        from app.scheduler import TradingScheduler

        account_type = args.account or "simulation"
        engine = QmtEngine(account_type)
        if not engine.connect():
            logger.error("QMT 连接失败，请检查客户端是否启动")
            return

        # 注册内嵌引擎供 dashboard 使用（serve 模式）
        set_runtime_engine(engine, account_type)

        scheduler = TradingScheduler(engine, account_type)
        scheduler.start()
        logger.info("交易调度器已启动")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            scheduler.stop()
            set_runtime_engine(None, account_type)
            engine.disconnect()

    trade_thread = threading.Thread(target=run_trade, daemon=True)
    trade_thread.start()
    time.sleep(2)  # 等待调度器初始化

    # 1b. 副账户：serve 模式下同一进程无法创建两个 trader，
    #     副账户需要通过独立 proxy 进程获取数据。
    #     自动在后台启动副账户 proxy
    import subprocess
    primary = args.account or "simulation"
    secondary = "live" if primary == "simulation" else "simulation"
    sec_cfg = cfg.get("accounts", {}).get(secondary, {})
    proxy_proc = None
    if sec_cfg.get("enabled", False):
        logger.info(f"正在后台启动副账户 {secondary} 的 QMT 代理...")
        proxy_proc = subprocess.Popen(
            [sys.executable, "main.py", "proxy", "--account", secondary],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    # 2. 启动 Dashboard + API (同一端口)
    def run_dashboard():
        uvicorn.run(
            "app.signal_api:app",
            host="0.0.0.0",
            port=args.port or 8010,
            log_level="info",
        )

    dashboard_thread = threading.Thread(target=run_dashboard, daemon=True)
    dashboard_thread.start()

    logger.info("完整服务已启动: Dashboard=8010, 交易调度=后台")
    logger.info("访问 http://localhost:8010 查看 Dashboard")

    # 保持主线程存活
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到退出信号，正在停止...")
        if proxy_proc:
            proxy_proc.terminate()
            proxy_proc.wait()


def cmd_dashboard(args):
    """启动 Arena dashboard 服务"""
    import uvicorn

    uvicorn.run(
        "app.signal_api:app",
        host=args.host or "0.0.0.0",
        port=args.port or 8010,
        log_level="info",
    )


def cmd_arena_buy(args):
    """手动触发 Arena 合并买单。"""
    from datetime import date

    from app.execution_queue import load_queue, populate_from_arena
    from app.qmt_engine import QmtEngine

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    account_type = args.account or "simulation"
    engine = QmtEngine(account_type)
    if not engine.connect():
        logger.error("QMT 连接失败，请检查客户端是否启动并确认 account_id 配置")
        sys.exit(1)

    try:
        populate_from_arena(target_date, account_type=account_type)
        batch = load_queue(target_date, account_type=account_type)
        success = 0
        for signal in batch.signals:
            record = engine.place_order(signal)
            if getattr(record, "status", "") == "submitted":
                success += 1
        logger.info(f"Arena 合并买单执行完成: success={success}/{len(batch.signals)}")
    finally:
        engine.disconnect()


def cmd_arena_sell(args):
    """手动触发 Arena 三阶段卖出。"""
    from datetime import date

    from app.arena_portfolio import (
        cancel_unfilled_sells,
        closing_auction_sell,
        continuous_auction_sell,
    )
    from app.qmt_engine import QmtEngine

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    account_type = args.account or "simulation"
    engine = QmtEngine(account_type)
    if not engine.connect():
        logger.error("QMT 连接失败，请检查客户端是否启动并确认 account_id 配置")
        sys.exit(1)

    try:
        phase = args.phase or "all"

        if phase in ("all", "1"):
            count = continuous_auction_sell(engine, target_date)
            logger.info(f"Phase 1 连续竞价卖出: {count} 笔")

        if phase in ("all", "2"):
            cancelled = cancel_unfilled_sells(engine)
            logger.info(f"Phase 2 撤单: {cancelled} 笔")

        if phase in ("all", "3"):
            count = closing_auction_sell(engine, target_date)
            logger.info(f"Phase 3 收盘竞价卖出: {count} 笔")

        logger.info("Arena 三阶段卖出完成")
    finally:
        engine.disconnect()


def cmd_arena_signals(args):
    """手动触发 Arena 信号生成（不执行交易）"""
    from datetime import date
    from app.arena_runner import generate_daily_arena_signals

    target_date = date.fromisoformat(args.date) if args.date else None
    batch = generate_daily_arena_signals(
        target_date=target_date,
        provider_name=args.provider,
    )
    print(f"\n生成完成: batch_id={batch.batch_id}, {len(batch.signals)} 条信号")
    for s in batch.signals:
        print(f"  {s.stock_code} {s.direction.value} {s.volume}股 reason={s.reason}")


def cmd_arena_compare(args):
    """对比各 Agent 的选股结果"""
    from datetime import date
    from app.arena_comparison import compare_providers, format_comparison_report

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    report = compare_providers(target_date=target_date)
    print(format_comparison_report(report))


def cmd_arena_review(args):
    """复盘各 Agent 的 T-1 选股表现"""
    from datetime import date

    from app.arena_reviewer import review_all_agents

    target = date.fromisoformat(args.date) if args.date else date.today()
    reviews = review_all_agents(target)
    for review in reviews:
        valid = "有效" if review.get("valid") else "无效"
        print(
            f"\n[{review['provider']}] ({valid}) "
            f"命中率={review.get('hit_rate', 0):.0%} "
            f"超额={review.get('excess_return', 0):.2f}% "
            f"经验={len(review.get('lessons', []))}条"
        )
        for lesson in review.get("lessons", []):
            print(f"  - {lesson}")


def cmd_arena_settle(args):
    """手动触发 Arena 每日结算。"""
    from datetime import date
    from app.arena_settlement import settle_all_agents
    from app.qmt_engine import QmtEngine

    target = date.fromisoformat(args.date) if args.date else date.today()
    account_type = args.account or "simulation"
    engine = QmtEngine(account_type)
    connected = engine.connect()
    if not connected:
        logger.warning("QMT 未连接，结算将无法读取真实持仓（engine=None）")

    try:
        summary = settle_all_agents(target, engine=engine)
        print(f"\n结算完成: {summary}")
    finally:
        if connected:
            engine.disconnect()


def main():
    from app.config import load_config
    load_config()
    setup_logging()

    parser = argparse.ArgumentParser(
        description="QLiBRD-QMT 模拟交易系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # trade
    sp_trade = subparsers.add_parser("trade", help="启动实盘模拟交易(定时调度)")
    sp_trade.add_argument("--account", choices=["simulation", "live"], default="simulation")
    sp_trade.set_defaults(func=cmd_trade)

    # test
    sp_test = subparsers.add_parser("test", help="手动执行一轮测试")
    sp_test.add_argument("--account", choices=["simulation", "live"], default="simulation")
    sp_test.add_argument("--timing", choices=["open_auction", "close", "vwap"], default=None)
    sp_test.set_defaults(func=cmd_test)

    # backtest
    sp_bt = subparsers.add_parser("backtest", help="执行历史信号回测")
    sp_bt.add_argument("--file", "-f", default="data/history_signals.csv", help="历史信号CSV路径")
    sp_bt.add_argument("--mode", "-m", choices=["live_replay", "paper_calc"], default="paper_calc")
    sp_bt.add_argument("--start", "-s", default=None, help="开始日期 YYYY-MM-DD")
    sp_bt.add_argument("--end", "-e", default=None, help="结束日期 YYYY-MM-DD")
    sp_bt.add_argument("--output", "-o", default=None, help="导出记录CSV路径")
    sp_bt.set_defaults(func=cmd_backtest)

    # generate-sample
    sp_gen = subparsers.add_parser("generate-sample", help="生成示例历史信号文件")
    sp_gen.add_argument("--output", "-o", default="data/history_signals.csv")
    sp_gen.set_defaults(func=cmd_generate_sample)

    # api
    sp_api = subparsers.add_parser("api", help="启动模拟信号API服务")
    sp_api.add_argument("--host", default="0.0.0.0")
    sp_api.add_argument("--port", type=int, default=8010)
    sp_api.set_defaults(func=cmd_api)

    # dashboard
    sp_dashboard = subparsers.add_parser("dashboard", help="启动Arena dashboard服务")
    sp_dashboard.add_argument("--host", default="0.0.0.0")
    sp_dashboard.add_argument("--port", type=int, default=8010)
    sp_dashboard.set_defaults(func=cmd_dashboard)

    # serve (all-in-one)
    sp_serve = subparsers.add_parser("serve", help="启动完整服务(Dashboard+交易调度,单进程)")
    sp_serve.add_argument("--account", choices=["simulation", "live"], default="simulation")
    sp_serve.add_argument("--port", type=int, default=8010)
    sp_serve.set_defaults(func=cmd_serve)

    # proxy
    sp_proxy = subparsers.add_parser("proxy", help="启动QMT代理服务(独立进程)")
    sp_proxy.add_argument("--account", choices=["simulation", "live"], default="simulation")
    sp_proxy.set_defaults(func=cmd_proxy)

    # proxies
    sp_proxies = subparsers.add_parser("proxies", help="同时启动模拟盘+实盘QMT代理")
    sp_proxies.set_defaults(func=cmd_proxies)

    # arena-buy
    sp_arena_buy = subparsers.add_parser("arena-buy", help="手动触发Arena模拟买单")
    sp_arena_buy.add_argument("--date", "-d", default=None, help="交易日期 YYYY-MM-DD")
    sp_arena_buy.add_argument("--account", choices=["simulation", "live"], default="simulation")
    sp_arena_buy.set_defaults(func=cmd_arena_buy)

    # arena-sell
    sp_arena_sell = subparsers.add_parser("arena-sell", help="手动触发Arena三阶段卖出")
    sp_arena_sell.add_argument("--date", "-d", default=None, help="交易日期 YYYY-MM-DD")
    sp_arena_sell.add_argument("--account", choices=["simulation", "live"], default="simulation")
    sp_arena_sell.add_argument(
        "--phase",
        choices=["1", "2", "3", "all"],
        default="all",
        help="执行阶段: 1=连续竞价 2=撤单 3=收盘竞价 all=全部",
    )
    sp_arena_sell.set_defaults(func=cmd_arena_sell)

    # arena-signals
    sp_arena = subparsers.add_parser("arena-signals", help="手动触发Arena信号生成")
    sp_arena.add_argument("--date", "-d", default=None, help="交易日期 YYYY-MM-DD")
    sp_arena.add_argument("--provider", "-p", default=None, help="指定provider名称")
    sp_arena.set_defaults(func=cmd_arena_signals)

    # arena-compare
    sp_cmp = subparsers.add_parser("arena-compare", help="对比各Agent选股结果")
    sp_cmp.add_argument("--date", "-d", default=None, help="交易日期 YYYY-MM-DD")
    sp_cmp.set_defaults(func=cmd_arena_compare)

    # arena-settle
    sp_settle = subparsers.add_parser("arena-settle", help="执行Arena每日结算")
    sp_settle.add_argument("--date", "-d", default=None, help="交易日期 YYYY-MM-DD")
    sp_settle.add_argument("--account", choices=["simulation", "live"], default="simulation")
    sp_settle.set_defaults(func=cmd_arena_settle)

    # arena-review
    sp_review = subparsers.add_parser("arena-review", help="复盘各Agent选股表现")
    sp_review.add_argument("--date", "-d", default=None, help="复盘日期 YYYY-MM-DD")
    sp_review.set_defaults(func=cmd_arena_review)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
