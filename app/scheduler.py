"""
交易调度器 —— 根据信号的 timing 类型在正确的时间点执行交易。

支持三种时间策略:
  1. open_auction  - 集合竞价阶段（9:25）买入
  2. close         - 收盘前N分钟（默认14:55）卖出
  3. vwap          - 在交易时段内每隔N分钟市价拆单
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from .arena_portfolio import cancel_unfilled_sells, closing_auction_sell, continuous_auction_sell
from .config import get_config
from .models import OrderRecord, SignalDirection, TimingType, TradeSignal
from .qmt_engine import QmtEngine
from .signal_client import fetch_signals


def _next_trade_day(current: date) -> date:
    """返回下一个交易日（跳过周末）。"""
    next_day = current + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day


class TradingScheduler:
    """交易调度器"""

    def __init__(self, engine: QmtEngine, account_type: str = "simulation"):
        self.engine = engine
        self.account_type = account_type
        self.scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
        self._order_records: list[OrderRecord] = []
        self._today_signals: list[TradeSignal] = []

    @property
    def order_records(self) -> list[OrderRecord]:
        return list(self._order_records)

    def start(self):
        """启动日常调度"""
        cfg = get_config()
        schedule_cfg = cfg["schedule"]
        signal_mode = cfg.get("signal_source", {}).get("mode", "mock")

        # Arena 收盘后生成次日信号: 15:30 首次生成，15:45-21:00 每 15 分钟补齐
        if signal_mode == "arena":
            self.scheduler.add_job(
                self._arena_signal_generate_next_day,
                CronTrigger(hour=18, minute=0, day_of_week="mon-fri"),
                id="arena_signal_next_day_1800",
                name="Arena次日信号生成(18:00)",
                replace_existing=True,
            )
            for hour in range(18, 21):
                for minute in (0, 15, 30, 45):
                    if hour == 18 and minute == 0:
                        continue  # 18:00 已覆盖
                    self.scheduler.add_job(
                        self._arena_signal_retry_next_day,
                        CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri"),
                        id=f"arena_signal_retry_next_{hour}{minute:02d}",
                        name=f"Arena次日信号补齐({hour}:{minute:02d})",
                        replace_existing=True,
                    )

            # 早盘兜底: 07:30-09:00 每 15 分钟（若收盘后未生成成功）
            for minute in (30, 45):
                self.scheduler.add_job(
                    self._arena_signal_retry,
                    CronTrigger(hour=7, minute=minute, day_of_week="mon-fri"),
                    id=f"arena_signal_retry_7{minute}",
                    name=f"Arena信号补齐(07:{minute})",
                    replace_existing=True,
                )
            for minute in (0, 15, 30, 45):
                self.scheduler.add_job(
                    self._arena_signal_retry,
                    CronTrigger(hour=8, minute=minute, day_of_week="mon-fri"),
                    id=f"arena_signal_retry_8{minute}",
                    name=f"Arena信号补齐(08:{minute})",
                    replace_existing=True,
                )
            self.scheduler.add_job(
                self._arena_signal_retry,
                CronTrigger(hour=9, minute=0, day_of_week="mon-fri"),
                id="arena_signal_retry_900",
                name="Arena信号补齐(09:00)",
                replace_existing=True,
            )

        # 09:10 获取当日信号（读取已生成的或现场生成）
        self.scheduler.add_job(
            self._morning_fetch_and_prepare,
            CronTrigger(
                hour=9, minute=10,
                day_of_week="mon-fri",
            ),
            id="morning_fetch",
            name="获取当日交易信号",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._arena_buy_orders,
            CronTrigger(
                hour=9, minute=15,
                day_of_week="mon-fri",
            ),
            id="arena_buy_orders",
            name="Arena合并买入",
            replace_existing=True,
        )

        # Phase 1: 14:55 连续竞价卖出
        self.scheduler.add_job(
            self._phase1_continuous_sell,
            CronTrigger(hour=14, minute=55, day_of_week="mon-fri"),
            id="sell_phase1",
            name="Phase1连续竞价卖出",
            replace_existing=True,
        )

        # Phase 2: 14:56:50 撤销未成交卖单
        self.scheduler.add_job(
            self._phase2_cancel_sells,
            CronTrigger(hour=14, minute=56, second=50, day_of_week="mon-fri"),
            id="sell_phase2_cancel",
            name="Phase2撤单",
            replace_existing=True,
        )

        # Phase 3: 14:57 收盘集合竞价卖出
        self.scheduler.add_job(
            self._phase3_closing_auction_sell,
            CronTrigger(hour=14, minute=57, day_of_week="mon-fri"),
            id="sell_phase3",
            name="Phase3收盘竞价卖出",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._arena_settle,
            CronTrigger(
                hour=15, minute=10,
                day_of_week="mon-fri",
            ),
            id="arena_settle",
            name="Arena日终结算",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._arena_review,
            CronTrigger(
                hour=16, minute=0,
                day_of_week="mon-fri",
            ),
            id="arena_review",
            name="Arena复盘点评",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._end_of_day_summary,
            CronTrigger(
                hour=15, minute=5,
                day_of_week="mon-fri",
            ),
            id="eod_summary",
            name="日终汇总",
            replace_existing=True,
        )

        self.scheduler.start()
        logger.info("交易调度器已启动")

    def stop(self):
        self.scheduler.shutdown(wait=False)
        logger.info("交易调度器已停止")

    def run_now(self, timing: TimingType | None = None):
        """手动立即执行一轮（用于测试/回测）"""
        self._morning_fetch_and_prepare()
        if timing is None or timing == TimingType.OPEN_AUCTION:
            self._arena_buy_orders()
        if timing is None or timing == TimingType.CLOSE:
            self._phase1_continuous_sell()

    # ---- 内部任务 ----

    def _morning_fetch_and_prepare(self):
        """每日早盘获取信号"""
        logger.info("=== 获取当日交易信号 ===")
        try:
            batch = self._fetch_signal_batch()
            self._today_signals = batch.signals
            logger.info(
                f"获取到 {len(batch.signals)} 条信号 "
                f"(batch_id={batch.batch_id})"
            )
            for s in batch.signals:
                logger.info(
                    f"  {s.signal_id}: {s.stock_code} "
                    f"{s.direction.value} {s.volume}股 "
                    f"timing={s.timing.value}"
                )
        except Exception as e:
            logger.error(f"获取信号失败: {e}")
            self._today_signals = []

    def _fetch_signal_batch(self):
        """根据 signal_source.mode 选择信号来源。"""
        cfg = get_config()
        mode = cfg.get("signal_source", {}).get("mode", "mock")

        if mode == "arena":
            return self._fetch_arena_signals()

        return fetch_signals(date.today())

    def _arena_signal_generate_next_day(self):
        """15:30 收盘后生成次日信号。"""
        next_day = _next_trade_day(date.today())
        logger.info(f"=== Arena 次日信号生成 (target={next_day}) ===")
        try:
            from .arena_runner import generate_daily_arena_signals
            batch = generate_daily_arena_signals(target_date=next_day)
            logger.info(
                f"次日信号生成完成: {len(batch.signals)} 条信号 "
                f"(batch={batch.batch_id})"
            )
        except Exception as e:
            logger.error(f"Arena 次日信号生成失败: {e}")

    def _arena_signal_retry_next_day(self):
        """15:45/16:00 补齐次日缺失 provider 的信号。"""
        next_day = _next_trade_day(date.today())
        if self._all_providers_have_signals(next_day):
            logger.info(f"[{next_day}] 所有 provider 信号已齐全，跳过补齐")
            return
        logger.info(f"=== Arena 次日信号补齐 (target={next_day}) ===")
        try:
            from .arena_runner import generate_daily_arena_signals
            batch = generate_daily_arena_signals(target_date=next_day)
            logger.info(
                f"次日信号补齐完成: {len(batch.signals)} 条信号 "
                f"(batch={batch.batch_id})"
            )
        except Exception as e:
            logger.error(f"Arena 次日信号补齐失败: {e}")

    def _arena_signal_retry(self):
        """07:30-09:00 每 15 分钟补齐缺失 provider 的信号（早盘兜底）。"""
        now = datetime.now()
        if now.hour == 7 and now.minute < 30:
            return
        logger.info(f"=== Arena 信号补齐 ({now.strftime('%H:%M')}) ===")
        try:
            from .arena_runner import generate_daily_arena_signals
            batch = generate_daily_arena_signals()
            logger.info(
                f"信号补齐完成: {len(batch.signals)} 条信号 "
                f"(batch={batch.batch_id})"
            )
        except Exception as e:
            logger.error(f"Arena 信号补齐失败: {e}")

    def _all_providers_have_signals(self, target_date: date) -> bool:
        """检查所有 enabled provider 是否已有信号。"""
        try:
            from .arena_portfolio import get_enabled_providers
            from .arena_runner import _persist_signals
            from pymongo import MongoClient
            from .config import get_mongo_db, get_mongo_uri
            uri = get_mongo_uri()
            db_name = get_mongo_db()
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            col = client[db_name]["arena_signals"]
            for provider in get_enabled_providers():
                doc = col.find_one({
                    "trade_date": target_date.isoformat(),
                    "provider": provider,
                })
                if not doc or doc.get("status") != "success":
                    return False
            return True
        except Exception as e:
            logger.warning(f"检查信号完整性失败: {e}")
            return False

    def _fetch_arena_signals(self) -> SignalBatch:
        """从执行队列读取信号（执行队列由 arena_signals 合并而来）。"""
        from .execution_queue import load_queue, populate_from_arena
        batch = load_queue(date.today(), account_type=self.account_type)
        if batch and batch.signals:
            logger.info(
                f"[{self.account_type}] 从执行队列读取 {len(batch.signals)} 条信号"
            )
            return batch

        logger.warning(f"[{self.account_type}] 执行队列为空，尝试从 arena_signals 填充")
        count = populate_from_arena(date.today(), account_type=self.account_type)
        if count > 0:
            return load_queue(date.today(), account_type=self.account_type)

        logger.error(f"[{self.account_type}] arena_signals 中也无可用信号")
        return SignalBatch(batch_id="empty", signals=[])

    def _execute_open_auction(self):
        """执行集合竞价阶段的买入信号"""
        signals = [
            s for s in self._today_signals
            if s.timing == TimingType.OPEN_AUCTION
        ]
        if not signals:
            logger.info("集合竞价阶段无待执行信号")
            return

        logger.info(f"=== 集合竞价执行 {len(signals)} 条信号 ===")
        for signal in signals:
            record = self.engine.place_order(signal)
            self._order_records.append(record)
            logger.info(
                f"集合竞价: {signal.stock_code} {signal.direction.value} "
                f"order_id={record.order_id} status={record.status}"
            )

    def _execute_close_sell(self):
        """执行收盘前卖出信号"""
        signals = [
            s for s in self._today_signals
            if s.timing == TimingType.CLOSE
        ]
        if not signals:
            logger.info("收盘阶段无待执行信号")
            return

        logger.info(f"=== 收盘卖出执行 {len(signals)} 条信号 ===")
        for signal in signals:
            record = self.engine.place_order(signal)
            self._order_records.append(record)
            logger.info(
                f"收盘卖出: {signal.stock_code} {signal.direction.value} "
                f"order_id={record.order_id} status={record.status}"
            )

    def _execute_vwap_slice(self):
        """VWAP 单次拆单执行（由定时触发）"""
        signals = [
            s for s in self._today_signals
            if s.timing == TimingType.VWAP
        ]
        if not signals:
            return

        cfg = get_config()
        total_minutes = 4 * 60  # 4小时交易时间
        interval = cfg["trading"]["vwap_interval_minutes"]
        num_slices = total_minutes // interval

        for signal in signals:
            slice_volume = max(100, (signal.volume // num_slices // 100) * 100)
            if slice_volume <= 0:
                continue

            slice_signal = signal.model_copy(update={"volume": slice_volume})
            record = self.engine.place_order(slice_signal)
            self._order_records.append(record)
            logger.info(
                f"VWAP拆单: {signal.stock_code} {signal.direction.value} "
                f"本次={slice_volume}股 order_id={record.order_id}"
            )

    def _execute_vwap_full(self):
        """VWAP 完整执行（测试用，一次性拆完所有份额）"""
        signals = [
            s for s in self._today_signals
            if s.timing == TimingType.VWAP
        ]
        if not signals:
            return

        cfg = get_config()
        interval = cfg["trading"]["vwap_interval_minutes"]
        total_minutes = 4 * 60
        num_slices = total_minutes // interval

        for signal in signals:
            remaining = signal.volume
            for i in range(num_slices):
                if remaining <= 0:
                    break
                slice_volume = min(
                    max(100, (signal.volume // num_slices // 100) * 100),
                    remaining,
                )
                if slice_volume <= 0:
                    break
                slice_signal = signal.model_copy(update={"volume": slice_volume})
                record = self.engine.place_order(slice_signal)
                self._order_records.append(record)
                remaining -= slice_volume
                logger.info(
                    f"VWAP拆单 [{i + 1}/{num_slices}]: {signal.stock_code} "
                    f"本次={slice_volume}股 剩余={remaining}股"
                )

    def _arena_buy_orders(self):
        """09:15 执行合并后的 Arena 买单。"""
        logger.info("=== Arena 合并买入 ===")
        try:
            from .execution_queue import load_queue, populate_from_arena

            count = populate_from_arena(date.today(), account_type=self.account_type)
            logger.info(f"[{self.account_type}] 执行队列刷新完成: {count} 条信号")

            batch = load_queue(date.today(), account_type=self.account_type)
            if not batch.signals:
                logger.info("合并执行队列为空，跳过买入")
                return

            self._today_signals = batch.signals
            success = 0
            for signal in batch.signals:
                record = self.engine.place_order(signal)
                self._order_records.append(record)
                if getattr(record, "status", "") == "submitted":
                    success += 1
                logger.info(
                    f"合并买入: {signal.stock_code} {signal.direction.value} "
                    f"order_id={record.order_id} status={record.status}"
                )

            logger.info(
                f"[{self.account_type}] Arena 合并买入完成: success={success}/{len(batch.signals)}"
            )
        except Exception as e:
            logger.error(f"Arena 合并买入失败: {e}")

    def _phase1_continuous_sell(self):
        """Phase 1: 14:55 连续竞价卖出全部非涨停仓位。"""
        logger.info("=== Phase 1: 连续竞价卖出 ===")
        try:
            count = continuous_auction_sell(self.engine, date.today())
            logger.info(f"Phase 1 完成: {count} 笔卖出")
        except Exception as e:
            logger.error(f"Phase 1 卖出失败: {e}")

    def _phase2_cancel_sells(self):
        """Phase 2: 14:56:50 撤销未成交卖单。"""
        logger.info("=== Phase 2: 撤销未成交卖单 ===")
        try:
            cancelled = cancel_unfilled_sells(self.engine)
            logger.info(f"Phase 2 完成: {cancelled} 笔撤单")
        except Exception as e:
            logger.error(f"Phase 2 撤单失败: {e}")

    def _phase3_closing_auction_sell(self):
        """Phase 3: 14:57 收盘集合竞价卖出剩余仓位。"""
        logger.info("=== Phase 3: 收盘集合竞价卖出 ===")
        try:
            count = closing_auction_sell(self.engine, date.today())
            logger.info(f"Phase 3 完成: {count} 笔卖出")
        except Exception as e:
            logger.error(f"Phase 3 卖出失败: {e}")

    def _arena_settle(self):
        """执行 Arena 日终结算。"""
        logger.info("=== Arena 日终结算 ===")
        try:
            from .arena_settlement import settle_all_agents
            settle_all_agents(date.today())
        except Exception as e:
            logger.error(f"Arena 结算失败: {e}")

    def _arena_review(self):
        """执行 Arena 日终复盘。"""
        logger.info("=== Arena 日终复盘 ===")
        try:
            from .arena_reviewer import review_all_agents
            review_all_agents(date.today())
        except Exception as e:
            logger.error(f"Arena 复盘失败: {e}")

    def _end_of_day_summary(self):
        """日终汇总"""
        logger.info("=== 日终交易汇总 ===")
        try:
            asset = self.engine.query_asset()
            if asset:
                logger.info(
                    f"账户资产: 总资产={asset.total_asset:.2f} "
                    f"可用={asset.cash:.2f} 持仓市值={asset.market_value:.2f}"
                )
        except Exception as e:
            logger.warning(f"查询资产失败: {e}")

        logger.info(f"今日委托记录: {len(self._order_records)} 笔")
        for r in self._order_records:
            logger.info(
                f"  {r.signal_id}: {r.stock_code} {r.direction.value} "
                f"量={r.order_volume} order_id={r.order_id} status={r.status}"
            )

        # 执行逆回购
        self._execute_reverse_repo()

        try:
            from .settlement import save_daily_snapshot
            save_daily_snapshot(self.engine, self.account_type)
        except Exception as e:
            logger.error(f"保存结算快照失败: {e}")

        self._today_signals = []
        self._order_records = []

    def _execute_reverse_repo(self) -> None:
        """收盘后执行 1 日逆回购（204001.SH）。"""
        cfg = get_config()
        acc_cfg = cfg.get("accounts", {}).get(self.account_type, {})

        if not acc_cfg.get("auto_reverse_repo_enabled", False):
            logger.info(f"账户 {self.account_type} 未开启自动逆回购，跳过")
            return

        min_amount = acc_cfg.get("reverse_repo_min_amount", 1000)
        try:
            asset = self.engine.query_asset()
            if not asset:
                logger.warning("查询资产失败，跳过逆回购")
                return
        except Exception as e:
            logger.warning(f"查询资产失败: {e}，跳过逆回购")
            return

        available_cash = float(asset.cash) if asset else 0
        usable = available_cash - min_amount
        if usable < 1000:
            logger.info(
                f"可用资金 {available_cash:.2f} 元，"
                f"扣除保留 {min_amount} 元后不足 1000 元，跳过逆回购"
            )
            return

        lots = int(usable / 1000)
        logger.info(
            f"=== 执行逆回购 204001.SH {lots} 手 ({lots * 1000} 元) ==="
        )

        try:
            repo_signal = TradeSignal(
                signal_id=f"repo-{date.today().isoformat()}",
                stock_code="204001.SH",
                direction=SignalDirection.BUY,
                volume=lots,
                price=0.0,
                timing=TimingType.CLOSE,
            )
            record = self.engine.place_order(repo_signal)
            if record and getattr(record, "order_id", -1) > 0:
                logger.info(f"逆回购委托成功: order_id={record.order_id}")
            else:
                logger.warning(f"逆回购委托未成功: {record}")
        except Exception as e:
            logger.warning(f"逆回购执行异常（不影响后续流程）: {e}")
