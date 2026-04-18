"""
QMT 交易引擎 —— 封装 xtquant 的连接、下单、查询等操作。

使用前须确保 QMT 客户端（或 mini 版本）已启动并登录。
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Optional

from loguru import logger

from .config import get_config
from .models import OrderRecord, SignalDirection, TradeSignal




def _ensure_qmt_path(account_type: str = "simulation"):
    """延迟注入 QMT site-packages 到 sys.path（放在末尾，避免覆盖标准库）。"""
    from .config import get_account_config
    acc = get_account_config(account_type)
    qmt_path = acc.get("qmt_path", "")
    if not qmt_path:
        logger.warning(f"[QMT] 未配置 qmt_path for {account_type}")
        return
    # qmt_path 可能是 userdata_mini 目录，site-packages 在上级的 bin.x64 下
    # 也可能是 QMT 根目录
    p = Path(qmt_path)
    candidates = [
        p / "bin.x64" / "Lib" / "site-packages",
        p.parent / "bin.x64" / "Lib" / "site-packages",
    ]
    for sp in candidates:
        if sp.exists() and str(sp) not in sys.path:
            sys.path.append(str(sp))
            return


# 延迟导入 xtquant，避免模块加载时污染 sys.path
_xtconstant = None
_XtQuantTrader = None
_XtQuantTraderCallback = None
_StockAccount = None


def _get_xt_modules(account_type: str = "simulation"):
    global _xtconstant, _XtQuantTrader, _XtQuantTraderCallback, _StockAccount
    if _XtQuantTrader is None:
        _ensure_qmt_path(account_type)
        from xtquant import xtconstant
        from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
        from xtquant.xttype import StockAccount
        _xtconstant = xtconstant
        _XtQuantTrader = XtQuantTrader
        _XtQuantTraderCallback = XtQuantTraderCallback
        _StockAccount = StockAccount
    return _xtconstant, _XtQuantTrader, _XtQuantTraderCallback, _StockAccount


def _get_direction_map():
    _, _, _, _ = _get_xt_modules()  # noqa: ensure loaded
    return {
        SignalDirection.BUY: _xtconstant.STOCK_BUY,
        SignalDirection.SELL: _xtconstant.STOCK_SELL,
    }


def _get_price_type_map():
    _, _, _, _ = _get_xt_modules()
    return {
        "FIX_PRICE": _xtconstant.FIX_PRICE,
        "LATEST_PRICE": _xtconstant.LATEST_PRICE,
        "MARKET_SH_CONVERT_5_CANCEL": _xtconstant.MARKET_SH_CONVERT_5_CANCEL,
        "MARKET_SH_CONVERT_5_LIMIT": _xtconstant.MARKET_SH_CONVERT_5_LIMIT,
        "MARKET_PEER_PRICE_FIRST": _xtconstant.MARKET_PEER_PRICE_FIRST,
        "MARKET_MINE_PRICE_FIRST": _xtconstant.MARKET_MINE_PRICE_FIRST,
        "MARKET_SZ_INSTBUSI_RESTCANCEL": _xtconstant.MARKET_SZ_INSTBUSI_RESTCANCEL,
        "MARKET_SZ_CONVERT_5_CANCEL": _xtconstant.MARKET_SZ_CONVERT_5_CANCEL,
        "MARKET_SZ_FULL_OR_CANCEL": _xtconstant.MARKET_SZ_FULL_OR_CANCEL,
    }


class _TradeCallback:
    """交易回调，记录关键事件"""

    def on_connected(self):
        logger.info("[QMT] 连接成功")

    def on_disconnected(self):
        logger.warning("[QMT] 连接断开")

    def on_stock_order(self, order):
        logger.info(
            f"[QMT] 委托回报: {order.stock_code} "
            f"方向={order.order_type} 量={order.order_volume} "
            f"成交量={order.traded_volume} 状态={order.status_msg}"
        )

    def on_stock_trade(self, trade):
        logger.info(
            f"[QMT] 成交回报: {trade.stock_code} "
            f"成交量={trade.traded_volume} 成交价={trade.traded_price}"
        )

    def on_order_error(self, order_error):
        logger.error(
            f"[QMT] 委托错误: {order_error.order_id} "
            f"错误码={order_error.error_id} 信息={order_error.error_msg}"
        )

    def on_order_stock_async_response(self, response):
        logger.info(
            f"[QMT] 异步委托响应: order_id={response.order_id} "
            f"备注={response.order_remark}"
        )

    def on_account_status(self, status):
        logger.info(
            f"[QMT] 账号状态: {status.account_id} status={status.status}"
        )


class QmtEngine:
    """QMT 交易引擎"""

    def __init__(self, account_type: str = "simulation"):
        self.account_type = account_type
        self._trader: Optional[XtQuantTrader] = None
        self._account: Optional[StockAccount] = None
        self._callback = _TradeCallback()
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        """连接到 QMT 客户端"""
        from .config import get_account_config

        qmt_cfg = get_account_config(self.account_type)

        path = qmt_cfg["qmt_path"]
        session_id = qmt_cfg["session_id"]
        account_id = qmt_cfg["account_id"]
        account_type = qmt_cfg.get("account_type", "STOCK")

        if not account_id:
            logger.error("请在 config/settings.yaml 中设置 qmt.account_id")
            return False

        if not Path(path).exists():
            logger.error(f"QMT 路径不存在: {path}，请检查 QMT 客户端是否已安装")
            return False

        logger.info(f"正在连接 QMT: path={path}, session={session_id}")
        _, XtQuantTrader, _, StockAccount = _get_xt_modules(self.account_type)
        self._trader = XtQuantTrader(path, session_id)
        self._trader.register_callback(self._callback)
        self._trader.start()

        result = self._trader.connect()
        if result != 0:
            logger.error(f"QMT 连接失败, 错误码: {result}")
            self._connected = False
            return False

        self._account = StockAccount(account_id, account_type)
        sub_result = self._trader.subscribe(self._account)
        if sub_result != 0:
            logger.warning(
                f"订阅账号失败 (result={sub_result}): "
                f"account_id={account_id}。"
                f"尝试自动发现 QMT 账号..."
            )
            auto_id = self._discover_account_id()
            if auto_id:
                logger.info(f"自动发现账号: {auto_id}")
                _, _, _, StockAccount = _get_xt_modules()
                self._account = StockAccount(auto_id, account_type)
                sub_result = self._trader.subscribe(self._account)
                if sub_result == 0:
                    logger.info(f"自动发现订阅成功: {auto_id}")
                else:
                    logger.error(f"自动发现订阅仍失败，请检查 QMT 登录状态")
                    return False
            else:
                logger.error("未找到可用账号，请确认 QMT 客户端已登录")
                return False
        else:
            logger.info(f"订阅账号成功: {account_id}")

        self._connected = True
        logger.info("QMT 连接成功")
        return True

    def disconnect(self):
        """断开连接"""
        if self._trader:
            try:
                self._trader.stop()
            except Exception as e:
                logger.warning(f"断开连接异常: {e}")
        self._connected = False
        logger.info("QMT 已断开")

    def _discover_account_id(self) -> str | None:
        """通过 query_account_infos 自动发现 QMT 交易账号"""
        try:
            infos = self._trader.query_account_infos()
            if infos:
                return str(infos[0].account_id)
        except Exception as e:
            logger.warning(f"自动发现账号失败: {e}")
        return None

    def place_order(self, signal: TradeSignal) -> OrderRecord:
        """根据信号下单"""
        if not self._connected or not self._trader:
            raise RuntimeError("QMT 未连接")

        cfg = get_config()
        strategy_name = cfg["trading"]["strategy_name"]
        price_type_name = cfg["trading"]["default_price_type"]

        order_type = _get_direction_map()[signal.direction]
        price_type = _resolve_price_type(signal)
        price = signal.price

        record = OrderRecord(
            signal_id=signal.signal_id,
            stock_code=signal.stock_code,
            direction=signal.direction,
            order_volume=signal.volume,
        )

        try:
            logger.info(
                f"下单: {signal.stock_code} {signal.direction.value} "
                f"数量={signal.volume} 价格类型={price_type} 价格={price}"
            )
            order_id = self._trader.order_stock(
                self._account,
                signal.stock_code,
                order_type,
                signal.volume,
                price_type,
                price,
                strategy_name,
                signal.signal_id,
            )
            record.order_id = order_id
            record.status = "submitted"
            logger.info(f"下单成功: order_id={order_id}")
        except Exception as e:
            record.status = "error"
            record.error_msg = str(e)
            logger.error(f"下单失败: {e}")

        return record

    def query_asset(self):
        """查询账户资产"""
        if not self._connected or not self._trader:
            raise RuntimeError("QMT 未连接")
        return self._trader.query_stock_asset(self._account)

    def query_positions(self):
        """查询当前持仓"""
        if not self._connected or not self._trader:
            raise RuntimeError("QMT 未连接")
        return self._trader.query_stock_positions(self._account)

    def query_orders(self):
        """查询当日委托"""
        if not self._connected or not self._trader:
            raise RuntimeError("QMT 未连接")
        return self._trader.query_stock_orders(self._account)

    def query_trades(self):
        """查询当日成交"""
        if not self._connected or not self._trader:
            raise RuntimeError("QMT 未连接")
        return self._trader.query_stock_trades(self._account)

    def cancel_order(self, order_id: int) -> int:
        """撤单"""
        if not self._connected or not self._trader:
            raise RuntimeError("QMT 未连接")
        return self._trader.cancel_order_stock(self._account, order_id)


def _resolve_price_type(signal: TradeSignal) -> int:
    """根据信号和证券市场选择合适的报价类型。

    注意：模拟环境不支持市价类型（MARKET_*），
    市价类型只在实盘环境中生效。模拟环境应使用 LATEST_PRICE。
    """
    cfg = get_config()
    default_name = cfg["trading"]["default_price_type"]
    simulation_mode = True
    if "accounts" in cfg:
        for acc_key, acc_val in cfg["accounts"].items():
            if acc_val.get("enabled", False):
                simulation_mode = acc_val.get("simulation_mode", True)
                break

    _xtconstant = _get_xt_modules()[0]

    if signal.price > 0:
        return _xtconstant.FIX_PRICE

    if simulation_mode:
        return _xtconstant.LATEST_PRICE

    code = signal.stock_code
    if code.endswith(".SH"):
        return _xtconstant.MARKET_SH_CONVERT_5_CANCEL
    elif code.endswith(".SZ"):
        return _xtconstant.MARKET_SZ_CONVERT_5_CANCEL
    else:
        return _get_price_type_map().get(default_name, _xtconstant.LATEST_PRICE)
