from collections import defaultdict

from typing import Callable, Dict, Optional
from core.event import Event, EventEngine
from datastructure.object import OrderData, OrderRequest, LogData, TradeData
from core.engine import BaseEngine
from core.event import EVENT_TRADE, EVENT_ORDER, EVENT_LOG, EVENT_TIMER
from datastructure.constant import Direction, Status




class RiskEngine(BaseEngine):

    def __init__(self, event_engine: EventEngine) -> None:
        """"""
        super().__init__(event_engine)

        self.active: bool = False

        self.order_flow_count: int = 0
        self.order_flow_limit: int = 50

        self.order_flow_clear: int = 1
        self.order_flow_timer: int = 0

        self.order_size_limit: int = 100

        self.trade_count: int = 0
        self.trade_limit: int = 1000

        self.order_cancel_limit: int = 500
        self.order_cancel_counts: Dict[str, int] = defaultdict(int)

        self.active_order_limit: int = 50


    def send_order(self, req: OrderRequest, gateway_name: str) -> str:
        """"""
        result: bool = self.check_risk(req)
        if not result:
            return ""

        return self._send_order(req, gateway_name)


    def process_trade_event(self, event: Event) -> None:
        """"""
        trade: TradeData = event.data
        self.trade_count += trade.volume

    def process_timer_event(self, event: Event) -> None:
        """"""
        self.order_flow_timer += 1

        if self.order_flow_timer >= self.order_flow_clear:
            self.order_flow_count = 0
            self.order_flow_timer = 0


    def check_risk(self, req: OrderRequest) -> bool:
        """"""
        if not self.active:
            return True

        # Check order volume
        if req.volume <= 0:
            self.write_log("委托数量必须大于0")
            return False

        if req.volume > self.order_size_limit:
            self.write_log(
                f"单笔委托数量{req.volume}，超过限制{self.order_size_limit}")
            return False

        # Check trade volume
        if self.trade_count >= self.trade_limit:
            self.write_log(
                f"今日总成交合约数量{self.trade_count}，超过限制{self.trade_limit}")
            return False

        # Check flow count
        if self.order_flow_count >= self.order_flow_limit:
            self.write_log(
                f"委托流数量{self.order_flow_count}，超过限制每{self.order_flow_clear}秒{self.order_flow_limit}次")
            return False

        # Check all active orders
        active_order_count: int = len(self.main_engine.get_all_active_orders())
        if active_order_count >= self.active_order_limit:
            self.write_log(
                f"当前活动委托次数{active_order_count}，超过限制{self.active_order_limit}")
            return False

        # Check order cancel counts
        order_cancel_count: int = self.order_cancel_counts.get(req.vt_symbol, 0)
        if order_cancel_count >= self.order_cancel_limit:
            self.write_log(f"当日{req.vt_symbol}撤单次数{order_cancel_count}，超过限制{self.order_cancel_limit}")
            return False

        # Add flow count if pass all checks
        self.order_flow_count += 1
        return True

