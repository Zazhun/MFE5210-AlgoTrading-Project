#!/usr/bin/env python
# coding: utf-8

# In[6]:


from .template import StrategyTemplate
from core.event import EventEngine, Event
from datastructure.constant import Direction
from datastructure.object import TickData, BarData, OrderData, TradeData, SignalData
from typing import Any, Dict, List, Optional


from datetime import datetime


class DoubleMaStrategy(StrategyTemplate):
    """"""""
    # 作者
    author = "TangaoChen"
    # 定义参数
    fast_window = 10
    slow_window = 20
    # 定义变量
    fast_ma0 = 0.0
    fast_ma1 = 0.0
    slow_ma0 = 0.0
    slow_ma1 = 0.0
 
    parameters = ["fast_window", "slow_window"]
    variables = ["fast_ma0", "fast_ma1", "slow_ma0", "slow_ma1"]
 
    def __init__(self, event_engine: EventEngine) -> None:
        super().__init__(event_engine)
        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager(100)
 
    # 重写父类函数
    def on_init(self):
        self.write_log("demo策略初始化###")
        self.load_bar(10)
 
    def on_start(self):
        self.write_log("demo策略启动##")
 
    def on_tick(self, tick: TickData):
        # tick更新
        self.bg.update_tick(tick)
 
    def on_stop(self):
        self.write_log("demo策略停止")
 
    def on_bar(self, bar: BarData):
        # K线更新
        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return
        fast_ma = am.sma(self.fast_window, array=True)
        fast_ma0 = fast_ma[-1]
        fast_ma1 = fast_ma[-2]
 
        slow_ma = am.sma(self.slow_window, array=True)
        slow_ma0 = slow_ma[-1]
        slow_ma1 = slow_ma[-2]
 
        # 判断均线交叉
        cross_over = fast_ma0 >= slow_ma0 and fast_ma1 < slow_ma1
 
        cross_below = fast_ma0 <= slow_ma0 and fast_ma1 > slow_ma1
 
        ##如果是金叉，则执行买入操作
        if cross_over:
            # 定义交易价格（滑点）,当前价格+5
            price = bar.close_price + 5
            if not self.pos:
                self.buy(price, 1)
            elif self.pos < 0:
                self.cover(price, 1)
                self.buy(price, 1)
 
        ##如果是死叉，则执行卖出/开空
        elif cross_below:
            price = bar.close_price - 5
            if not self.pos:
                self.short(price, 1)
            elif self.pos > 0:
                self.sell(price, 1)
                self.short(price, 1)

