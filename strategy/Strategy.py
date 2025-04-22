#交易时间为10：00-14：45
import pandas as pd
import numpy as np
from datetime import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from sqlalchemy import create_engine, DateTime, Float, Integer, String, Boolean

plt.style.use('tableau-colorblind10')
sns.set_palette("deep")

class EnhancedRSIStrategyBacktest:
    def __init__(self, data, initial_capital=1e6, commission=2e-4):
        self.data = data
        self.initial_capital = initial_capital
        self.commission_rate = commission
        self.trades = []
        self.current_position = 0
        self.entry_price = None
        self.dates = data.index
        self.open_prices = data['open'].values
        self.close_prices = data['close'].values
        self.is_trading_hour = data['is_trading_hour'].values
        self.equity = np.zeros(len(data))
        self.equity[0] = initial_capital
        self.commissions = np.zeros(len(data))
        self.eod_condition = np.array([(dt.time() >= time(14, 45)) for dt in self.dates], dtype=bool)

    def check_database_connection(self):
        """检查数据库连接有效性"""
        try:
            with self.engine.connect() as conn:
                return conn.execute("SELECT 1").scalar() == 1
        except Exception as e:
            print(f"数据库连接异常: {str(e)}")
            return False
        
    def generate_signals(self):
        df = self.data.copy()
        L, S = 50, 80

        df['long_signal'] = ((df['rsi_15min'].shift(1) > L) &
                            (df['rsi_5min'].shift(1) > S) &
                            self.is_trading_hour)
        df['short_signal'] = ((df['rsi_15min'].shift(1) < (100 - L)) &
                             (df['rsi_5min'].shift(1) < (100 - S)) &
                             self.is_trading_hour)
        df['signal'] = np.where(df['long_signal'], 1, np.where(df['short_signal'], -1, 0))
        self.signals = df['signal'].values
        return df

    def run_backtest(self):
        df = self.generate_signals()
        signals = self.signals
        n = len(signals)
        close_pct_change = np.zeros(n)
        close_pct_change[1:] = (self.close_prices[1:] - self.close_prices[:-1]) / self.close_prices[:-1]

        for i in range(1, n):
            if self.eod_condition[i] and self.current_position != 0:
                self._close_position(i, self.open_prices[i], "EOD Close")

            prev_signal = signals[i-1]

            if self.current_position == 0 and prev_signal != 0:
                self._open_position(i, self.open_prices[i], prev_signal)

            if self.current_position != 0:
                current_return = (self.close_prices[i] - self.entry_price) / self.entry_price * self.current_position
                if abs(current_return) >= 0.02:
                    self._close_position(i, self.open_prices[i], "Stop Loss/Take Profit")

            self._update_equity(close_pct_change, i)

        self.equity_curve = pd.Series(self.equity, index=self.dates)
        df['cum_returns'] = self.equity_curve.pct_change().add(1).cumprod()
        self.results = df
        return df

    def _open_position(self, idx, entry_price, direction):
        self.current_position = direction
        self.entry_price = entry_price
        commission = self.initial_capital * self.commission_rate
        self.commissions[idx] += commission
        self.trades.append({
            'datetime': self.dates[idx],
            'direction': direction,
            'entry_price': entry_price,
            'commission': commission,
            'exit_datetime': None,
            'exit_price': None,
            'returns': -commission
        })

    def _close_position(self, idx, exit_price, reason):
        if self.current_position == 0:
            return

        commission = self.initial_capital * self.commission_rate
        self.commissions[idx] += commission
        trade = self.trades[-1]
        entry_price = trade['entry_price']
        direction = self.current_position

        returns = (exit_price - entry_price) / entry_price * direction
        net_returns = returns - 2 * self.commission_rate
        trade_duration = (self.dates[idx] - trade['datetime']).total_seconds() / 3600

        trade.update({
            'exit_datetime': self.dates[idx],
            'exit_price': exit_price,
            'returns': net_returns,
            'duration': trade_duration,
            'close_reason': reason
        })

        self.current_position = 0
        self.entry_price = None

    def _update_equity(self, close_pct_change, idx):
        if idx == 0:
            return
        position_return = close_pct_change[idx] * self.current_position
        self.equity[idx] = self.equity[idx-1] * (1 + position_return) - self.commissions[idx]

    def plot_results(self):
        """可视化"""
        plt.figure(figsize=(16, 12))

        # 价格与信号图
        ax1 = plt.subplot(3, 1, 1)
        plt.plot(self.results['close'], label='Price', alpha=0.7)

        # 标记交易信号
        long_signals = self.results[self.results['signal'] == 1]
        short_signals = self.results[self.results['signal'] == -1]
        plt.scatter(long_signals.index, long_signals['close'], marker='^',
                   color='g', s=100, label='Long Signal')
        plt.scatter(short_signals.index, short_signals['close'], marker='v',
                   color='r', s=100, label='Short Signal')
        plt.title('Price with Trading Signals')
        plt.legend()

        # 收益曲线图
        ax2 = plt.subplot(3, 1, 2, sharex=ax1)
        plt.plot(self.results['cum_returns'], label='Strategy', color='b')
        plt.plot((self.results['close']/self.results['close'].iloc[0]),
                label='Buy & Hold', alpha=0.5)
        plt.title('Cumulative Returns')
        plt.legend()

        # 回撤曲线图
        ax3 = plt.subplot(3, 1, 3, sharex=ax1)
        max_returns = self.results['cum_returns'].cummax()
        drawdown = (self.results['cum_returns'] - max_returns)/max_returns
        plt.fill_between(drawdown.index, drawdown*100, 0,
                        color='red', alpha=0.3)
        plt.title('Drawdown (%)')

        # 格式调整
        for ax in [ax1, ax2, ax3]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.grid(True, alpha=0.3)
        plt.tight_layout()

    def get_performance_report(self):
        """生成报告"""
        trades_df = pd.DataFrame(self.trades)

        if trades_df.empty:
            return "No trades executed"

        # 基础指标
        total_return = self.results['cum_returns'].iloc[-1] - 1
        years = (self.results.index[-1] - self.results.index[0]).days / 365
        annualized_return = (1 + total_return) ** (1/years) - 1
        max_drawdown = (self.results['cum_returns'] /
                      self.results['cum_returns'].cummax() - 1).min()

        # 交易统计
        trades_df['win'] = trades_df['returns'] > 0
        win_rate = trades_df['win'].mean()
        avg_win = trades_df[trades_df['win']]['returns'].mean()
        avg_loss = trades_df[~trades_df['win']]['returns'].mean()
        profit_factor = (trades_df[trades_df['returns'] > 0]['returns'].sum() /
                        abs(trades_df[trades_df['returns'] < 0]['returns'].sum()))

        # report = f"""
        # ========== 策略绩效报告 ==========
        # 年化收益率: {annualized_return:.2%}
        # 累计收益率: {total_return:.2%}
        # 最大回撤: {max_drawdown:.2%}
        # 胜率: {win_rate:.2%}
        # 平均盈利: {avg_win:.2%}
        # 平均亏损: {avg_loss:.2%}
        # 盈亏比: {profit_factor:.2f}
        # 总交易次数: {len(trades_df)}
        # 多头交易占比: {trades_df[trades_df['direction']==1].shape[0]/len(trades_df):.1%}
        # 平均持仓时间: {trades_df['duration'].mean():.2f}小时
        # """

        report = f"""
        <div style="
            font-family: var(--font);
            max-width: 800px;
            margin: 20px auto;
            padding: 20px;
            background: rgba(var(--block-background-fill), 0.9);
            border-radius: 8px;
            border: 1px solid rgba(var(--border-color-primary), 0.2);
            box-shadow: var(--shadow-drop-lg);
            color: var(--body-text-color);
        ">
            <h2 style="
                color: var(--header-text-color);
                border-bottom: 2px solid rgba(var(--primary-color), 0.8);
                padding-bottom: 8px;
                margin-bottom: 20px;
            ">📊 策略绩效报告</h2>
            
            <!-- 三列布局 -->
            <div style="display: flex; gap: 15px; margin-bottom: 20px;">
                <!-- 收益指标 -->
                <div style="flex: 1; padding: 15px; background: rgba(var(--block-background-fill), 0.5); border-radius: 6px;">
                    <h3 style="color: var(--primary-color); margin: 0 0 10px 0;">收益</h3>
                    <table style="width: 100%; color: var(--body-text-color);">
                        <tr><td>年化收益</td><td style="color: var(--success-color); text-align: right;">{annualized_return:.2%}</td></tr>
                        <tr><td>累计收益</td><td style="color: var(--success-color); text-align: right;">{total_return:.2%}</td></tr>
                    </table>
                </div>

                <!-- 风险指标 -->
                <div style="flex: 1; padding: 15px; background: rgba(var(--block-background-fill), 0.5); border-radius: 6px;">
                    <h3 style="color: var(--error-color); margin: 0 0 10px 0;">风险</h3>
                    <table style="width: 100%; color: var(--body-text-color);">
                        <tr><td>最大回撤</td><td style="color: var(--error-color); text-align: right;">{max_drawdown:.2%}</td></tr>
                        <tr><td>盈亏比</td><td style="text-align: right;">{profit_factor:.2f}</td></tr>
                    </table>
                </div>

                <!-- 交易统计 -->
                <div style="flex: 1; padding: 15px; background: rgba(var(--block-background-fill), 0.5); border-radius: 6px;">
                    <h3 style="color: var(--neutral-color); margin: 0 0 10px 0;">交易</h3>
                    <table style="width: 100%; color: var(--body-text-color);">
                        <tr><td>总次数</td><td style="text-align: right;">{len(trades_df)}</td></tr>
                        <tr><td>胜率</td><td style="text-align: right;">{win_rate:.2%}</td></tr>
                    </table>
                </div>
            </div>

            <!-- 明细统计 -->
            <div style="
                padding: 15px;
                background: rgba(var(--block-background-fill), 0.5);
                border-radius: 6px;
                color: var(--body-text-color);
            ">
                <div style="display: flex; gap: 20px;">
                    <div style="flex: 1;">
                        <p style="color: var(--success-color); margin: 5px 0;">
                            ▲ 平均盈利 <span style="float: right;">+{avg_win:.2%}</span>
                        </p>
                        <p style="color: var(--error-color); margin: 5px 0;">
                            ▼ 平均亏损 <span style="float: right;">-{avg_loss:.2%}</span>
                        </p>
                    </div>
                    <div style="flex: 1;">
                        <p style="margin: 5px 0;">
                            多头占比 <span style="float: right;">{trades_df[trades_df['direction']==1].shape[0]/len(trades_df):.1%}</span>
                        </p>
                        <p style="margin: 5px 0;">
                            持仓时长 <span style="float: right;">{trades_df['duration'].mean():.1f}h</span>
                        </p>
                    </div>
                </div>
            </div>
        </div>
        """
        return report


if __name__ == "__main__":
    # 创建数据库连接
    db_engine = create_engine('sqlite:///db/financial_data.db')
    
    try:
        # 从数据库读取预处理好的数据
        strategy_data = pd.read_sql_table(
            'rsi_strategy_results',
            con=db_engine,
            index_col='datetime',  # 确保使用日期时间作为索引
            parse_dates=['datetime']
        )
        
        # 验证数据完整性
        required_columns = ['rsi_15min', 'rsi_5min', 'is_trading_hour', 'close']
        if not set(required_columns).issubset(strategy_data.columns):
            missing = set(required_columns) - set(strategy_data.columns)
            raise ValueError(f"缺少必要字段: {missing}，请检查数据处理流程")

        # 运行回测
        backtester = EnhancedRSIStrategyBacktest(strategy_data)
        results = backtester.run_backtest()
        
        # 输出结果
        print(backtester.get_performance_report())
        backtester.plot_results()
        plt.show()

    except Exception as e:
        print(f"策略执行失败: {str(e)}")
        print("故障排除建议：")
        print("1. 确认已运行Data_Process.py完成数据预处理")
        print("2. 检查数据库中存在rsi_strategy_results表")
        print("3. 验证预处理数据包含所有必需字段")