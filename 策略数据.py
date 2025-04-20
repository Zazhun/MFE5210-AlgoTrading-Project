import pandas as pd
import numpy as np
from datetime import time, datetime


def debug_print(df, name):
    """调试用输出函数"""
    print(f"\n{name}数据概览:")
    print(f"时间范围: {df.index.min()} 至 {df.index.max()}")
    print(f"总行数: {len(df)}")
    print(f"列信息:\n{df.dtypes}")
    print(f"前3行数据:\n{df.head(3)}")
    print(f"NA值统计:\n{df.isna().sum()}")


def load_and_clean(filepath):
    """数据加载与清洗函数"""
    try:
        # 读取文件并解析时间列
        df = pd.read_csv(filepath)
        df['datetime'] = pd.to_datetime(df['datetime'], format='mixed')
        df = df.set_index('datetime').sort_index()

        # 基础清洗
        df = df.assign(
            volume=df['volume'].replace(0, np.nan).ffill().fillna(0),
            open=df['open'].ffill(),
            high=df['high'].ffill(),
            low=df['low'].ffill(),
            close=df['close'].ffill()
        )
        return df

    except Exception as e:
        print(f"数据加载错误: {str(e)}")
        raise


def calculate_rsi(series, window=14):
    """RSI计算函数"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window, min_periods=window).mean()
    avg_loss = loss.rolling(window, min_periods=window).mean()

    rs = avg_gain / (avg_loss + 1e-10)  # 避免除零
    return 100 - (100 / (1 + rs))


def preprocess_for_rsi_strategy(df):
    """RSI策略专用数据处理"""
    # 计算双周期RSI（带预热期检查）
    df['rsi_15min'] = calculate_rsi(df['close'].resample('15min').last()).reindex(df.index, method='ffill')
    df['rsi_5min'] = calculate_rsi(df['close'].resample('5min').last()).reindex(df.index, method='ffill')

    # 改进的隔夜变化计算（使用前一根K线收盘价）
    df['prev_close'] = df['close'].shift(1)
    df['overnight_change'] = (df['open'] - df['prev_close']) / (df['prev_close'] + 1e-10)

    # 精确的交易时段标记（10:00-15:00）
    df['is_trading_hour'] = df.index.to_series().apply(
        lambda x: time(10, 0) <= x.time() < time(15, 0)
    )

    # 基础技术指标
    df['pct_change'] = df['close'].pct_change()
    df['price_range'] = df['high'] - df['low']
    df['mid_price'] = (df['high'] + df['low']) / 2

    # 清理临时列
    df.drop(columns=['prev_close'], inplace=True)

    return df


def process_data(filepath):
    """完整的数据处理流程"""
    print("开始数据处理...")

    try:
        # 1. 数据加载与清洗
        print("\n1. 数据加载与清洗...")
        cleaned_data = load_and_clean(filepath)
        debug_print(cleaned_data, "清洗后")

        if len(cleaned_data) == 0:
            raise ValueError("清洗后数据为空，请检查输入文件")

        # 2. 计算RSI策略所需字段
        print("\n2. 计算RSI策略数据字段...")
        processed_data = preprocess_for_rsi_strategy(cleaned_data)
        debug_print(processed_data, "计算完成后")

        # 3. 保存结果
        output_file = "IF_RSI_Strategy_Data_READY.csv"
        output_cols = [
            'open', 'high', 'low', 'close', 'volume',
            'rsi_15min', 'rsi_5min', 'is_trading_hour',
            'overnight_change', 'pct_change', 'price_range', 'mid_price'
        ]

        processed_data = processed_data[processed_data.index <= datetime.now()]
        processed_data[output_cols].to_csv(output_file)

        print(f"\n处理成功！结果已保存到 {output_file}")
        print(f"总处理数据量: {len(processed_data)} 行")
        print("\n关键指标统计：")
        print(f"15分钟RSI均值: {processed_data['rsi_15min'].mean():.2f}")
        print(f"5分钟RSI均值: {processed_data['rsi_5min'].mean():.2f}")
        print("\n交易时段数据样例（10:00-15:00）：")
        print(processed_data.between_time('10:00', '15:00')[output_cols].head(10))

        return processed_data

    except Exception as e:
        print(f"\n处理失败: {str(e)}")
        return None


# 主程序
if __name__ == "__main__":
    input_file = "IF.csv"
    result = process_data(input_file)

    if result is None:
        print("\n调试建议:")
        print("1. 检查文件路径是否正确")
        print("2. 确认文件包含datetime,open,high,low,close,volume等列")
        print("3. 尝试手动检查数据：")
        print('''import pandas as pd; df = pd.read_csv("IF.csv"); print(df[['datetime', 'close']].head())''')