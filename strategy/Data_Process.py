import pandas as pd
import numpy as np
from datetime import time, datetime
from sqlalchemy import create_engine, DateTime, Float, Integer, String, Boolean


def debug_print(df, name):
    """调试用输出函数"""
    print(f"\n{name}数据概览:")
    print(f"时间范围: {df.index.min()} 至 {df.index.max()}")
    print(f"总行数: {len(df)}")
    print(f"列信息:\n{df.dtypes}")
    print(f"前3行数据:\n{df.head(3)}")
    print(f"NA值统计:\n{df.isna().sum()}")


def load_and_clean(engine, table_name='if_data'):
    """从数据库加载数据并进行清洗"""
    try:
        # 从数据库读取数据
        df = pd.read_sql_table(table_name, con=engine)
        
        # 处理时间列并设为索引
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime').sort_index()

        # 基础数据清洗
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

    rs = avg_gain / (avg_loss + 1e-10)
    return 100 - (100 / (1 + rs))


def preprocess_for_rsi_strategy(df):
    """RSI策略专用数据处理"""
    # 计算双周期RSI
    df['rsi_15min'] = calculate_rsi(df['close'].resample('15min').last()).reindex(df.index, method='ffill')
    df['rsi_5min'] = calculate_rsi(df['close'].resample('5min').last()).reindex(df.index, method='ffill')

    # 隔夜价格变化
    df['prev_close'] = df['close'].shift(1)
    df['overnight_change'] = (df['open'] - df['prev_close']) / (df['prev_close'] + 1e-10)

    # 交易时段标记
    df['is_trading_hour'] = df.index.to_series().apply(
        lambda x: time(10, 0) <= x.time() < time(15, 0)
    )

    # 技术指标计算
    df['pct_change'] = df['close'].pct_change()
    df['price_range'] = df['high'] - df['low']
    df['mid_price'] = (df['high'] + df['low']) / 2

    df.drop(columns=['prev_close'], inplace=True)
    return df


def process_data(engine, input_table='if_data', output_table='processed_data'):
    """完整数据处理流程"""
    print("开始数据处理...")
    try:
        # 数据加载与清洗
        cleaned_data = load_and_clean(engine, input_table)
        debug_print(cleaned_data, "清洗后")

        if len(cleaned_data) == 0:
            raise ValueError("清洗后数据为空，请检查数据源")

        # RSI策略处理
        processed_data = preprocess_for_rsi_strategy(cleaned_data)
        debug_print(processed_data, "计算完成后")

        # 准备写入数据库
        output_cols = [
            'symbol', 'open', 'high', 'low', 'close', 'volume',
            'rsi_15min', 'rsi_5min', 'is_trading_hour',
            'overnight_change', 'pct_change', 'price_range', 'mid_price'
        ]

        # 重置索引并过滤时间
        processed_data = processed_data.reset_index()
        processed_data = processed_data[processed_data['datetime'] <= datetime.now()]
        processed_data = processed_data[['datetime'] + output_cols]

        # 写入数据库
        processed_data.to_sql(
            output_table,
            con=engine,
            if_exists='append',
            index=False,
            dtype={
                'datetime': DateTime,
                'symbol': String(20),
                'open': Float,
                'high': Float,
                'low': Float,
                'close': Float,
                'volume': Integer,
                'rsi_15min': Float,
                'rsi_5min': Float,
                'is_trading_hour': Boolean,
                'overnight_change': Float,
                'pct_change': Float,
                'price_range': Float,
                'mid_price': Float
            }
        )

        print(f"\n处理成功！数据已保存到 {output_table}")
        print(f"总处理数据量: {len(processed_data)} 行")
        return processed_data

    except Exception as e:
        print(f"\n处理失败: {str(e)}")
        return None


if __name__ == "__main__":
    db_engine = create_engine('sqlite:///db/financial_data.db')
    
    # 运行处理流程
    result = process_data(
        engine=db_engine,
        input_table='if_data',
        output_table='rsi_strategy_results'
    )

    if result is None:
        print("\n调试建议:")
        print("1. 检查数据库连接参数")
        print("2. 确认输入表存在且包含必要字段")
        print("3. 验证目标表结构是否匹配输出字段")