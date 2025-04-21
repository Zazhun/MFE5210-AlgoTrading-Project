import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, Time, Float
from sqlalchemy.orm import declarative_base, sessionmaker
import os

# 1. 定义数据库模型
Base = declarative_base()


class MarketData(Base):
    __tablename__ = 'market_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    action_day = Column(Date)
    trading_day = Column(Date)
    update_time = Column(Time)
    instrument_id = Column(String(20))
    last_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    open_price = Column(Float)
    volume = Column(Integer)
    turnover = Column(Float)
    open_interest = Column(Integer)
    upper_limit = Column(Float)
    lower_limit = Column(Float)
    bid_price1 = Column(Float)
    bid_volume1 = Column(Integer)
    ask_price1 = Column(Float)
    ask_volume1 = Column(Integer)


# 2. 创建数据库（SQLite）
db_path = 'market_data.db'
if os.path.exists(db_path):
    os.remove(db_path)

engine = create_engine(f'sqlite:///{db_path}', echo=False)
Base.metadata.create_all(engine)


# 3. 读取CSV文件
def read_market_data(file_path):
    try:
        # 读取CSV文件（自动检测分隔符）
        df = pd.read_csv(file_path, sep=None, engine='python')

        # 检查必要的列是否存在
        required_columns = ['ActionDay', 'TradingDay', 'UpdateTime', 'InstrumentID']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"CSV文件中缺少必需的列: {col}")

        # 重命名列（保持原始CSV列名大小写）
        column_mapping = {
            'ActionDay': 'action_day',
            'TradingDay': 'trading_day',
            'UpdateTime': 'update_time',
            'InstrumentID': 'instrument_id',
            'LastPrice': 'last_price',
            'HighPrice': 'high_price',
            'LowPrice': 'low_price',
            'OpenPrice': 'open_price',
            'Volume': 'volume',
            'Turnover': 'turnover',
            'OpenInterest': 'open_interest',
            'UpperLimitPrice': 'upper_limit',
            'LowerLimitPrice': 'lower_limit',
            'BidPrice1': 'bid_price1',
            'BidVolume1': 'bid_volume1',
            'AskPrice1': 'ask_price1',
            'AskVolume1': 'ask_volume1'
        }

        # 只保留我们需要的列
        available_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df[list(available_columns.keys())].rename(columns=available_columns)

        # 转换日期格式（确保列存在）
        if 'action_day' in df.columns:
            df['action_day'] = pd.to_datetime(df['action_day'].astype(str), format='%Y%m%d').dt.date
        if 'trading_day' in df.columns:
            df['trading_day'] = pd.to_datetime(df['trading_day'].astype(str), format='%Y%m%d').dt.date

        # 转换时间格式（确保列存在）
        if 'update_time' in df.columns:
            # 处理时间格式（如"0:00:00"或"7:03:39"）
            df['update_time'] = pd.to_datetime(
                df['update_time'].astype(str).str.replace('^(\d):', '0\\1:', regex=True),
                format='%H:%M:%S'
            ).dt.time

        return df

    except Exception as e:
        print(f"读取CSV文件时出错: {str(e)}")
        raise


# 4. 主程序
if __name__ == "__main__":
    csv_file = "IF2503.csv"  # 确保文件路径正确

    try:
        # 读取数据
        print(f"正在读取CSV文件: {csv_file}")
        market_df = read_market_data(csv_file)

        # 显示前5行数据
        print("\n数据预览:")
        print(market_df.head())

        # 写入数据库
        print("\n正在写入数据库...")
        market_df.to_sql(
            'market_data',
            con=engine,
            if_exists='append',
            index=False,
            dtype={
                'action_day': Date,
                'trading_day': Date,
                'update_time': Time,
                'instrument_id': String(20),
                'last_price': Float,
                'high_price': Float,
                'low_price': Float,
                'open_price': Float,
                'volume': Integer,
                'turnover': Float,
                'open_interest': Integer,
                'upper_limit': Float,
                'lower_limit': Float,
                'bid_price1': Float,
                'bid_volume1': Integer,
                'ask_price1': Float,
                'ask_volume1': Integer
            }
        )

        # 验证数据
        print("\n数据验证（查询前5条记录）:")
        Session = sessionmaker(bind=engine)
        session = Session()
        results = session.query(MarketData).limit(5).all()

        for i, row in enumerate(results, 1):
            print(f"{i}. {row.instrument_id} | {row.action_day} {row.update_time} | 最新价: {row.last_price}")

        session.close()
        print(f"\n数据导入完成！数据库已保存到: {os.path.abspath(db_path)}")

    except FileNotFoundError:
        print(f"错误: 文件 {csv_file} 未找到，请检查文件路径")
    except pd.errors.EmptyDataError:
        print(f"错误: 文件 {csv_file} 为空或格式不正确")
    except Exception as e:
        print(f"发生错误: {str(e)}")