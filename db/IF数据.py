import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from datetime import datetime

# 1. 定义数据库模型
Base = declarative_base()


class IFData(Base):
    __tablename__ = 'if_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    amount = Column(Integer)
    position = Column(Integer)
    symbol = Column(String(20))


# 2. 创建数据库（SQLite）
db_path = 'db/financial_data.db'
if os.path.exists(db_path):
    os.remove(db_path)

engine = create_engine(f'sqlite:///{db_path}', echo=False)
Base.metadata.create_all(engine)


# 3. 读取CSV文件
def read_if_data(file_path):
    try:
        # 读取CSV文件（逗号分隔，有标题行）
        df = pd.read_csv(
            file_path,
            sep=',',  # 明确指定逗号分隔
            header=0,  # 第一行是标题行
            parse_dates=False,
            encoding='utf-8-sig'
        )

        # 清洗列名（去除前后空格和特殊字符）
        df.columns = df.columns.str.strip().str.replace(r'[\"\']', '', regex=True)
        print(f"检测到的列名: {list(df.columns)}")  # 调试输出

        # 检查列名是否匹配（不区分大小写）
        expected_columns = {'datetime', 'open', 'high', 'low', 'close',
                            'volume', 'amount', 'position', 'symbol'}
        actual_columns = set(col.lower() for col in df.columns)

        if not expected_columns.issubset(actual_columns):
            missing = expected_columns - actual_columns
            raise ValueError(f"缺少必要的列: {missing}。实际列名: {list(df.columns)}")

        # 转换日期时间格式
        df['datetime'] = pd.to_datetime(
            df['datetime'],
            errors='coerce'
        )

        # 检查并报告无效日期
        if df['datetime'].isnull().any():
            bad_rows = df[df['datetime'].isnull()]
            print(f"警告: 发现 {len(bad_rows)} 行日期格式无效，已自动跳过。样例:")
            print(bad_rows.head(2))
            df = df.dropna(subset=['datetime'])

        # 转换数值类型
        numeric_cols = ['open', 'high', 'low', 'close']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        int_cols = ['volume', 'amount', 'position']
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # 确保symbol是字符串
        df['symbol'] = df['symbol'].astype(str).str.strip()

        return df

    except Exception as e:
        print(f"读取CSV文件时出错: {str(e)}")
        raise


# 4. 主程序
if __name__ == "__main__":
    # 使用绝对路径确保文件位置正确
    csv_file = os.path.abspath("data/IF.csv")
    print(f"尝试从以下路径读取文件: {csv_file}")

    # 检查文件是否存在
    if not os.path.exists(csv_file):
        print(f"错误: 文件 {csv_file} 不存在")
        exit(1)

    try:
        # 读取数据
        print(f"正在读取CSV文件: {csv_file}")
        if_df = read_if_data(csv_file)

        # 显示前5行数据
        print("\n数据预览:")
        print(if_df.head())

        # 写入数据库
        print("\n正在写入数据库...")
        if_df.to_sql(
            'if_data',
            con=engine,
            if_exists='append',
            index=False,
            dtype={
                'datetime': DateTime,
                'open': Float,
                'high': Float,
                'low': Float,
                'close': Float,
                'volume': Integer,
                'amount': Integer,
                'position': Integer,
                'symbol': String(20)
            }
        )

        # 验证数据
        print("\n数据验证（查询前5条记录）:")
        Session = sessionmaker(bind=engine)
        session = Session()
        results = session.query(IFData).order_by(IFData.datetime).limit(5).all()

        for i, row in enumerate(results, 1):
            print(f"{i}. {row.datetime} | {row.symbol} | O:{row.open} H:{row.high} L:{row.low} C:{row.close}")

        session.close()
        print(f"\n数据导入完成！数据库已保存到: {os.path.abspath(db_path)}")

    except pd.errors.EmptyDataError:
        print(f"错误: 文件 {csv_file} 为空或格式不正确")
    except Exception as e:
        print(f"发生错误: {str(e)}")