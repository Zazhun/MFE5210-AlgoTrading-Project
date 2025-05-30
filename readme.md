# MFE5210-AlgoTrading-Project

## 项目简介

本项目为量化交易策略回测与分析平台，支持基于A股股指期货（如沪深300期货合约）的数据处理、策略开发、回测与可视化。

## 项目结构

```
MFE5210-AlgoTrading-Project/
│
├─ app.py                  # Web 回测界面主程序
│
├─ db/
│   ├─ IF数据.py           # 合约数据入库脚本
│   ├─ 建库入库.py         # 市场数据建库与入库脚本
│   ├─ financial_data.db   # 主数据库
│   └─ ...                 # 其它数据库相关文件
│
├─ exchange/
│   └─ Exchange            # 回测撮合引擎
│
├─ strategy/
│   ├─ Data_Process.py     # 数据清洗和处理
│   ├─ Strategy.py         # 策略实现
│   └─ __init__.py
│
├─ data/                   # 原始行情数据（如IF.csv）
└─ ...
```

## 主要模块说明

- **db/**  
  数据库相关脚本，包括原始数据的清洗、入库和表结构定义。支持从CSV批量导入合约行情数据，并存储为SQL数据库，便于后续分析和回测。

- **exchange/Exchange**  
  回测撮合引擎，模拟真实交易所的订单撮合、成交生成、日结算等功能。支持订单管理、成交记录、日度统计等，便于策略回测的真实还原。

- **strategy/Data_Process.py**  
  数据预处理模块，包括行情数据清洗、特征工程（如RSI、价格区间、隔夜变动等），为策略提供高质量输入。

- **strategy/Strategy.py**  
  策略实现模块。以增强型RSI策略为例，支持多周期RSI信号、交易时段过滤、资金管理等。可扩展为多因子或其它量化策略。

- **app.py**  
  Web 回测界面，基于 Gradio 实现。支持参数输入、回测执行、绩效图表、指标统计和数据摘要等功能，界面友好，适合交互式策略研究。

## 量化策略简介

### 增强型RSI策略（EnhancedRSIStrategy）

- **核心思想**：  
  利用多周期RSI指标（如5分钟、15分钟）判断市场超买超卖状态，结合隔夜跳空、交易时段等辅助因子，进行多空交易决策。参考资料见reference文件夹下研报。

- **主要特征**：
  - 双周期RSI信号过滤，提升信号可靠性
  - 交易时段过滤，规避非活跃时段噪声
  - 支持手续费、资金管理等真实回测参数

- **回测流程**：
  1. 数据预处理（清洗、特征工程）
  2. 生成交易信号
  3. 撮合成交、资金曲线更新
  4. 输出绩效图表与统计报告

## 快速开始
1. **安装依赖**  
   终端运行`pip install -r requirements.txt`安装依赖。

2. **准备数据**  
   将原始IF合约行情CSV放入 `data/` 目录，运行 `db/IF数据.py`完成数据库初始化。

3. **数据处理**  
   运行 `strategy/Data_Process.py`，生成带有技术指标的策略输入表存入数据库。

4. **启动回测界面**  
   运行`python app.py`，浏览器访问终端提示的WebUI界面地址。

5. **参数设置与回测**  
   在Web界面选择合约、日期、策略参数、初始资金、手续费等，点击“开始回测”即可查看绩效图表和统计结果。

## 依赖环境

- Python 3.8+
- pandas, numpy, sqlalchemy, gradio, matplotlib 等

## 备注

- 策略和数据处理可根据实际需求扩展
- 支持多策略、多品种扩展
- 如需自定义数据库路径或表结构，请同步修改相关脚本

---

如有问题欢迎交流与反馈！