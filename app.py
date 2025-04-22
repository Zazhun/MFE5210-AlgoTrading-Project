# webui-app.py
import gradio as gr
import pandas as pd
import matplotlib
from datetime import datetime
from sqlalchemy import create_engine
from strategy.Strategy import EnhancedRSIStrategyBacktest
import os

# 配置matplotlib非交互模式
matplotlib.use('Agg')

DB_PATH = f'sqlite:///db/financial_data.db'

print(f"数据库路径：{DB_PATH}") 

def create_backtest_interface():
    with gr.Blocks(title="量化回测系统", theme=gr.themes.Soft()) as app:
        # 初始化数据库连接
        engine = create_engine(DB_PATH)
        
        # 头部说明
        gr.Markdown("""
        #  策略回测平台
        """)

        # 输入区域
        with gr.Row(variant="panel"):
            with gr.Column(scale=2):
                contract = gr.Dropdown(
                    label="合约代码",
                    choices=["IF"],
                    value="IF"
                )
                with gr.Row():  # 新增日期输入行
                    start_date = gr.Textbox(
                        label="开始日期",
                        placeholder="YYYY-MM-DD",
                        value="2024-01-02"
                    )
                    end_date = gr.Textbox(
                        label="结束日期", 
                        placeholder="YYYY-MM-DD",
                        value="2024-04-01"
                    )

                param_input = gr.Textbox(
                    label="策略参数",
                    placeholder="L=50, S=80"
                )
                
            with gr.Column(scale=1):
                capital = gr.Number(
                    label="初始资金(万)",
                    value=100,
                    minimum=10,
                    maximum=1000
                )
                commission = gr.Slider(
                    label="手续费率(%)",
                    minimum=0.01,
                    maximum=1.0,
                    value=0.02,
                    step=0.01
                )

        # 控制按钮
        with gr.Row():
            run_btn = gr.Button("开始回测", variant="primary")
            gr.ClearButton(components=[param_input, capital])

        # 结果显示区
        with gr.Tabs():
            with gr.TabItem("📈 绩效概览"):
                plot_output = gr.Plot(label="回测结果")
                
            with gr.TabItem("📊 指标统计"):
                report_output = gr.HTML()
                
            with gr.TabItem("💹 数据摘要"):
                data_stats = gr.DataFrame(
                    label="行情数据统计",
                    headers=["统计指标", "值"],
                    datatype=["str", "number"],
                    row_count=10,  
                    col_count=(2, "fixed")
                )

        # 回测执行函数
        def execute_backtest(contract, start_date, end_date, params, capital, commission):
            try:
                # 从数据库获取数据
                query = f"""
                    SELECT * FROM rsi_strategy_results 
                    WHERE datetime BETWEEN '{start_date}' AND '{end_date}'
                """
                df = pd.read_sql_query(query, engine, 
                                     index_col='datetime', 
                                     parse_dates=['datetime'])
                
                # 初始化策略
                strategy = EnhancedRSIStrategyBacktest(
                    df, 
                    initial_capital=capital*1e4,
                    commission=commission/100
                )
                results = strategy.run_backtest()
                
                # 生成图表
                strategy.plot_results()
                figure = matplotlib.pyplot.gcf()
                
                # 生成报告
                report = strategy.get_performance_report()
                
                # 数据统计
                stats = df[['close']].describe()\
                    .reset_index()\
                    .rename(columns={'index':'统计指标'})
                
                return {
                    plot_output: figure,
                    report_output: report,
                    data_stats: stats
                }
            except Exception as e:
                raise gr.Error(f"回测失败: {str(e)}")

        # 绑定事件
        run_btn.click(
            fn=execute_backtest,
            inputs=[contract, start_date, end_date, param_input, capital, commission],
            outputs=[plot_output, report_output, data_stats]
        )

    return app

if __name__ == "__main__":
    web_app = create_backtest_interface()
    web_app.launch(debug = True)