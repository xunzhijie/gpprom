from datetime import datetime

import akshare as ak
import backtrader as bt
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
import pandas as pd

plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置画图时的中文显示
plt.rcParams["axes.unicode_minus"] = False  # 设置画图时的负号显示


class MyStrategy(bt.Strategy):
    """
    主策略程序
    """
    params = (("maperiod", 18),
              ('printlog', True),
              ('p_downdays', 4),
              ('p_updays', 5),
              ('befdays', 30),
              # ('down',0)
              )  # 全局设定交易策略的参数, maperiod是 MA 均值的长度

    def __init__(self):
        """
        初始化函数
        """
        self.data_close = self.datas[0].close  # 指定价格序列

        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        # 添加移动均线指标
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod
        )
        self.sma_sell = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.p_updays
        )


    def next(self):
        """
        主逻辑
        """
        # self.log(f'收盘价, {self.data_close[0]}')  # 记录收盘价
        if self.order:  # 检查是否有指令等待执行,
            return
        # 检查是否持仓
        if not self.position:  # 没有持仓
            # 获取近几日用于判断是否连续下跌
            lastcloses = list()
            for i in range(self.p.p_downdays):
                lastcloses.append(self.sma[-i - 1])

            days = 0
            upflag = self.sma[- 1] < self.sma[0]
            downflag = self.sma[- 1] > self.sma[0]
            befclose=1
            for i in range(self.p.befdays):
                if (downflag and self.sma[-i - 1] > self.sma[-i]):
                    days = i + 1
                elif (upflag and self.sma[-i - 1] < self.sma[-i]):
                    days = i + 1
                else:
                    befclose=self.data_close[-i - 1]
                    break
            #rate = (lastcloses[0] - lastcloses[len(lastcloses) - 1]) / lastcloses[len(lastcloses) - 1]
            rate=(self.data_close[0]-befclose)/befclose
            if upflag: self.log(f"ycy-up:days:{days},data_close:{self.data_close[0]},sma:{round(self.sma[0],3)},rate:{round(rate, 3)},CC:{self.position.size}")
            if downflag: self.log(f"ycy-down:days:{days},data_close:{self.data_close[0]},sma:{round(self.sma[0],3)},rate:{round(rate, 3)},CC:{self.position.size}")
            # 连续N日下跌
            # 执行买入条件判断：收盘价格上涨突破15日均线
            #if lastcloses == sorted(lastcloses) and rate < -0.03 and self.data_close[0] > self.sma[0]:
            if self.data_close[0] > self.sma[0]:
                self.log("BUY CREATE, %.2f" % self.data_close[0])
                # 执行买入
                self.order = self.buy()

        else:
            lastcloses_buy = list()
            for i in range(self.p.p_downdays):
                lastcloses_buy.append(self.sma[-i - 1])
            lastcloses_sell = list()
            for i in range(self.p.p_downdays):
                lastcloses_sell.append(self.sma_sell[-i - 1])
            days = 0
            upflag = self.sma_sell[- 1] < self.sma_sell[0]
            downflag = self.sma[- 1] > self.sma[0]
            befclose = 1
            for i in range(self.p.befdays):
                if (downflag and self.sma[-i - 1] > self.sma[-i]):
                    days = i + 1
                elif (upflag and self.sma_sell[-i - 1] < self.sma_sell[-i]):
                    days = i + 1
                else:
                    befclose = self.data_close[-i - 1]
                    break
            #rate = (lastcloses[0] - lastcloses[len(lastcloses) - 1]) / lastcloses[len(lastcloses) - 1]
            rate = (self.data_close[0] - befclose) / befclose
            if upflag: self.log(f"ycy-up:days:{days},data_close:{self.data_close[0]},sma:{round(self.sma[0],3)},rate:{round(rate,3)},CC:{self.position.size}")
            if downflag: self.log(f"ycy-down:days:{days},data_close:{self.data_close[0]},sma:{round(self.sma[0],3)},rate:{round(rate,3)},CC:{self.position.size}")
            # 连续N日下跌
            # 执行买入条件判断：收盘价格上涨突破15日均线
            # if lastcloses == sorted(lastcloses) and rate < -0.03 and self.data_close[0] > self.sma[0]:
            if self.data_close[0] > self.sma[0]:
                self.log("BUY CREATE, %.2f" % self.data_close[0])
                # 执行买入
                self.order = self.buy()
            # 执行卖出条件判断：收盘价格跌破15日均线
            elif lastcloses_sell == sorted(lastcloses_sell, reverse=True) and rate > 0.03 and self.data_close[0] < self.sma_sell[0]:
                self.log("SELL CREATE, %.2f" % self.data_close[0])
                # 执行卖出
                self.order = self.sell()


    def log(self, txt, dt=None, do_print=False):
        """
        Logging function fot this strategy
        """
        if self.params.printlog or do_print:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        """
        记录交易执行情况
        """
        # 如果 order 为 submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"买入:\n价格:{order.executed.price},\
                成本:{order.executed.value},\
                手续费:{order.executed.comm}"
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(
                    f"卖出:\n价格：{order.executed.price},\
                成本: {order.executed.value},\
                手续费{order.executed.comm}"
                )
            self.bar_executed = len(self)

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")
        self.order = None

    def notify_trade(self, trade):
        """
        记录交易收益情况
        """
        if not trade.isclosed:
            return
        self.log(f"策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}")

    def stop(self):
        """
        回测结束后输出结果
        """
        self.log("(MA均线： %2d日) 期末总资金 %.2f" % (self.params.maperiod, self.broker.getvalue()), do_print=True)


def main(code="600070", start_cash=1000000, stake=100, commission_fee=0.001):
    cerebro = bt.Cerebro()  # 创建主控制器
    cerebro.addstrategy(MyStrategy)  # 导入策略参数寻优，画图用这个
    #cerebro.optstrategy(MyStrategy, maperiod=range(3, 31))  # 导入策略参数寻优 ，此项不能画图
    # 利用 AKShare 获取股票的后复权数据，这里只获取前 6 列
    stock_hfq_df = ak.fund_etf_hist_em(symbol=code, adjust="qfq", start_date='20100101', end_date='20230617').iloc[:, :6]
    # 处理字段命名，以符合 Backtrader 的要求
    stock_hfq_df.columns = [
        'date',
        'open',
        'close',
        'high',
        'low',
        'volume',
    ]
    # 把 date 作为日期索引，以符合 Backtrader 的要求
    stock_hfq_df.index = pd.to_datetime(stock_hfq_df['date'])
    start_date = datetime(2021, 1, 1)  # 回测开始时间
    end_date = datetime(2023, 6, 1)  # 回测结束时间
    data = bt.feeds.PandasData(dataname=stock_hfq_df, fromdate=start_date, todate=end_date)  # 规范化数据格式
    cerebro.adddata(data)  # 将数据加载至回测系统
    cerebro.broker.setcash(start_cash)  # broker设置资金
    cerebro.broker.setcommission(commission=commission_fee)  # broker手续费
    cerebro.addsizer(bt.sizers.FixedSize, stake=stake)  # 设置买入数量
    print("期初总资金: %.2f" % cerebro.broker.getvalue())
    cerebro.run(maxcpus=1)  # 用单核 CPU 做优化
    print("期末总资金: %.2f" % cerebro.broker.getvalue())
    cerebro.plot()


if __name__ == '__main__':
    main(code="159995", start_cash=100000, stake=2000, commission_fee=0.001)