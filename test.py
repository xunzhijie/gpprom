#import akshare as ak

#stock_sse_summary = ak.fund_etf_hist_em(symbol='510500',period='daily',start_date='20230301',end_date='20230324',adjust='qfq')
#print(stock_sse_summary)
#print(type(stock_sse_summary))

from ETF.DataUtil import DataUtil
import datetime
#import talib

df=DataUtil().getEtfHis('510500',30);
df.set_index('日期',inplace=True)


etf_close = df[['收盘']]
price=df
etf_close['ma5'] = etf_close['收盘'].rolling(5).mean()
#etf_close_MA5 = talib.SMA(etf_close.收盘, timeperiod=5)
#etf_close_MA30 = talib.SMA(etf_close.收盘, timeperiod=30)

#etf_close = etf_close.assign(ETF_MA5 = etf_close_MA5.values)
#etf_close = etf_close.assign(ETF_MA30 = etf_close_MA30.values)

etf_close['pct_change'] = etf_close.收盘.pct_change()

import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.use('TkAgg')
#plt.use('TkAgg')
plt.show()
etf_close.plot()

print(etf_close)
