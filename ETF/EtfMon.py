import datetime
import math

from DBUtil import DBUtil
from ETF.DataUtil import DataUtil
from FunUtil import FunUtil
from pydantic import BaseModel
import pandas as pd

from Util import Util


class Dict(BaseModel):
    code: str
    name:str
    type: int;

class EtfMon:
    def getDownOrUp(self,codes,end_day,days):
        result = []
        for code in codes:
            code_rt={}
            df_etf=DataUtil().getEtfHis(code,end_day,days)
            df_etf.set_index('日期', inplace=True)
            df_etf['ma5'] = df_etf['收盘'].rolling(5).mean()
            df_etf_desc = df_etf.sort_values(by=['日期'], ascending=False)
            #print(df_etf_desc)
            min = 0
            minDay = ''
            firstDay = ''
            stop_flag = True
            flag=''
            for row in df_etf_desc.itertuples(index=True, name='Pandas'):
                Ma5=getattr(row, "ma5")
                if(min!=0 and Ma5 >= min and flag==''):
                    stop_flag = False
                    flag='Down'
                elif(min!=0 and Ma5 < min and flag==''):
                    stop_flag=False
                    flag='Up'
                elif(min != 0 and Ma5 >= min and flag == 'Down'):
                    stop_flag = False
                elif(min != 0 and Ma5 < min and flag == 'Up'):
                    stop_flag = False
                else:
                    stop_flag=True
                if(stop_flag and min!=0):break
                if(stop_flag and min==0):firstDay=getattr(row, "Index")

                min = Ma5
                minDay = getattr(row, "Index")

            #print(firstDay,minDay)
            #print(df_etf_desc.loc[firstDay]['收盘'])
            code_rt['code']=code
            code_rt['start_day'] = minDay
            code_rt['end_day'] = firstDay
            code_rt['start_close'] = '%.3f' % float(df_etf_desc.loc[minDay]['收盘'])
            code_rt['end_close'] = '%.3f' % float(df_etf_desc.loc[firstDay]['收盘'])
            code_rt['up_or_down'] = flag
            code_rt['ma5'] = '%.3f' % float(df_etf_desc.loc[firstDay]['ma5'])
            code_rt['close_rate'] = '%.3f' % float(df_etf_desc.loc[firstDay]['涨跌幅'])
            result.append(code_rt)
            #print(result)
        return  result


    def getEtfCodes(self):
        etfList = FunUtil().getMonitorList(0)
        codes = []
        for i in range(len(etfList)):
            codes.append(etfList[i]['code'])
        return codes

    def getEtfList(self):
        codes=EtfMon().getEtfCodes()
        etfDt=DataUtil().getEtfNow(codes)
        df_a = pd.DataFrame(etfDt)
        df_b = df_a.stack()
        df = df_b.unstack(0)
        #print(df)
        result = []
        for row in df.itertuples(index=True, name='Pandas'):
            etf={}
            etf['code']=getattr(row, "Index")
            etf['name'] = getattr(row, "name")
            etf['now'] = getattr(row, "now")
            etf['code'] = getattr(row, "Index")
            etf['rate'] = '%.4f' % float((getattr(row, "now") - getattr(row, "close")) / getattr(row, "close")*100) if float(getattr(row, "close")) != 0 else 0
            result.append(etf)
            #print(row)
        # avg = df['volume'][0] / df['turnover'][0] if int(df['turnover'][0]) != 0 else 0
        # now = df['now'][0]
        # rate = '%.4f' % float((now - avg) / now) if float(now) != 0 else 0
        # zdf = '%.4f' % float((now - float(df['close'][0])) / float(df['close'][0])) if float(df['close'][0]) != 0 else 0
        # avg = '%.3f' % avg
        # return {'code': id, 'now': now, 'close': df['close'][0], 'avg': avg, 'rate': rate, 'zdf': zdf,
        #         'name': str(df['name'][0])}

        return result


    def insUpOrDown(self,now,days):
        if(Util().isHoliday(now)):return
        codes = EtfMon().getEtfCodes()
        #now=datetime.datetime.now().strftime("%Y%m%d")
        rs=EtfMon().getDownOrUp(codes,now,days)
        print(rs)
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        del_sql="delete from monitor_etf_gp where code=%s and event_day=%s"
        ins_sql = "insert into monitor_etf_gp(code,event_day,start_day,end_day,start_close,end_close,up_or_down,ma5,uddays,angle," \
                  "ud_rate,ma5_rate,close_rate) " \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for item in rs:
            del_val=(item['code'],item['end_day'])
            mysql_conn.ExecNonQuery(del_sql, del_val)
            end_close=float(item['end_close'])
            start_close=float(item['start_close'])
            ma5=float(item['ma5'])
            close_rate = float(item['close_rate'])

            angle_norm = math.atan((end_close / start_close - 1)*100)*180 / 3.1415926
            ud_rate='%.2f' %float((end_close-start_close)/start_close*100)
            ma5_rate='%.2f' %float((end_close-ma5)/ma5*100)

            val=(item['code'],item['end_day'],item['start_day'],item['end_day'],start_close,end_close,item['up_or_down'],
                 ma5,Util().workdays(item['start_day'],item['end_day'])-1,angle_norm,ud_rate,ma5_rate,close_rate)
            mysql_conn.ExecNonQuery(ins_sql, val)




if __name__ == "__main__":
    codes=['516950','510500']
    now = datetime.datetime.now().strftime("%Y%m%d")
    d=20230331
   # d = datetime.strptime(d, '%Y-%m-%d').date()
    i=0
    while i<20:
        EtfMon().insUpOrDown(str(d), 40)
        d=d-1
        i=i+1

    #print(rt)
