import pandas as pd

from DBUtil import DBUtil
from datetime import datetime,date,time
import decimal

class FunUtil:
    def getallinfo(self,type):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        dellist_sql="select a.code code,a.name name,a.vol vol,a.price price,b.bos bos,b.vol l_vol,b.price l_price " \
                    "from (select * from (select code,name,sum(case when bos='B' then vol else -vol end) vol," \
                    "round(sum(case when bos='B' then price*vol else -(price*vol) end)/sum(case when bos='B' then vol else -vol end),3) price " \
                    "from deallist GROUP BY code,name) a where a.vol>0)a," \
                    "(select * from deallist where id in (" \
                    "select max(id)from deallist group by code)) b,etf_gp_dict c where a.code =b.code and a.code=c.code and c.type=%s"
        val=(type)
        dellist_rt = mysql_conn.ExecQueryVal(dellist_sql,val)

        all_item = {"data": {"list": []}}
        for i in range(len(dellist_rt)):
            item_del = {}
            item_del['code']=dellist_rt[i][0]
            item_del['name'] = dellist_rt[i][1]
            item_del['vol'] = int(decimal.Decimal(dellist_rt[i][2]).quantize(decimal.Decimal('0')))
            item_del['price'] = dellist_rt[i][3]
            item_del['bos'] = dellist_rt[i][4]
            item_del['l_vol'] = dellist_rt[i][5]
            item_del['l_price'] = dellist_rt[i][6]
            all_item.get('data').get('list').append(item_del)
        return all_item

    def addGp(self,item):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        add_sql="insert into deallist(code,name,bos,vol,price,bosdate,mission) values (%s,%s,%s,%s,%s,%s,%s)"
        val=(item.code,item.name,item.bos,item.vol,item.price,item.bosdate,item.mission)
        mysql_conn.ExecNonQuery(add_sql,val)

    def getInfo(self,code):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        info_sql="select code,name,bos,vol,price,bosdate from deallist where code=%s"\
                 "order by bosdate desc,id desc"
        dellist_rt = mysql_conn.ExecQueryVal(info_sql,code)
        all_item = [];
        for i in range(len(dellist_rt)):
            item_del = {}
            item_del['code'] = dellist_rt[i][0]
            item_del['name'] = dellist_rt[i][1]
            item_del['vol'] = int(decimal.Decimal(dellist_rt[i][3]).quantize(decimal.Decimal('0')))
            item_del['price'] = dellist_rt[i][4]
            item_del['bos'] = dellist_rt[i][2]
            item_del['bosdate'] = dellist_rt[i][5].strftime('%Y-%m-%d')
            all_item.append(item_del)
        return all_item

    def getDict(self,type):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        info_sql="select code,name,type from etf_gp_dict where type=%s"
        dellist_rt = mysql_conn.ExecQueryVal(info_sql,type)
        all_item = [];
        for i in range(len(dellist_rt)):
            item_del = {}
            item_del['id'] = dellist_rt[i][2]*1000+i+1
            item_del['code'] = dellist_rt[i][0]
            item_del['name'] = dellist_rt[i][1]
            item_del['type'] = dellist_rt[i][2]
            all_item.append(item_del)
        return all_item

    def delDict(self,code):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        info_sql = "delete from etf_gp_dict where code=%s"
        mysql_conn.ExecNonQuery(info_sql,code)

    def addDict(self,item):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        add_sql = "insert into etf_gp_dict(code,name,type) values (%s,%s,%s)"
        val = (item.code, item.name, item.type)
        mysql_conn.ExecNonQuery(add_sql, val)


    def getMonitorList(self,type):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        info_sql="select code,name,type from etf_gp_dict where monitor=1 and type=%s"
        dellist_rt = mysql_conn.ExecQueryVal(info_sql,type)
        all_item = [];
        for i in range(len(dellist_rt)):
            item_del = {}
            item_del['code'] = dellist_rt[i][0]
            item_del['name'] = dellist_rt[i][1]
            item_del['type'] = dellist_rt[i][2]
            all_item.append(item_del)
        return all_item


    def updEtfOrGpMon(self,codes,flag):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        upd_sql =''
        val = str(tuple(codes)).replace(',)', ')')
        if(flag=='left'):upd_sql = "update etf_gp_dict set monitor=0  where code in "+val
        if (flag == 'right'): upd_sql = "update etf_gp_dict set monitor=1  where code in "+val
        mysql_conn.ExecNonQuery(upd_sql,None)

    def getMonitorMsgList(self,code):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        # info_sql="select event_day," \
        #          "case when up_or_down='Up' then " \
        #          "CONCAT('延5日线从',start_day,'到',end_day,'连续',uddays,'日上涨，涨幅为',ud_rate,'%,',end_day,'日收盘价为',end_close,'与5日线',ma5,'之间幅率为',ma5_rate,'%') " \
        #          "when up_or_down='Down' then " \
        #          "CONCAT('延5日线从',start_day,'到',end_day,'连续',uddays,'日下跌，跌幅为',ud_rate,'%,',end_day,'日收盘价为',end_close,'与5日线',ma5,'之间幅率为',ma5_rate,'%') end msg " \
        #          "from monitor_etf_gp where code=%s order by event_day desc"
        info_sql ="select *" \
                  "from monitor_etf_gp where code=%s order by event_day desc"
        dellist_rt = mysql_conn.ExecQueryVal(info_sql,code)
        all_item = [];
        for i in range(len(dellist_rt)):
            item_del = {}
            code= dellist_rt[i][0]
            event_day= dellist_rt[i][1].strftime("%Y-%m-%d")
            start_day= dellist_rt[i][2].strftime("%Y-%m-%d")
            end_day= dellist_rt[i][3].strftime("%Y-%m-%d")
            start_close= dellist_rt[i][4]
            end_close= dellist_rt[i][5]
            up_or_down = dellist_rt[i][6]
            ma5= dellist_rt[i][7]
            uddays= dellist_rt[i][8]
            angle = dellist_rt[i][9]
            ud_rate = dellist_rt[i][10]
            ma5_rate = dellist_rt[i][11]
            msg=''
            if(up_or_down=='Up'):
                msg='延5日线从'+start_day+'到'+end_day+'连续'+str(uddays)+'日上涨，涨幅为'+str(ud_rate)+'%,'+end_day+'日收盘价为'+str(end_close)+'与5日线'+str(ma5)+'之间幅率为'+str(ma5_rate)+'%'
            if (up_or_down == 'Down'):
                msg = '延5日线从' + start_day + '到' + end_day + '连续' + str(uddays) + '日下跌，跌幅为' + str(ud_rate) + '%,' + end_day + '日收盘价为' + str(end_close) + '与5日线' + str(ma5) + '之间幅率为' + str(ma5_rate) + '%'
            item_del['event_day']=event_day
            item_del['msg'] = msg
            all_item.append(item_del)
        return all_item

    def getMonitorMsgListNew(self,sday=None,eday=None,code=None,ud=None):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        sel_sql="select * from monitor_etf_gp where 1=1"
        val=()
        if(sday!=None):
            sel_sql =sel_sql+ " and event_day>=%s and event_day<=%s"
            val=val+(sday,eday,)
        else:
            sel_sql = sel_sql + " and event_day=(select max(event_day) from monitor_etf_gp)"
        if(code!=None):
            sel_sql = sel_sql + " and code=%s"
            val=val+(code,)
        if(ud!=None):
            sel_sql = sel_sql + " and up_or_down=%s"
            val = val + (ud,)
        sel_sql=sel_sql + " order by code,event_day desc"
        print(sel_sql,val)
        dellist_rt = mysql_conn.ExecQueryVal(sel_sql, val)
        all_item = {"data":[]};
        for i in range(len(dellist_rt)):
            item_del = {}
            item_del['id'] = int(dellist_rt[i][0])+int(dellist_rt[i][1].strftime("%Y%m%d"))
            item_del['code']= dellist_rt[i][0]
            item_del['event_day']= dellist_rt[i][1].strftime("%Y-%m-%d")
            item_del['start_day']= dellist_rt[i][2].strftime("%Y-%m-%d")
            item_del['end_day']= dellist_rt[i][3].strftime("%Y-%m-%d")
            item_del['start_close']= dellist_rt[i][4]
            item_del['end_close']= dellist_rt[i][5]
            item_del['up_or_down'] = dellist_rt[i][6]
            item_del['ma5']= dellist_rt[i][7]
            item_del['uddays']= dellist_rt[i][8]
            item_del['angle'] = dellist_rt[i][9]
            item_del['ud_rate'] = dellist_rt[i][10]
            item_del['ma5_rate'] = dellist_rt[i][11]
            item_del['close_rate'] = dellist_rt[i][12]
            all_item.get('data').append(item_del)
        return all_item

    def getOneEtfList(self,code):
        mysql_conn = DBUtil(host='127.0.0.1', user="root", pwd="Boco@123", db="gpdb")
        sel_sql="select a.* from monitor_etf_gp a ,( "\
                "select code,max(event_day) event_day,start_day,max(end_day) end_day,up_or_down,max(uddays)  uddays "\
                "from monitor_etf_gp where code = %s "\
                "group by code,start_day,up_or_down) b "\
                "where a.code=b.code and a.event_day=b.event_day "\
                "order by code,event_day desc"
        val=(code)
        dellist_rt = mysql_conn.ExecQueryVal(sel_sql, val)
        all_item = [];
        for i in range(len(dellist_rt)):
            item_del = {}
            item_del['id'] = int(dellist_rt[i][0])+int(dellist_rt[i][1].strftime("%Y%m%d"))
            item_del['code']= dellist_rt[i][0]
            item_del['event_day']= dellist_rt[i][1].strftime("%Y-%m-%d")
            item_del['start_day']= dellist_rt[i][2].strftime("%Y-%m-%d")
            item_del['end_day']= dellist_rt[i][3].strftime("%Y-%m-%d")
            item_del['start_close']= dellist_rt[i][4]
            item_del['end_close']= dellist_rt[i][5]
            item_del['up_or_down'] = dellist_rt[i][6]
            item_del['ma5']= dellist_rt[i][7]
            item_del['uddays']= dellist_rt[i][8]
            item_del['angle'] = dellist_rt[i][9]
            item_del['ud_rate'] = dellist_rt[i][10]
            item_del['ma5_rate'] = dellist_rt[i][11]
            item_del['close_rate'] = dellist_rt[i][12]
            all_item.append(item_del)
        return all_item


if __name__ == "__main__":
    codes=['159905','1111']
    flag='left'
    sday='2023-04-01'
    eday='2023-04-03'
    val=()
    val=val+(sday,)
    val=val+(eday,)

    rs=FunUtil().getMonitorMsgListNew(sday=None,eday=None,code=None,ud=None)

    print(rs)