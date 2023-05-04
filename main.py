from datetime import datetime

from fastapi import FastAPI,Query

from ETF.EtfMon import EtfMon
from FunUtil import FunUtil
import uvicorn
import easyquotation as eq
import pandas as pd
from pydantic import BaseModel
from typing import Optional, List


class Item(BaseModel):
    code: str
    name:str
    bos: str;
    vol: int;
    price: float;
    bosdate: str;
    mission:float;

class Dict(BaseModel):
    code: str
    name:str
    type: int;
    id: int;
    index: float;

qs = eq.use('sina') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
#dw = qs.market_snapshot(prefix=True) # prefix 参数指定返回的行情字典中的股票代码 key 是否带 sz/sh 前缀

# 类似于 app = Flask(__name__)
app = FastAPI()

# 绑定路由和视图函数
@app.get("/api/getall")
async def index(type:int):
    result=FunUtil().getallinfo(type)
    return result


@app.get("/api/now")
async def get_info(id: str):
    gp = qs.real(id)
    df_a = pd.DataFrame(gp)
    df_b = df_a.stack()
    df = df_b.unstack(0)
    avg = df['volume'][0] / df['turnover'][0] if int(df['turnover'][0]) != 0 else 0
    now = df['now'][0]
    rate = '%.4f' % float((now - avg) / avg) if float(avg) != 0 else 0
    zdf = '%.4f' % float((now - float(df['close'][0])) / float(df['close'][0])) if float(df['close'][0]) != 0 else 0
    avg='%.3f' %avg
    return {'code':id,'now':now,'close':df['close'][0],'avg':avg,'rate':rate,'zdf':zdf,'name':str(df['name'][0])}

@app.get("/api/getInfo")
async def get_msg(id:str):
    result=FunUtil().getInfo(id)
    return result


@app.post('/api/addgp/item')
async def add_item(item: Item):
    FunUtil().addGp(item)
    print(item.code)
    return item

@app.get("/api/dict")
async def get_msg(type:int):
    result=FunUtil().getDict(type)
    return result

@app.get("/api/dict/del")
async def get_msg(code:int):
    FunUtil().delDict(code)
    return "success"

@app.post("/api/dict/add")
async def get_msg(dict:Dict):
    FunUtil().delDict(dict.code)
    FunUtil().addDict(dict)
    return "success"

@app.get("/api/monitor/list")
async def get_msg(type:int):
    result=FunUtil().getMonitorList(type)
    return result

@app.get("/api/monitor/upd")
async def get_msg(flag:str,codes :Optional[List[str]] = Query([], max_length=50)):
    FunUtil().updEtfOrGpMon(codes,flag)
    return "success"

@app.get("/api/monitor/etf")
async def get_msg():
    result=EtfMon().getEtfList()
    return result

@app.get("/api/monitor/msglist")
async def get_msg(code:str):
    result=FunUtil().getMonitorMsgList(code)
    return result

@app.get("/api/monitor/etfMonlist")
async def get_msg(sday:Optional[str] = None,eday:Optional[str] = None,code:Optional[str] = None,up_or_down:Optional[str] = None):
    result=FunUtil().getMonitorMsgListNew(sday=sday,eday=eday,code=code,ud=up_or_down)
    return result

# 在 Windows 中必须加上 if __name__ == "__main__"，否则会抛出 RuntimeError: This event loop is already running
if __name__ == "__main__":
    # 启动服务，因为我们这个文件叫做 main.py，所以需要启动 main.py 里面的 app
    # 第一个参数 "main:app" 就表示这个含义，然后是 host 和 port 表示监听的 ip 和端口
    uvicorn.run("main:app", host="127.0.0.1", port=5555)