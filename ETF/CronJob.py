from apscheduler.schedulers.blocking import BlockingScheduler
import datetime

from ETF.EtfMon import EtfMon

scheduler = BlockingScheduler()

@scheduler.scheduled_job("cron", day_of_week="0-4",hour="14",minute="45")
@scheduler.scheduled_job("cron", day_of_week="0-4",hour="15",minute="05")
def request_update_status():
    now = datetime.datetime.now().strftime("%Y%m%d")
    EtfMon().insUpOrDown(now, 40)
    print("Doing job")

scheduler.start()