from datetime import datetime, timedelta
from chinese_calendar import is_holiday

class Util:
    def workdays(self,start, end):
        '''
        计算两个日期间的工作日
        start:开始时间
        end:结束时间
        '''
        # 字符串格式日期的处理
        if type(start) == str:
            start = datetime.strptime(start, '%Y-%m-%d').date()
        if type(end) == str:
            end = datetime.strptime(end, '%Y-%m-%d').date()
            # 开始日期大，颠倒开始日期和结束日期
        if start > end:
            start, end = end, start
        counts = 0
        while True:
            if start > end:
                break
            if is_holiday(start) or start.weekday() == 5 or start.weekday() == 6:
                start += timedelta(days=1)
                continue
            counts += 1
            start += timedelta(days=1)
        return counts

    def isHoliday(self,day):
        if type(day) == str:
            day = datetime.strptime(day, '%Y%m%d').date()
        if is_holiday(day) or day.weekday() == 5 or day.weekday() == 6:
            return True
        else:
            return False



if __name__ == "__main__":
    #rt=Util().workdays('2023-03-15','2023-03-31')
    rt=Util().isHoliday('20230401')
    print(rt)