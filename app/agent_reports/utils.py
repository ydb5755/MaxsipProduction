import time
from datetime import datetime, timedelta

def get_range(date:float, date_offset:int) -> str:
    today_minus_offset = time.mktime((datetime.today() - timedelta(days=date_offset)).date().timetuple())
    three_days = time.mktime((datetime.today() - timedelta(days=3 + date_offset)).date().timetuple())
    seven_days = time.mktime((datetime.today() - timedelta(days=7 + date_offset)).date().timetuple())
    fifteen_days = time.mktime((datetime.today() - timedelta(days=15 + date_offset)).date().timetuple())

    if three_days <= date <= today_minus_offset:
        return '1-3'
    if seven_days <= date < three_days:
        return '4-7'
    if fifteen_days <= date < seven_days:
        return '8-15'
    if date < fifteen_days:
        return '16-30'
