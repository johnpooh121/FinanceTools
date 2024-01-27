"""
utility functions
"""

import datetime as dt
import os
import pandas as pd

base_path = os.path.dirname(os.path.abspath(__file__)).replace(os.sep,'/')
weekday_holidays = set(map(lambda x: dt.datetime.strptime(x, "%Y-%m-%d"),
                           pd.read_csv(base_path + "/data/holidays/holiday_total.csv", dtype='str')[
                               'week holiday'].values))

# code_df = pd.read_csv(base_path + "/data/basic/kor_basic_240126.csv", encoding='euc-kr')[['단축코드', '표준코드']]
code_df = pd.read_csv(base_path + "/data/basic/kor_basic.csv",
                      encoding='euc-kr')[['단축코드', '표준코드']]


def file_name(type, date=dt.datetime.now(), adj=True, **kwargs):
    """
    file name for the given query type
    :param type: query type
    :param date: date for 'daily' type query
    :param adj: boolean to adjust
    :param kwargs: start date, end date, target code
    :return:
    """
    if type == 'daily':
        ret = 'kor_daily_' + date.strftime('%y%m%d') + '.csv'
        return ret
    if type == 'span':
        start_date = kwargs['start_date'];
        end_date = kwargs['end_date'];
        target_code = kwargs['target_code'];
        ret = ('kor_' + ('adj_' if adj else '') + 'span_' + target_code + '_' +
               start_date.strftime('%y%m%d') + '_' + end_date.strftime('%y%m%d') + '.csv')
        return ret
    if type == 'basic':
        ret = 'kor_basic_' + date.strftime('%y%m%d') + '.csv'
        return ret
    raise Exception("type invalid")


def is_valid_date(date):
    """
    check whether the market is open or closed on the given date
    :param date: datetime object to check
    :return: bool
    """
    if date.weekday() >= 5:
        return False
    if date in weekday_holidays:
        return False
    return True


def nearest_opendate_before(date):
    """
    get closest open date before the given date (hour is NOT considered)
    :param date: datetime object
    :return: datetime object
    """
    while True:
        if date.weekday() <= 4 and date not in weekday_holidays:
            break
        date -= dt.timedelta(days=1)
    return date


def is_ok_to_update(date):
    """
    check whether it is ok to update for the given date or not
    :param date: datetime object
    :return: bool
    """
    if not is_valid_date(date):
        return False
    if (dt.datetime.now() - dt.timedelta(days=1)).date() >= date.date():
        return True
    if dt.datetime.now().date() == date.date():
        if dt.datetime.now().hour > 18:  # After today's market closed
            return True
    return False


def nearest_updateable_date_before_now():
    """
    get closest updateable date before now
    :return: aatetime object
    """
    date = dt.datetime.now()
    if is_ok_to_update(date):
        return date
    return nearest_opendate_before(date - dt.timedelta(days=1))


def code_to_isucd(target):
    """
    convert stock code to isucd
    :param target: code string of the target
    :return: isucd string
    """
    search_code = code_df[code_df['단축코드'].str.contains(target)]
    if search_code.empty:
        print("Target Code not found!!\n")
        return None
    return search_code['표준코드'].values[0]
    pass


if __name__ == '__main__':
    pass
