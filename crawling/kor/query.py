import datetime as dt
import cloudscraper
import pandas as pd
import sender
import time
from util import *
import builder

base_path = os.path.dirname(os.path.abspath(__file__))
base_date = dt.datetime(year=2010, month=1, day=4)

def get_daily_quotation(date,realtime = False):
    """
    get daily quotation for the given date
    :param date: datetime object
    :param realtime: whether to allow realtime query
    :return: DataFrame
    """
    path = builder.build_daily_quotation(date,realtime=realtime)
    if os.path.isfile(path):
        return pd.read_csv(path,encoding='euc-kr',dtype='str')
    print("query failed!\n")

def get_span_quotation(target, adj=False):
    """
    get span quotation for the given target on the given time interval
    :param target: stock code string
    :param adj: boolean
    :return: dataframe
    """
    if adj:
        path = base_path+'/data/adj_span/'+target+'.csv'
    else:
        path = base_path+'/data/raw_span/'+target+'.csv'

    if not os.path.exists(path):
        builder.build_span(target, adj)

    df = pd.read_csv(path,encoding='euc-kr',dtype='str')
    return df

def get_basic_information(date=dt.datetime.now()):
    """
    returns basic info data for a given date
    if date is not specified, it collects today's basic info
    available date is only after 2024/1/16
    :param date:
    :return:
    """
    builder.build_basic_information_today()
    fname = file_name('basic', date=date)
    path = base_path+'/data/basic/'+fname
    if os.path.exists(path):
        return pd.read_csv(path,encoding='euc-kr',dtype='str')
    return None

if __name__=='__main__':
    # get_daily_quoatation(dt.datetime(year=2024,month=1,day=16))
    # get_daily_quotation(nearest_opendate_before(dt.datetime.now()-dt.timedelta(days=1)))
    pass