"""
build files from scratch
"""
import os.path
import time

from crawling.kor import sender
from crawling.kor.util import *
from crawling.kor import db

base_path = os.path.dirname(os.path.abspath(__file__))
base_date = dt.datetime(year=2010, month=1, day=4)


def build_span(target="005930", adj=True):
    """
    Build a span of target stock
    :param target: stock code
    :param adj: whether to adjust price or not
    :return: result of query, succeeded or failed
    """
    near_workday = nearest_updateable_date_before_now()
    if adj == False:
        save_path = base_path + "/data/raw_span/" + target + ".csv"
    else:
        save_path = base_path + "/data/adj_span_from_query/" + target + ".csv"
    target_isucd = code_to_isucd(target)
    if target_isucd is None:
        return False
    print("building target : ", target)
    count = 0
    while count < 10:
        count += 1
        path = sender.sendquery('span', target_code=target_isucd, start_date=base_date, end_date=near_workday,
                                adj=adj)

        if path is None:
            time.sleep(10)
            print("retrying\n")
            continue

        tmp = pd.read_csv(path, encoding='euc-kr', dtype='str')
        tmp_rev = tmp.loc[::-1]  # reverse
        tmp_rev.to_csv(save_path, index=False, encoding='euc-kr')
        break
    if count >= 10:
        return False

    return True


def build_daily_quotation(date, realtime=False):
    """
    build daily quotation for the given date
    :param date: datetime
    :param realtime: bool
    :return: path to the saved file
    """
    if not realtime and not is_ok_to_update(date):  # daily data only acquired after market close
        return None
    if realtime and not is_valid_date(date):
        return None
    fname = file_name('daily', date=date)
    path = base_path + '/data/daily/' + fname
    cnt = 0
    while not os.path.isfile(path) and cnt < 10:  # if already exists, don't send query again
        cnt += 1
        print(str(cnt) + "th trying..\n")
        sender.sendquery('daily', date=date)
        db.builder.make_table_daily(date)

    if not os.path.isfile(path):
        print("build failed!\n")
        return None
    return path


def collect_targets_sorted(date=nearest_updateable_date_before_now()):
    """
    Return stock information table sorted by market cap on a given date
    :param date:
    :return: sorted stock information
    """
    path = build_daily_quotation(date)
    daily_df = pd.read_csv(path, encoding='euc-kr', dtype='str')
    daily_df = daily_df.astype({'시가총액': 'int64'})
    sorted_df = daily_df.sort_values(by=['시가총액'], ascending=False)
    return sorted_df


def collect_top_n_stock_until(n, adj=True):
    """
    collect top n stocks by market cap
    :param n: number of collecting stocks
    :param adj: whether to adjust the price or not
    :return: None
    """
    stockinfo = collect_targets_sorted()
    count = 0
    for i, (_i_, stock) in enumerate(stockinfo.iterrows()):
        if i >= n: break
        stockcode = stock['종목코드']
        print("collecting : ", stockcode, " : ", stock['종목명'], "\n")
        verd = build_span(target=stockcode, adj=adj)
        if not verd:
            return
    pass


def build_adj_span_target(target):
    """
    Build adj_span file for target
    :param target: target stock code
    :return: None
    """
    print("building ", target, " :\n")
    path_raw = base_path + "/data/raw_span/" + target + ".csv"
    path_query = base_path + "/data/adj_span_from_query/" + target + ".csv"
    path_save = base_path + "/data/adj_span/" + target + ".csv"
    if not os.path.exists(path_raw) or not os.path.exists(path_query):
        print("no data\n")
        return
    df_raw = pd.read_csv(path_raw, encoding='euc-kr')
    df_query = pd.read_csv(path_query, encoding='euc-kr')
    df_raw[['종가', '거래량']] = df_raw[['종가', '거래량']].astype('int64')
    df_query[['종가', '거래량']] = df_query[['종가', '거래량']].astype('int64')
    if len(df_raw) != len(df_query):
        print("length inconsistent\n")
        return
    for i, row in df_raw.iterrows():
        df_query.loc[i, '거래량'] = round(df_query.loc[i, '거래량'] * (df_raw.loc[i, '종가'] / df_query.loc[i, '종가']))
    df_query.to_csv(path_save, encoding='euc-kr', index=False)


def build_basic_information_today():
    """
    save today's basic information query result
    :return: bool
    """
    fname = file_name('basic')
    path = base_path + '/data/basic/' + fname
    cnt = 0
    while not os.path.isfile(path) and cnt < 10:
        cnt += 1
        print(str(cnt) + "th trying for basic info get..\n")
        sender.sendquery('basic')
    if os.path.isfile(path):
        db.builder.make_table_basic(dt.datetime.now()) # db
        return True

    print("basic query failed!\n");
    return False


if __name__ == '__main__':
    pass
