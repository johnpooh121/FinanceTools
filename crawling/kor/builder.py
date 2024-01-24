"""
build files from scratch
"""
import os.path
import time

import sender
import query
from util import *

base_path = os.path.dirname(os.path.abspath(__file__))
base_date = dt.datetime(year=2010, month=1, day=4)



def build_span(target="005930", adj=True):
    """
    Build a span of target stock
    :param target: stock code
    :param adj: whether to adjust
    :return:
    """
    near_workday = nearest_updateable_date_before_now()
    if adj == False:
        save_path = base_path + "/data/raw_span/" + target + ".csv"
    else:
        save_path = base_path + "/data/adj_span_from_query/" + target + ".csv"
    target_isucd = code_to_isucd(target)
    if target_isucd is None:
        return False
    print("building target : ",target)
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


def collect_targets_sorted(date = nearest_updateable_date_before_now()):
    daily_df = query.get_daily_quotation(date)
    daily_df = daily_df.astype({'시가총액': 'int64'})
    sorted_df = daily_df.sort_values(by=['시가총액'], ascending=False)
    return sorted_df


def collect_top_n_stock_until(n, adj=True):
    # stockinfo = pd.read_csv(base_path + "/data/basic/kor_basic.csv",encoding='euc-kr',dtype='str')
    stockinfo = collect_targets_sorted()
    count = 0
    for i, (_i_, stock) in enumerate(stockinfo.iterrows()):
        stockcode = stock['종목코드']
        print("collecting : ", stockcode, " : ", stock['종목명'], "\n")
        verd = build_span(target=stockcode, adj=adj)

        if not verd:
            return
        if i >= n:
            break

        count += 1
        if count >= 20:
            print("long sleep\n")
            time.sleep(20)
            count = 0
        else:
            time.sleep(7)
    pass


def build_adj_span_target(target):
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


def build_adj_span():
    stockinfo = collect_targets_sorted()
    for i, (_i_, stock) in enumerate(stockinfo.iterrows()):
        if i > 400:
            break
        stock_code = stock['종목코드']
        build_adj_span_target(stock_code)


def correct():
    stockinfo = collect_targets_sorted()
    for i, (j, stock) in enumerate(stockinfo.iterrows()):
        stockcode = stock['종목코드']
        path = base_path + "/data/span/" + stockcode + ".csv"
        if os.path.exists(path):
            print("inspecting : ", stockcode, "\n")
            df = pd.read_csv(path, encoding='euc-kr', dtype='str')
            dates = df['일자'].map(lambda x: dt.datetime.strptime(x, "%Y/%m/%d"))
            flag = False
            for i in range(1, len(df) - 1):
                if dates.iloc[i - 1].date() != nearest_opendate_before(dates.iloc[i] - dt.timedelta(days=1)).date():
                    print(stockcode + " : date error detected : ", df.iloc[i - 1]['일자'], "and", df.iloc[i]['일자'], "\n")
                    flag = True
            if flag:
                while len(df) >= 2:  # only fixes last 2 rows' duplication error
                    if df['일자'].iloc[len(df) - 1] == df['일자'].iloc[len(df) - 2]:
                        df = df.drop(len(df) - 1)
                    else:
                        break
                df.to_csv(path, encoding='euc-kr', index=False)


if __name__ == '__main__':
    # collect_until("005930", dt.datetime(2014, 3, 1))
    # collect_until("366030",dt.datetime.now())
    # collect_top_n_stock_until(400, dt.datetime.now(),adj=False)
    # sorted_df = collect_targets_sorted()
    # sorted_df.to_csv(base_path + "/data/sorted.csv",encoding="euc-kr", index=False)
    # correct()
    # build_adj_span()
    df = collect_targets_sorted()
    pass
