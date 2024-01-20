import os.path
import time

import sender
import query
import datetime as dt
from util import *

base_path = os.path.dirname(os.path.abspath(__file__))
base_date = dt.datetime(year=2010, month=1, day=4)


def collect_until(target="005930", date=dt.datetime.now(), adj=True):
    near_workday = nearest_opendate_before(date)
    if adj==False:
        save_path = base_path + "/data/span/" + target + ".csv"
    else:
        save_path = base_path + "/data/adj_span/" + target + ".csv"
    target_isucd = code_to_isucd(target)
    if target_isucd is None:
        return False

    if os.path.exists(save_path):
        tmp = pd.read_csv(save_path, encoding="euc-kr", dtype='str')
        curr_date = dt.datetime.strptime(tmp.iloc[len(tmp) - 1]['일자'], "%Y/%m/%d")
    else:
        curr_date = base_date

    if curr_date.date() >= date.date():
        print(target, " : skipping due to date already filled\n")
        return True

    count = 0
    while count < 10:
        count += 1
        (verd, path) = sender.sendquery('span', target_code=target_isucd, start_date=curr_date, end_date=near_workday, adj=adj)

        if not verd:
            time.sleep(10)
            print("retrying\n")
            continue

        tmp = pd.read_csv(path, encoding='euc-kr', dtype='str')
        tmp_rev = tmp.loc[::-1]  # reverse
        if os.path.exists(save_path):
            tmp_rev.to_csv(save_path, mode='a', index=False, encoding='euc-kr', header=False)
        else:
            tmp_rev.to_csv(save_path, mode='a', index=False, encoding='euc-kr', header=True)
        break

    if count >= 10:
        return False

    return True


def collect_targets_sorted():
    daily_df = query.get_daily_quotation(nearest_opendate_before(dt.datetime.now()-dt.timedelta(days=1)))
    daily_df = daily_df.astype({'시가총액': 'int64'})
    sorted_df = daily_df.sort_values(by=['시가총액'], ascending=False)
    return sorted_df


def collect_top_n_stock_until(n, date, adj=True):
    # stockinfo = pd.read_csv(base_path + "/data/basic/kor_basic.csv",encoding='euc-kr',dtype='str')
    stockinfo = collect_targets_sorted()
    count = 0
    for i, (_i_, stock) in enumerate(stockinfo.iterrows()):
        stockcode = stock['종목코드']
        print("collecting : ", stockcode, " : ", stock['종목명'], "\n")
        verd = collect_until(target=stockcode, date=date, adj=adj)

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


def collect_from_daily_data(date = nearest_opendate_before(dt.datetime.now()-dt.timedelta(days=1))):
    """
    collect daily data from a given date

    default date is a business day before today ( for up-to-date Close-Price )
    :param date:
    :return:
    """

    daily_df = query.get_daily_quotation(date)
    for i, row in daily_df.iterrows():
        stockcode = row['종목코드']
        path = base_path + "/data/span/" + stockcode + ".csv"
        # path = base_path + "/data/test/" + stockcode + ".csv"
        if not os.path.exists(path):
            continue
        print("collecting : ", stockcode, "\n")
        df = pd.read_csv(path, encoding='euc-kr', dtype='str').dropna()
        last_date = dt.datetime.strptime(df.iloc[len(df) - 1]['일자'], "%Y/%m/%d")
        if last_date.date() != nearest_opendate_before(date-dt.timedelta(days=1)).date():
            print("date inconsistency detected!\n");
            continue
        val=row[['종가','대비','등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']]
        val['일자']=date.strftime('%Y/%m/%d')
        # df.loc[len(df)] = val
        val.to_frame().T.to_csv(path, encoding='euc-kr',index=False, mode='a', header=False,
                   columns=['일자','종가','대비','등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수'],)

# todo : daily date update 루틴 짜기, 이 때 종가 추출을 위해 16시 이후 실행시키기, 액면분할 감지 시스템 넣기, 쿼리 마무리
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
                if dates.iloc[i - 1].date() != nearest_opendate_before(dates.iloc[i]-dt.timedelta(days=1)).date():
                    print(stockcode + " : date error detected : ",df.iloc[i-1]['일자'],"and", df.iloc[i]['일자'], "\n")
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
    collect_top_n_stock_until(400, dt.datetime.now(),adj=False)
    # sorted_df = collect_targets_sorted()
    # sorted_df.to_csv(base_path + "/data/sorted.csv",encoding="euc-kr", index=False)
    # correct()
    # collect_from_daily_data()
