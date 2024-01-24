from util import *
import query
import builder
import time

base_path = os.path.dirname(os.path.abspath(__file__))


def daily_update_for_today(n=400):
    query.save_basic_information_today()
    date = nearest_updateable_date_before_now()
    query.get_daily_quotation(date)
    update_raw_span_auto(n)
    update_query_span_auto(n)
    update_adj_span_auto(n)


def update_raw_span(n, date=nearest_opendate_before(dt.datetime.now() - dt.timedelta(days=1))):
    if not is_ok_to_update(date):
        print("invalid datetime for updating raw span\n")
        return
    daily_df = builder.collect_targets_sorted(date)
    for i, (_i_, row) in enumerate(daily_df.iterrows()):
        if i >= n: break;
        stock_code = row['종목코드']
        path = base_path + "/data/raw_span/" + stock_code + ".csv"
        print("collecting : ", stock_code, "\n")
        if not os.path.exists(path):
            print("no data, downloading")
            builder.build_span(stock_code,adj=False)
            continue
        df = pd.read_csv(path, encoding='euc-kr', dtype='str')
        last_date = dt.datetime.strptime(df.iloc[len(df) - 1]['일자'], "%Y/%m/%d")
        if last_date.date() != nearest_opendate_before(date - dt.timedelta(days=1)).date():
            print("date inconsistency detected!\n")
            builder.build_span(stock_code,adj=False)
            continue
        val = row[['종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']]
        val['일자'] = date.strftime('%Y/%m/%d')
        val.to_frame().T.to_csv(path, encoding='euc-kr', index=False, mode='a', header=False,
                                columns=['일자', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수'], )


def update_query_span(n, date=nearest_opendate_before(dt.datetime.now() - dt.timedelta(days=1))):
    if not is_ok_to_update(date):
        print("invalid datetime for updating query span\n")
        return
    prev_df = query.get_daily_quotation(nearest_opendate_before(date - dt.timedelta(days=1)))
    daily_df = builder.collect_targets_sorted(date)
    for i, (_, row) in enumerate(daily_df.iterrows()):
        if i >= n: break;
        stock_code = row['종목코드']
        path = base_path + "/data/adj_span_from_query/" + stock_code + ".csv"
        search = prev_df[prev_df['종목코드'] == stock_code]
        print("collecting adj_query : ", stock_code, "\n")
        if not os.path.exists(path) or search.empty:
            print("no data, downloading")
            builder.build_span(stock_code, adj=True)
            continue

        search_row = search.iloc[0]
        if int(search_row['종가']) != int(row['종가']) - int(row['대비']):
            print("recapitalization detected\n")
            builder.build_span(stock_code, adj=True)
            continue

        df = pd.read_csv(path, encoding='euc-kr', dtype='str').dropna()
        last_date = dt.datetime.strptime(df.iloc[len(df) - 1]['일자'], "%Y/%m/%d")
        if last_date.date() != nearest_opendate_before(date - dt.timedelta(days=1)).date():
            print("date inconsistency detected!\n")
            builder.build_span(stock_code, adj=True)
            continue
        val = row[['종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']]
        val['일자'] = date.strftime('%Y/%m/%d')
        val.to_frame().T.to_csv(path, encoding='euc-kr', index=False, mode='a', header=False,
                                columns=['일자', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수'], )


def update_adj_span(n, date=nearest_opendate_before(dt.datetime.now() - dt.timedelta(days=1))):
    if not is_ok_to_update(date):
        print("invalid datetime for updating adj span\n")
        return
    prev_df = query.get_daily_quotation(nearest_opendate_before(date - dt.timedelta(days=1)))
    daily_df = builder.collect_targets_sorted(date)
    for i, (_, row) in enumerate(daily_df.iterrows()):
        if i>=n : break
        stock_code = row['종목코드']
        path = base_path + "/data/adj_span/" + stock_code + ".csv"
        search = prev_df[prev_df['종목코드'] == stock_code]
        if not os.path.exists(path) or search.empty:
            print("no data, building")
            builder.build_adj_span_target(stock_code)
            continue
        print("collecting adj : ", stock_code, "\n")
        search_row = search.iloc[0]
        if int(search_row['종가']) != int(row['종가']) - int(row['대비']):
            print("recapitalization detected\n")
            builder.build_adj_span_target(stock_code)
            continue

        df = pd.read_csv(path, encoding='euc-kr', dtype='str').dropna()
        last_date = dt.datetime.strptime(df.iloc[len(df) - 1]['일자'], "%Y/%m/%d")
        if last_date.date() != nearest_opendate_before(date - dt.timedelta(days=1)).date():
            print("date inconsistency detected!\n")
            builder.build_adj_span_target(stock_code)
            continue
        val = row[['종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']]
        val['일자'] = date.strftime('%Y/%m/%d')
        val.to_frame().T.to_csv(path, encoding='euc-kr', index=False, mode='a', header=False,
                                columns=['일자', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수'], )


def get_oldest_date(n,folder_name):
    ret = dt.datetime.now()
    if not os.path.exists(base_path + "/data/" + folder_name):
        return
    stockinfo = builder.collect_targets_sorted()
    for i,(_i_, stock) in enumerate(stockinfo.iterrows()):
        if i>=n:break
        stock_code = stock['종목코드']
        path = base_path + "/data/" + folder_name + "/" + stock_code + ".csv"
        if os.path.exists(path):
            df = pd.read_csv(path, encoding='euc-kr')
            ret = min(ret, dt.datetime.strptime(df.iloc[len(df) - 1]['일자'], "%Y/%m/%d"))
    return ret


def update_raw_span_auto(n):
    print("auto-updating raw data\n")
    date = get_oldest_date(n,"raw_span")
    curr = date + dt.timedelta(days=1)
    end_date = nearest_updateable_date_before_now()
    while curr.date() <= end_date.date():
        update_raw_span(n, curr)
        curr += dt.timedelta(days=1)


def update_query_span_auto(n):
    print("auto-updating query-span data\n")
    date = get_oldest_date(n,"adj_span_from_query")
    curr = date + dt.timedelta(days=1)
    end_date = nearest_updateable_date_before_now()
    while curr.date() <= end_date.date():
        update_query_span(n, curr)
        curr += dt.timedelta(days=1)


def update_adj_span_auto(n):
    print("auto-updating adj-span data\n")
    date = get_oldest_date(n,"adj_span")
    curr = date + dt.timedelta(days=1)
    end_date = nearest_updateable_date_before_now()
    while curr.date() <= end_date.date():
        update_adj_span(n, curr)
        curr += dt.timedelta(days=1)


if __name__ == '__main__':
    n = 400
    daily_update_for_today(n)
