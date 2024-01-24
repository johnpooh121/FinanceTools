import pandas as pd
import os
import builder
import datetime as dt

base_path = os.path.dirname(os.path.abspath(__file__))


def adjust_target_file(target):
    """
    read /data/raw_span/target.csv and adjust, save it to /data/adj_span/target.csv
    :param target:
    :return:
    """
    read_path = base_path + "/data/raw_span/" + target + ".csv"
    if not os.path.exists(read_path):
        return
    df = pd.read_csv(read_path, encoding="euc-kr", dtype='str')
    df[['종가', '대비', '시가', '고가', '저가', '거래량', '상장주식수']] = df[['종가', '대비', '시가', '고가', '저가', '거래량', '상장주식수']].astype(
        'float64')
    print("converting target ", target, "\n")
    prev_total_stock = df.loc[0, '상장주식수']
    prev_end_price = df.loc[0, '종가']
    for i in range(1, len(df)):
        # if df.loc[i, '시가'] == 0:
        #     continue
        p = df.iloc[i]['상장주식수'] / prev_total_stock
        prev_price_est = df.loc[i, '종가'] - df.loc[i, '대비']
        if prev_price_est != prev_end_price:
            n = prev_end_price / prev_price_est
            print(" stock split detected : ", df.iloc[i]['일자'], " , factor : ", n, " ", p, "\n")
            df.loc[:i - 1, ['종가', '대비', '시가', '고가', '저가']] = df.loc[:i - 1, [''
                                                                             '종가', '대비', '시가', '고가', '저가']].div(n)
            df.loc[:i - 1, '거래량'] = df.loc[:i - 1, '거래량'].div(1 / n)
        prev_end_price = df.iloc[i]['종가']
        prev_total_stock = df.iloc[i]['상장주식수']
    df[['종가', '대비', '시가', '고가', '저가', '거래량', '상장주식수']] = df[['종가', '대비', '시가', '고가', '저가', '거래량', '상장주식수']].round(
        decimals=0)
    df[['종가', '대비', '시가', '고가', '저가', '거래량', '상장주식수']] = df[['종가', '대비', '시가', '고가', '저가', '거래량', '상장주식수']].astype(
        'int64')
    df.to_csv(base_path + "/data/adj_span/" + target + ".csv", encoding='euc-kr', index=False)
    pass


def compare_adjquery_adjcustom(n=400):
    stockinfo = builder.collect_targets_sorted()
    for _, row in stockinfo.iterrows():
        stock_code = row['종목코드']
        path_query = base_path + "/data/adj_span_from_query/" + stock_code + ".csv"
        path_adj = base_path + "/data/adj_span/" + stock_code + ".csv"
        if os.path.exists(path_query) and os.path.exists(path_adj):
            df_query = pd.read_csv(path_query, encoding='euc-kr',dtype='str')
            df_adj = pd.read_csv(path_adj, encoding='euc-kr',dtype='str')
            print("target : ",stock_code,"\n")
            for i in range(min(len(df_query), len(df_adj))):
                adj_close = int(df_adj.iloc[i]['종가'])
                adj_query = int(df_query.iloc[i]['종가'])
                if int(df_adj.iloc[i]['거래량'])==0:
                    continue
                if abs(adj_close - adj_query) >1:
                    print("     inconsistency at date ", df_adj.loc[i, '일자'], "\n")


def build_adjust_folder():
    stockinfo = builder.collect_targets_sorted()
    for i, row in stockinfo.iterrows():
        adjust_target_file(row['종목코드'])

def build_file_w_adj_volume(target):
    path_query = base_path + "/data/adj_span_from_query/"+target+".csv"
    path_raw = base_path + "/data/raw_span/"+target+".csv"
    if not os.path.exists(path_query) or not os.path.exists(path_raw):
        print("path not exists\n")
        return
    df_query = pd.read_csv(path_query, encoding='euc-kr')
    df_raw = pd.read_csv(path_raw, encoding='euc-kr')
    if len(df_query) != len(df_raw):
        print("end date not same\n")
        return
    df_query[['종가','거래량']] = df_query[['종가','거래량']].astype('int64')
    df_raw[['종가', '거래량']] = df_raw[['종가', '거래량']].astype('int64')
    for i in range(len(df_raw)):
        adj_price = df_query.loc[i,'종가']
        raw_price = df_raw.loc[i,'종가']
        df_query.loc[i,'거래량'] = df_query.loc[i,'거래량'].mul(raw_price/adj_price).round(decimals=0)


if __name__ == '__main__':
    # adjust_target_file('214150')
    # build_adjust_folder()
    compare_adjquery_adjcustom()