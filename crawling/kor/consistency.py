"""
check consistency
"""
from crawling.kor import collect_sorted
from util import *
import builder

def date_check(dir_name='raw_span',do_correction=False):
    """
    detect date inconsistency in the given dir
    :param dir_name: dir name
    :param do_correction: do correction or not
    :return: None
    """
    print("inspecting ",dir_name,"\n")
    stockinfo = collect_sorted.collect_targets_sorted()
    for i, (j, stock) in enumerate(stockinfo.iterrows()):
        stockcode = stock['종목코드']
        path = base_path + "/data/"+dir_name+"/" + stockcode + ".csv"
        if os.path.exists(path):
            # print("inspecting : ", stockcode, "\n")
            df = pd.read_csv(path, encoding='euc-kr', dtype='str')
            dates = df['일자'].map(lambda x: dt.datetime.strptime(x, "%Y/%m/%d"))
            flag = False
            k=0
            for i in range(1, len(df)):
                if dates.iloc[i - 1].date() != nearest_opendate_before(dates.iloc[i] - dt.timedelta(days=1)).date():
                    print(stockcode + " : date error detected : ", df.iloc[i - 1]['일자'], "and", df.iloc[i]['일자'], "\n")
                    flag = True
                if not flag:
                    k=i
            if flag and do_correction:
                df.loc[:k].to_csv(path, encoding='euc-kr', index=False)

def adjprice_check(dir_name='adj_span_from_query'):
    stockinfo = collect_sorted.collect_targets_sorted()
    for i, (j, stock) in enumerate(stockinfo.iterrows()):
        stockcode = stock['종목코드']
        path = base_path + "/data/" + dir_name + "/" + stockcode + ".csv"
        if os.path.exists(path):
            print("inspecting : ", stockcode, "\n")
            df = pd.read_csv(path, encoding='euc-kr')
            for i in range(1,len(df)):
                if int(df.iloc[i-1]['종가'])+int(df.iloc[i]['대비'])!=int(df.iloc[i]['종가']):
                    print("adj price inconsistency detected on : ",df.iloc[i]['일자'])

    pass

if __name__ == '__main__':
    adjprice_check()