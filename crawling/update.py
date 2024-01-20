import datetime as dt
from util import *
import collector
import query

base_path = os.path.dirname(os.path.abspath(__file__))

def daily_update_for_today():
    date = nearest_opendate_before(dt.datetime.now() - dt.timedelta(days=1))
    if dt.datetime.now().hour >= 18:
        date = nearest_opendate_before(dt.datetime.now())
    daily_update(date)

def daily_update(date):
    query.save_basic_information_today()
    query.get_daily_quotation(date)

    if not check_stock_split(date,correct=False):
        collector.collect_from_daily_data(date)

    pass

def check_stock_split(date,correct):
    yd = nearest_opendate_before(date-dt.timedelta(days=1))
    prev_df=query.get_basic_information(yd)
    curr_df=query.get_basic_information(date)
    for i,row in curr_df.iterrows():
        stockcode = row['단축코드']
        search_df = prev_df.loc[prev_df['단축코드']==stockcode]
        if search_df.empty:
            continue
        prev_row=search_df.iloc[0]
        if row['액면가']!=prev_row['액면가']:
            print(stockcode," : stock split detected from ",prev_row['액면가']," to ",row['액면가'],"\n")
            if correct:
                divide_target(stockcode,int(prev_row['액면가'])/int(row['액면가']))
    return False

def divide_target(stockcode,n):
    path = base_path+"/data/span/"+stockcode+".csv"
    if not os.path.exists(path):
        print("No data found for stock ",stockcode)
        return False
    df = pd.read_csv(path,encoding='euc-kr',dtype='str')
    # df[]
    pass

# def check_stock_split_past(stockcode):
#     path = base_path+"/data/span/"+stockcode+".csv"
#     df = pd.read_csv(path,encoding='euc-kr',dtype='str')
#     for i in range(1,len(df)):
#         row1=df.iloc[i-1]
#         row2=df.iloc[i]
#         if row1[('\''
#                  '])

if __name__ == '__main__':
    daily_update_for_today()