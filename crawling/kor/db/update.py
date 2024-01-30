from crawling.kor import collect_sorted
from crawling.kor.db.glob import *
from crawling.kor.util import *
from crawling.kor.db import builder

base_date = dt.datetime(2010, 1, 4)


def db_auto_update_from_csv_basic():
    curr = base_date
    while curr.date() <= dt.datetime.now().date():
        path = base_path + "/data/basic/" + file_name('basic', curr)
        tname = table_name('basic')
        if os.path.exists(path):
            builder.load_csv('basic',date=curr)
        curr += dt.timedelta(days=1)


def db_auto_update_from_csv_daily():
    curr = base_date
    while curr.date() <= dt.datetime.now().date():
        path = base_path + "/data/daily/" + file_name('daily', curr)
        tname = table_name('daily')
        if os.path.exists(path):
            print("table newly created : ", tname)
            builder.load_csv('daily',date=curr)
        curr += dt.timedelta(days=1)


def db_auto_update_from_csv_span(adj):
    daily_df = collect_sorted.collect_targets_sorted()
    for i, (_i_, row) in enumerate(daily_df.iterrows()):
        # if i >= n: break;
        stock_code = row['종목코드']
        path = file_path('span',adj=adj,target=stock_code)
        tname = table_name('span', adj=adj)
        if os.path.exists(path):
            print("making table : ", tname,"stock : ",stock_code)
            builder.load_csv('span',adj=adj,target=stock_code)


if __name__ == '__main__':
    builder.make_every_table()
    db_auto_update_from_csv_basic()
    db_auto_update_from_csv_daily()
    db_auto_update_from_csv_span(adj=False)
    db_auto_update_from_csv_span(adj=True)
    pass
