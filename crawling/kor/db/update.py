from crawling.kor.db.glob import *
from crawling.kor.util import *
import crawling.kor.builder
import query, builder

base_date = dt.datetime(2010, 1, 4)


def db_auto_update_from_csv_basic():
    curr = base_date
    while curr.date() <= dt.datetime.now().date():
        path = base_path + "/data/basic/" + file_name('basic', curr)
        tname = table_name('basic', date=curr)
        if os.path.exists(path):
            if not query.is_table_exist(tname):
                print("table newly created : ", tname)
                builder.make_table_basic(curr)
        curr += dt.timedelta(days=1)


def db_auto_update_from_csv_daily():
    curr = base_date
    while curr.date() <= dt.datetime.now().date():
        path = base_path + "/data/daily/" + file_name('daily', curr)
        tname = table_name('daily', date=curr)
        if os.path.exists(path):
            if not query.is_table_exist(tname):
                print("table newly created : ", tname)
                builder.make_table_daily(curr)
        curr += dt.timedelta(days=1)


def db_auto_update_from_csv_span(type):
    daily_df = crawling.kor.builder.collect_targets_sorted()
    for i, (_i_, row) in enumerate(daily_df.iterrows()):
        # if i >= n: break;
        stock_code = row['종목코드']
        if type == 'raw':
            path = base_path + "/data/raw_span/" + stock_code + ".csv"
        else:
            path = base_path + "/data/adj_span/" + stock_code + ".csv"
        tname = table_name('span', adj=(type == 'adj'), target=stock_code)
        if os.path.exists(path) and not query.is_table_exist(tname):
            print("making table : ", tname)
            builder.make_table_span(type, stock_code)


def db_auto_update_from_csv():
    """
    guarantee that csv and db are consistent after this function
    :return:
    """
    daily_df = builder.collect_targets_sorted()
    for i, (_i_, row) in enumerate(daily_df.iterrows()):
        # if i >= n: break;
        stock_code = row['종목코드']
        path = base_path + "/data/raw_span/" + stock_code + ".csv"
    pass


if __name__ == '__main__':
    db_auto_update_from_csv_basic()
    db_auto_update_from_csv_daily()
    db_auto_update_from_csv_span('raw')
    db_auto_update_from_csv_span('adj')
    pass
