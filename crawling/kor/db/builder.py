import pymysql.cursors
import pandas as pd

from crawling.kor.db.glob import *
from crawling.kor.util import *

def make_table_span(type, target):
    if type == 'raw':
        path = base_path + "/data/raw_span/" + target + ".csv"
    else:
        path = base_path + "/data/adj_span/" + target + ".csv"
    tname = table_name('span',target=target,adj=(type=='adj'))
    if not os.path.exists(path):
        print("no such target exists\n")
        return
    with connection.cursor() as cursor:
        sql = f"""
            drop table if exists {tname}
        """
        cursor.execute(sql)
        connection.commit()
        sql = (f"create table if not exists {tname}(\
            일자 date NOT NULL,\
            종가 int NOT NULL,\
            대비 int NOT NULL,\
            등락률 float NOT NULL,\
            시가 int NOT NULL,\
            고가 int NOT NULL,\
            저가 int NOT NULL,\
            거래량 bigint NOT NULL,\
            거래대금 bigint NOT NULL,\
            시가총액 bigint NOT NULL,\
            상장주식수 bigint NOT NULL,\
            PRIMARY KEY (일자))")
        cursor.execute(sql)
        connection.commit()
        load_csv(tname,path)

def make_table_daily(date):
    path = base_path + "/data/daily/" + file_name('daily',date)
    tname = table_name('daily',date=date)
    if not os.path.exists(path):
        print("no such target exists\n")
        return
    with connection.cursor() as cursor:
        sql = f"""
            drop table if exists {tname}
        """
        cursor.execute(sql)
        connection.commit()
        sql = (f"create table if not exists {tname}(\
            종목코드 char(6) NOT NULL primary key,\
            종목명 char(200) NOT NULL,\
            시장구분 char(200) NOT NULL,\
            소속부 char(200) NOT NULL,\
            종가 int NOT NULL,\
            대비 int NOT NULL,\
            등락률 float NOT NULL,\
            시가 int NOT NULL,\
            고가 int NOT NULL,\
            저가 int NOT NULL,\
            거래량 bigint NOT NULL,\
            거래대금 bigint NOT NULL,\
            시가총액 bigint NOT NULL,\
            상장주식수 bigint NOT NULL\
            )")
        cursor.execute(sql)
        connection.commit()
        load_csv(tname,path)

def make_table_basic(date):
    path = base_path + "/data/basic/" + file_name('basic',date)
    tname = table_name('basic',date=date)
    if not os.path.exists(path):
        print("no such target exists\n")
        return
    with connection.cursor() as cursor:
        sql = f"""
            drop table if exists {tname}
        """
        cursor.execute(sql)
        connection.commit()
        sql = (f"""create table if not exists {tname}(
            `표준코드` char(200) not null unique,
            `단축코드` char(6) not null primary key,
            `한글 종목명` char(200) not null,
            `한글 종목약명` char(200) not null,
            `영문 종목명` char(200) not null,
            상장일 date not null,
            시장구분 char(200) not null,
            증권구분 char(200) not null,
            소속부 char(200),
            주식종류 char(200) not null,
            액면가 char(10) not null,
            상장주식수 bigint not null
            )""")
        cursor.execute(sql)
        connection.commit()
        load_csv(tname,path)
def load_csv(tname,path):
    with connection.cursor() as cursor:
        sql = (f"""
                    load data infile "{path}"
                    into table {tname}
                    fields terminated by ','
                    enclosed by '"'
                    lines terminated by '\n'
                    ignore 1 rows;
                       """)
        cursor.execute(sql)
        connection.commit()

def add_row(type, target, row):
    tname = ('raw_' if type == 'raw' else 'adj_') + target
    with connection.cursor() as cursor:
        sql = f"""insert into {tname}
             values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql, row.to_list())
        connection.commit()
    pass


if __name__ == '__main__':
    # make_table_span('raw', '005930')
    # row = pd.Series(['2024/01/27', '73400', '-700', '-0.94', '73700', '74500', '73300', '11160062', '824499022832',
    #                  '438182039170000', '5969782550'],
    #                 index=['일자', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수'])
    # print(row.values)
    # add_row('raw', '005930', row)
    # make_table_daily(dt.datetime(2024,1,26))
    make_table_basic(dt.datetime(2024,1,26))
    pass

