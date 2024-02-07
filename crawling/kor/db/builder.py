import pymysql.cursors
import pandas as pd
from sqlalchemy.dialects.mysql import insert

from crawling.kor.db.glob import *
from crawling.kor.util import *

def make_table_span(adj=False):
    tname = table_name('span',adj=adj)
    with connection.cursor() as cursor:
        sql = (f"create table if not exists {tname}(\
            종목코드 char(6) NOT NULL,\
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
            PRIMARY KEY (종목코드,일자))")
        cursor.execute(sql)
        connection.commit()

def make_table_daily():
    tname = table_name('daily')
    with connection.cursor() as cursor:
        sql = (f"create table if not exists {tname}(\
            일자 date NOT NULL,\
            종목코드 char(6) NOT NULL,\
            종목명 char(200) NOT NULL,\
            시장구분 char(200) NOT NULL,\
            소속부 char(200),\
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
            PRIMARY KEY (종목코드,일자))")
        cursor.execute(sql)
        connection.commit()

def make_table_basic():
    tname = table_name('basic')
    with connection.cursor() as cursor:
        sql = (f"""create table if not exists {tname}(
            일자 date not null,
            표준코드 char(200) not null,
            단축코드 char(6) not null,
            `한글 종목명` char(200) not null,
            `한글 종목약명` char(200) not null,
            `영문 종목명` char(200) not null,
            상장일 date not null,
            시장구분 char(200) not null,
            증권구분 char(200) not null,
            소속부 char(200),
            주식종류 char(200) not null,
            액면가 char(10) not null,
            상장주식수 bigint not null,
            PRIMARY KEY (단축코드,일자))""")
        cursor.execute(sql)
        connection.commit()

def insert_on_conflict_update(table, conn, keys, data_iter):
    # update columns "b" and "c" on primary key conflict
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
    )
    stmt = stmt.on_duplicate_key_update(b=stmt.inserted.b, c=stmt.inserted.c)
    result = conn.execute(stmt)
    return result.rowcount

def insert_on_conflict_nothing(table, conn, keys, data_iter):
    # "a" is the primary key in "conflict_table"
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["a"])
    result = conn.execute(stmt)
    return result.rowcount



def load_csv(type,adj=False,date=nearest_updateable_date_before_now(),target = '005930'):
    tname = table_name(type,adj=adj)
    path = file_path(type,adj,target=target,date=date)
    df = pd.read_csv(path,encoding='euc-kr',dtype='str')

    if type=='span':
        df.insert(0,"종목코드",[target]*len(df))
    else:
        df.insert(0,'일자',[date.strftime("%Y/%m/%d")]*len(df))
    df.to_sql('tmptable', engine, if_exists ='replace',index=False)
    sql = f""" insert ignore into {tname} select * from tmptable
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        connection.commit()
    # df.to_sql(tname,con=engine,if_exists='append',index=False,method = insert_on_conflict_nothing)

def add_row(adj, target, row):
    tname = ('raw_' if not adj else 'adj_') + 'span'
    with connection.cursor() as cursor:
        sql = f"""insert ignore into {tname}
             values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql, [target]+row.to_list())
        connection.commit()
    pass

def drop_target(adj,target):
    tname = table_name('span',adj=adj)
    with connection.cursor() as cursor:
        sql = f"""delete from {tname}
             where `종목코드` = %s
        """
        cursor.execute(sql,target)
        connection.commit()
    pass

def make_every_table():
    make_table_span(adj=False)
    make_table_span(adj=True)
    make_table_daily()
    make_table_basic()

if __name__ == '__main__':
    # make_table_span('raw', '005930')
    # row = pd.Series(['2024/01/27', '73400', '-700', '-0.94', '73700', '74500', '73300', '11160062', '824499022832',
    #                  '438182039170000', '5969782550'],
    #                 index=['일자', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수'])
    # row=pd.concat([pd.Series({'종목코드':'005930'}),row])
    # print(row.values)
    # add_row(False, '005930', row)
    # make_table_daily(dt.datetime(2024,1,26))
    # make_table_basic()
    # make_table_span(adj=False)
    # load_csv('span',target='005930',adj=False)
    # drop_target(adj=False,target='005930')
    load_csv('span',target='005440',adj=False)
    pass

