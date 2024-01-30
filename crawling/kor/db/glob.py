import pymysql.cursors
import datetime as dt
from sqlalchemy import create_engine

schemaname = 'stock'
# schemaname = 'stock_practice'

connection = pymysql.connect(host='localhost',
                             port=3307,
                             user='root',
                             password='root1234',
                             database=schemaname,
                             cursorclass=pymysql.cursors.DictCursor)

engine = create_engine(f'mysql+pymysql://root:root1234@localhost:3307/{schemaname}')

def table_name(type,**kwargs):
    if type == 'daily':
        return 'daily'
    if type == 'span':
        adj = kwargs['adj'];
        return ('adj_' if adj else 'raw_')+'span'
    if type == 'basic':
        return 'basic'