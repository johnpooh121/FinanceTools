import pymysql.cursors
import datetime as dt
connection = pymysql.connect(host='localhost',
                             port=3307,
                             user='root',
                             password='root0869',
                             database='stock',
                             cursorclass=pymysql.cursors.DictCursor)

def table_name(type,**kwargs):
    if type == 'daily':
        date = kwargs['date']
        return 'daily_'+date.strftime('%y%m%d')
    if type == 'span':
        adj = kwargs['adj'];target=kwargs['target']
        return ('adj_' if adj else 'raw_')+target
    if type == 'basic':
        date = kwargs['date']
        return 'basic_'+date.strftime('%y%m%d')