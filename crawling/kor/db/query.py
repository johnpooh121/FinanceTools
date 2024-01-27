from crawling.kor.db.glob import connection

def is_table_exist(table_name):
    with connection.cursor() as cursor:
        sql = f"""
            select count(*) from information_schema.TABLES
            where table_name = %s and table_schema = 'stock'
        """
        cursor.execute(sql,table_name)
        result = cursor.fetchone()
    return list(result.values())[0]==1

if __name__ == '__main__':
    print(is_table_exist('adj_005930'))
    pass
