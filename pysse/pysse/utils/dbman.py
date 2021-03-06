"""数据库操作辅助模块"""

from datetime import datetime


def query_sql(cursor, sql, vals, one=False):
    cursor.execute(sql, vals)
    if one:
        result = cursor.fetchone()
    else:
        result = cursor.fetchall()
    return result


def insert_item(cursor, table, item):
    item['gmt_create'] = datetime.now()
    item['user_create'] = 'lq'

    for field in ['file_urls', 'files']:
        if field in item:
            item.pop(field)

    sql = 'insert into {} {} values {}'
    fields, vals = [], []
    for field, value in item.items():
        fields.append(field)
        vals.append(value)
    placeholders = '(' + ','.join(['%s'] * len(fields)) + ')'
    fields = '(' + ','.join(fields) + ')'

    cursor.execute(sql.format(table, fields, placeholders), vals)


def update_item(cursor, table, item, key):
    sql = 'update {} set {} where {}=%s'
    key_val = item.pop(key)
    fields, vals = [], []
    for field, value in item.items():
        fields.append(field)
        vals.append(value)

    fields = '=%s,'.join(fields) + '=%s'
    vals.append(key_val)

    cursor.execute(sql.format(table, fields, key), vals)
