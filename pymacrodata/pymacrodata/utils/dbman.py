"""数据库操作辅助模块"""


def query_sql(cursor, sql, vals=None, one=False):
    cursor.execute(sql, vals)
    if one:
        result = cursor.fetchone()
    else:
        result = cursor.fetchall()
    return result


def insert_item(cursor, table, item, *ignored):
    for field in ignored:
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


def update_item(cursor, table, item, *keys, **ignore_fields):
    for field in ignore_fields:
        if field in item:
            item.pop(field)

    sql = 'update {} set {} where {}'
    key_names, key_vals = [], []
    for key in keys:
        key_names.append(key)
        key_vals.append(item.pop(key))
    key_names = '=%s and '.join(key_names) + '=%s'

    fields, vals = [], []
    for field, value in item.items():
        fields.append(field)
        vals.append(value)

    fields = '=%s,'.join(fields) + '=%s'

    vals.extend(key_vals)
    cursor.execute(sql.format(table, fields, key_names), vals)


def delete_item(cursor, table, item, *keys):
    key_names, key_vals = [], []
    for key in keys:
        key_names.append(key)
        key_vals.append(item[key])
    key_names = '=%s and '.join(key_names) + '=%s'

    sql = 'delete from {} where {}'

    cursor.execute(sql.format(table, key_names), key_vals)
