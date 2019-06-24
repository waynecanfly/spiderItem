# -*- coding: utf-8 -*-
import pymysql
import time
import xlrd
import os


num = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()

book = xlrd.open_workbook("./pj.xlsx")
sheet = book.sheet_by_index(0)
for r in range(1, sheet.nrows):
    code = sheet.cell(r, 0).value
    pj = sheet.cell(r, 9).value
    sql = "update bond_of_china set bond_rating=%s where bond_code=%s"
    cursor.execute(sql, [pj, code])
    conn.commit()
    num += 1
    print(num)
