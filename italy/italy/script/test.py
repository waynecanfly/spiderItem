# -*- coding: utf-8 -*-
import pymysql
import time


conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()

data = "Acquisitions/Disposals@Adherence to Codes of Conduct@Amendments to the instrument of incorporation@Annual Financial Report@Half Yearly Financial Report@Information Published in the previous 12 months@Interim Management Reports@Issue of Bonds@Lists of Candidates for Appointment of Members of Company Bodies@Measures pursuant to Article 2446 of the Italian Civil Code@Mergers/Spin-offs@Other Disclosure@Purchase and Sale of Treasury Shares@Share Capital Increases@Stock Option Plans@Transactions with related parties"
data_list = data.split("@")
for temp in data_list:
    params = [
        temp,
        "ITA",
        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
        "zx"
    ]
    sql = "insert into non_financial_announcement_detail_type(name,country_code,gmt_create,user_create)value(%s,%s,%s,%s)"
    cursor.execute(sql, params)
    conn.commit()