import xlrd
import pymysql
import time
import os
import logging


logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
formatter = logging.Formatter('%(lineno)d: %(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.ERROR)
handler.setFormatter(formatter)
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
console.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(console)


num = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
first = "select company_code from company_profile_detail where company_profile_definition_id = '2861'"
cursor.execute(first)
company_id_list = [i[0] for i in cursor.fetchall()]
dir_list = os.listdir("/data/OPDCMS/HCK/company_list")
for temp in dir_list:
    args = {}
    book = xlrd.open_workbook("/data/OPDCMS/HCK/company_list/" + temp)
    sheet = book.sheet_by_index(0)
    for r in range(3, sheet.nrows):
        code = sheet.cell(r, 0).value
        args["Category_HCK"] = sheet.cell(r, 2).value
        args["Sub_Category_HCK"] = sheet.cell(r, 3).value
        args["Board_Lot_HCK"] = sheet.cell(r, 4).value
        args["Expiry_Date_HCK"] = sheet.cell(r, 7).value
        args["Subject_to_Stamp_Duty_HCK"] = sheet.cell(r, 8).value
        args["Shortsell_Eligible_HCK"] = sheet.cell(r, 9).value
        args["CAS_Eligible_HCK"] = sheet.cell(r, 10).value
        args["VCM_Eligible_HCK"] = sheet.cell(r, 11).value
        args["Admitted_to_Stock_Options_HCK"] = sheet.cell(r, 12).value
        args["Admitted_to_Stock_Futures_HCK"] = sheet.cell(r, 13).value
        args["Admitted_to_CCASS_HCK"] = sheet.cell(r, 14).value
        args["ETF_Fund_Manager_HCK"] = sheet.cell(r, 15).value
        args["Debt_Securities_Board_Lot_HCK"] = sheet.cell(r, 16).value
        args["Debt_Securities_Investor_Type_HCK"] = sheet.cell(r, 17).value
        gmt_create = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        user_create = "zx"
        try:
            sql = "select company_id from company_data_source where security_code=%s"
            cursor.execute(sql, code)
            r = cursor.fetchone()
            print("智能忽略")
            if r[0] not in company_id_list:
                for each in args:
                    if each == "Category_HCK":
                        id = "2861"
                    elif each == "Sub_Category_HCK":
                        id = "2862"
                    elif each == "Board_Lot_HCK":
                        id = "2863"
                    elif each == "Expiry_Date_HCK":
                        id = "2864"
                    elif each == "Subject_to_Stamp_Duty_HCK":
                        id = "2865"
                    elif each == "Shortsell_Eligible_HCK":
                        id = "2866"
                    elif each == "CAS_Eligible_HCK":
                        id = "2867"
                    elif each == "VCM_Eligible_HCK":
                        id = "2868"
                    elif each == "Admitted_to_Stock_Options_HCK":
                        id = "2869"
                    elif each == "Admitted_to_Stock_Futures_HCK":
                        id = "2870"
                    elif each == "Admitted_to_CCASS_HCK":
                        id = "2871"
                    elif each == "ETF_Fund_Manager_HCK":
                        id = "2872"
                    elif each == "Debt_Securities_Board_Lot_HCK":
                        id = "2873"
                    else:
                        id = "2874"
                    jud = "select id from company_profile_detail where company_code=%s and company_profile_definition_id=%s"
                    cursor.execute(jud, [r[0], id])
                    s = cursor.fetchall()
                    # print(code, id, each, args[each], len(s))
                    if len(s) == 0:
                        sql_detail_insert = "insert into company_profile_detail(company_code,company_profile_definition_id,value,gmt_create,user_create)value(%s,%s,%s,%s,%s)"
                        params2 = [
                            r[0],
                            id,
                            args[each],
                            gmt_create,
                            user_create
                        ]
                        cursor.execute(sql_detail_insert, params2)
                        conn.commit()
                        num += 1
                        print("新插入第%s个" %num)
        except Exception as e:
            logger.error(e)