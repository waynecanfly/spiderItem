import xlrd
import pymysql
import time
import re


name_list = ["Category_HCK", "Sub_Category_HCK", "Board_Lot_HCK", "Expiry_Date_HCK", "Subject_to_Stamp_Duty_HCK",
             "Shortsell_Eligible_HCK", "CAS_Eligible_HCK", "VCM_Eligible_HCK", "Admitted_to_Stock_Options_HCK",
             "Admitted_to_Stock_Futures_HCK", "Admitted_to_CCASS_HCK", "ETF_Fund_Manager_HCK",
             "Debt_Securities_Board_Lot_HCK", "Debt_Securities_Investor_Type_HCK"]
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
book = xlrd.open_workbook("/data/OPDCMS/HCK/company_list/ListOfSecurities.xlsx")
sheet = book.sheet_by_index(0)


def codefunc(num):
    if num < 10:
        num = "000" + str(num)
        code = "HCK1" + num
    elif num <= 99:
        num = "00" + str(num)
        code = "HCK1" + num
    elif num <= 999:
        num = "0" + str(num)
        code = "HCK1" + num
    elif num <= 9999:
        code = "HCK1" + str(num)
    else:
        code = "HCK" + str(num + 10000)
    return code


def insert_definition():
    num = 0
    for temp in name_list:
        num += 1
        print(num)
        name = temp
        display_label = temp.replace("_HCK", "")
        data_type = "string"
        sort = 0
        gmt_create = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        user_create = "zx"
        params = [
            name,
            display_label,
            data_type,
            sort,
            gmt_create,
            user_create
        ]
        sql_definition = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create,user_create)value(%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql_definition, params)
        conn.commit()


def insert_company():
    num = 0
    for r in range(3, sheet.nrows):
        num += 1
        print(num)
        code = codefunc(num)
        num = int(num)
        Stock_Code = sheet.cell(r, 0).value
        Name_of_Securities = sheet.cell(r, 1).value
        try:
            Par_Value = re.search("\w{3}", str(sheet.cell(r, 5).value)).group()
        except:
            Par_Value = ""
        ISIN = sheet.cell(r, 6).value
        gmt_create = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        user_create = "zx"
        params1 = [
            code,
            Stock_Code,
            Name_of_Securities,
            Par_Value,
            ISIN,
            gmt_create,
            user_create,
            "HCK",
            "HCKX"
        ]
        sql_company = "insert into company(code,security_code,name_origin,currency_code,isin,gmt_create,user_create,country_code_listed,exchange_market_code)value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql_company, params1)
        conn.commit()


def insert_detail():
    num = 0
    args = {}
    for r in range(3, sheet.nrows):
        num += 1
        print(num)
        code = codefunc(num)
        num = int(num)
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
        for each in args:
            sql_detail_select = "select id from company_profile_definition where name=%s"
            cursor.execute(sql_detail_select, each)
            result = cursor.fetchall()
            if len(result) != 0:
                sql_detail_insert = "insert into company_profile_detail(company_code,company_profile_definition_id,value,gmt_create,user_create)value(%s,%s,%s,%s,%s)"
                params2 = [
                    code,
                    result[0][0],
                    args[each],
                    gmt_create,
                    user_create
                ]
                cursor.execute(sql_detail_insert, params2)
                conn.commit()


def main():
    # insert_definition()
    # insert_company()
    insert_detail()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()