# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class IndiaItem(scrapy.Item):
    file_name = scrapy.Field()
    file_path = scrapy.Field()
    doc_source_url = scrapy.Field()
    source_url = scrapy.Field()
    jud = scrapy.Field()

    disclosure_date = scrapy.Field()
    fiscal_year = scrapy.Field()
    company_code = scrapy.Field()
    country_code = scrapy.Field()
    exchange_market_code = scrapy.Field()
    financial_reporting_standard_code = scrapy.Field()
    doc_type = scrapy.Field()
    is_doc_url_direct = scrapy.Field()
    is_downloaded = scrapy.Field()
    currency_code = scrapy.Field()
    doc_downloaded_timestamp = scrapy.Field()
    language_written_code = scrapy.Field()
    report_id = scrapy.Field()
    doc_local_path = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    financial_statement_season_type_code = scrapy.Field()
    financial_statement_type_code = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    company_id = scrapy.Field()
    name_origin = scrapy.Field()
    name_en = scrapy.Field()
    security_code = scrapy.Field()
    country_code_listed = scrapy.Field()
    ipo_date = scrapy.Field()
    status = scrapy.Field()
    website_url = scrapy.Field()
    info_disclosure_id = scrapy.Field()
    ISIN = scrapy.Field()
    management_list = scrapy.Field()
    CIN = scrapy.Field()
    Impact_Cost = scrapy.Field()
    BC_RD = scrapy.Field()
    Market_Lot = scrapy.Field()
    Listing_Date = scrapy.Field()
    Registered_Office_Tel = scrapy.Field()
    Registered_Office_Fax = scrapy.Field()
    Registered_Office_Email = scrapy.Field()
    Registered_Office_Website = scrapy.Field()
    Registrars_Tel = scrapy.Field()
    Registrars_Fax = scrapy.Field()
    Registrars_Email = scrapy.Field()
    Registrars_Website = scrapy.Field()
    Code = scrapy.Field()
    Symbol = scrapy.Field()
    Name = scrapy.Field()
    Group_Num = scrapy.Field()
    Face_Value = scrapy.Field()
    Industry = scrapy.Field()
    Instrument = scrapy.Field()

    Company_Name = scrapy.Field()
    Manage_Name = scrapy.Field()
    Designation = scrapy.Field()

    First_Listing_Date = scrapy.Field()
    Paid_Up_Value = scrapy.Field()

    Date_of_Listing = scrapy.Field()
    Issued_Cap = scrapy.Field()
    Free_Float_Market_Cap = scrapy.Field()
    FiftyTwo_week_high_or_low_price = scrapy.Field()
    Ex_Date = scrapy.Field()
    Purpose = scrapy.Field()

    Type = scrapy.Field()
    Date_Begin = scrapy.Field()
    Date_End = scrapy.Field()
    Description = scrapy.Field()
    Net_Sales = scrapy.Field()
    Other_Operating_Income = scrapy.Field()
    Sales_Income_from_Operations = scrapy.Field()
    Other_Income = scrapy.Field()
    Total_Income = scrapy.Field()
    Expenditure = scrapy.Field()
    Changes_in_inventories_of_finished_goods_work_in_progress_and_stock_in_trade = scrapy.Field()
    Depreciation_and_amortisation_expense = scrapy.Field()
    Employee_benefit_expense = scrapy.Field()
    Purchases_of_stock_in_trade = scrapy.Field()
    Excise_Duty = scrapy.Field()
    Cost_of_Materials_Consumed = scrapy.Field()
    Finance_Costs = scrapy.Field()
    Other_Expenses = scrapy.Field()
    Sub_contracting_charges = scrapy.Field()
    Profit_after_Interest_but_before_Exceptional_Items = scrapy.Field()
    Profit_Loss_from_Ordinary_Activities_before_Tax = scrapy.Field()
    Tax = scrapy.Field()
    Current_tax = scrapy.Field()
    Deferred_tax = scrapy.Field()
    Net_Profit_Loss_from_Ordinary_Activities_after_Tax = scrapy.Field()
    Net_Profit = scrapy.Field()
    Basic_for_discontinued_continuing_operation = scrapy.Field()
    Diluted_for_discontinued_continuing_operation = scrapy.Field()

    title = scrapy.Field()
    value = scrapy.Field()

    Action = scrapy.Field()
    WKN = scrapy.Field()
    pdf_name = scrapy.Field()
    announcement = scrapy.Field()
