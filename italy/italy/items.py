# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ItalyCompanyItem(scrapy.Item):
    country_code_listed = scrapy.Field()
    exchange_market_code = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()
    security_code = scrapy.Field()
    web = scrapy.Field()
    isin = scrapy.Field()
    Industry = scrapy.Field()
    sector = scrapy.Field()
    jud = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    Admission_to_Listing = scrapy.Field()


class ItalydetailItem(scrapy.Item):
    security_code = scrapy.Field()
    jud = scrapy.Field()
    adress = scrapy.Field()
    tel = scrapy.Field()
    fax = scrapy.Field()
    profile = scrapy.Field()
    Bloomberg = scrapy.Field()
    Reuters = scrapy.Field()
    Market = scrapy.Field()
    BOARD_MEMBERS = scrapy.Field()
    BOARD_MEMBERS_OWNERSHIP = scrapy.Field()
    TOP_MANAGEMENT = scrapy.Field()
    Compensation = scrapy.Field()
    List_based_voting_system = scrapy.Field()
    SHAREHOLDERS = scrapy.Field()
    Admission_to_Listing_Market = scrapy.Field()
    Nominal_Value = scrapy.Field()
    Borsa_Italiana = scrapy.Field()
    Admission_to_Listing = scrapy.Field()
    Public_Offer_Period = scrapy.Field()
    First_Trading_Day = scrapy.Field()
    Bookbuilding = scrapy.Field()
    Offering_Price = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()


class ItalyfileItem(scrapy.Item):
    file_name = scrapy.Field()
    file_path = scrapy.Field()
    doc_source_url = scrapy.Field()

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
    detail_type = scrapy.Field()
    announcement_type = scrapy.Field()