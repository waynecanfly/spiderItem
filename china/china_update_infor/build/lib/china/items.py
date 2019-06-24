# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import scrapy


class ChinaIntroItem(scrapy.Item):
    file_name = scrapy.Field()
    file_path = scrapy.Field()
    doc_source_url = scrapy.Field()
    company_name = scrapy.Field()
    company_code = scrapy.Field()

    #company item
    code = scrapy.Field()
    name_origin = scrapy.Field()
    name_en = scrapy.Field()
    security_code = scrapy.Field()
    country_code_listed = scrapy.Field()
    country_code_origin = scrapy.Field()
    exchange_market_code = scrapy.Field()
    ipo_date = scrapy.Field()
    currency_code = scrapy.Field()
    status = scrapy.Field()
    website_url = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()

    #detail item_value_shanghai
    company_short_name_zh = scrapy.Field()
    company_short_name_en = scrapy.Field()
    registered_address = scrapy.Field()
    mailing_address = scrapy.Field()
    legal_representative = scrapy.Field()
    secretary_name = scrapy.Field()
    e_mail = scrapy.Field()
    phone_number = scrapy.Field()
    CSRC_industry = scrapy.Field()
    SSE_industry = scrapy.Field()
    district_belong_to = scrapy.Field()
    is_SSE_180_sample_stock = scrapy.Field()
    is_overseas_listing = scrapy.Field()
    overseas_listing_land = scrapy.Field()
    convertible_bonds_for_short = scrapy.Field()
    # detail item_title_shanghai
    company_short_name_zh_title = scrapy.Field()
    company_short_name_en_title = scrapy.Field()
    registered_address_title = scrapy.Field()
    mailing_address_title = scrapy.Field()
    legal_representative_title = scrapy.Field()
    secretary_name_title = scrapy.Field()
    e_mail_title = scrapy.Field()
    phone_number_title = scrapy.Field()
    CSRC_industry_title = scrapy.Field()
    SSE_industry_title = scrapy.Field()
    district_belong_to_title = scrapy.Field()
    is_SSE_180_sample_stock_title = scrapy.Field()
    is_overseas_listing_title = scrapy.Field()
    overseas_listing_land_title = scrapy.Field()
    convertible_bonds_for_short_title = scrapy.Field()

    #detail item title shenzhen
    industry_title = scrapy.Field()
    Total_share_capital_of_A_shares_title = scrapy.Field()
    A_shares_circulating_capital_title = scrapy.Field()
    #registered_address_title = scrapy.Field()
    #company_short_name_zh_title = scrapy.Field()
    #district_belong_to_title = scrapy.Field()

    #detail item value shenzhen
    industry = scrapy.Field()
    Total_share_capital_of_A_shares = scrapy.Field()
    A_shares_circulating_capital = scrapy.Field()
    #registered_address = scrapy.Field()
    #company_short_name_zh = scrapy.Field()
    #district_belong_to = scrapy.Field()