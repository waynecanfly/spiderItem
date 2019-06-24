# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BondItem(scrapy.Item):
    company_code = scrapy.Field()
    bond_code = scrapy.Field()
    short_name = scrapy.Field()
    issuer = scrapy.Field()
    bond_full_name_origin = scrapy.Field()
    bond_type = scrapy.Field()
    actual_circulation = scrapy.Field()
    total_value = scrapy.Field()
    issue_start_date = scrapy.Field()
    issue_end_date = scrapy.Field()
    start_interest_date = scrapy.Field()
    end_date = scrapy.Field()
    ipo_date = scrapy.Field()
    bank_code = scrapy.Field()
    interest_rate_type = scrapy.Field()
    interest_payment_method = scrapy.Field()
    coupon_rate = scrapy.Field()
    repayment_period = scrapy.Field()
    base_rate = scrapy.Field()
    float_interest_rate = scrapy.Field()
    rate = scrapy.Field()
    clean_price = scrapy.Field()
    manage_type = scrapy.Field()
    issuance_method = scrapy.Field()
    bond_rating = scrapy.Field()
    main_rating = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()


class AnnounItem(scrapy.Item):
    report_id = scrapy.Field()
    country_code = scrapy.Field()
    exchange_market_code = scrapy.Field()
    company_code = scrapy.Field()
    bond_code = scrapy.Field()
    bond_short_name = scrapy.Field()
    disclosure_date = scrapy.Field()
    doc_type = scrapy.Field()
    doc_source_url = scrapy.Field()
    is_doc_url_direct = scrapy.Field()
    doc_local_path = scrapy.Field()
    doc_downloaded_timestamp = scrapy.Field()
    is_downloaded = scrapy.Field()
    file_original_title = scrapy.Field()
    detail_type = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()

    file_urls = scrapy.Field()
    files = scrapy.Field()
