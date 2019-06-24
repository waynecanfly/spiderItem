# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class GbrItem(scrapy.Item):
    file_name = scrapy.Field()
    file_path = scrapy.Field()
    country_code = scrapy.Field()
    report_id = scrapy.Field()
    announcement_type = scrapy.Field()
    exchange_market_code = scrapy.Field()
    company_code = scrapy.Field()
    fiscal_year = scrapy.Field()
    financial_statement_type_code = scrapy.Field()
    financial_statement_season_type_code = scrapy.Field()
    financial_reporting_standard_code = scrapy.Field()
    disclosure_date = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    language_written_code = scrapy.Field()
    doc_type = scrapy.Field()
    doc_source_url = scrapy.Field()
    is_doc_url_direct = scrapy.Field()
    doc_local_path = scrapy.Field()
    doc_downloaded_timestamp = scrapy.Field()
    is_downloaded = scrapy.Field()
    currency_code = scrapy.Field()
    is_consolidated = scrapy.Field()
    version = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    detail_type = scrapy.Field()
    pdf_name = scrapy.Field()


class GbrItem2(scrapy.Item):
    file_name = scrapy.Field()
    file_path = scrapy.Field()
    report_id = scrapy.Field()
    doc_type = scrapy.Field()
    doc_source_url = scrapy.Field()
    doc_downloaded_timestamp = scrapy.Field()
    is_downloaded = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()
