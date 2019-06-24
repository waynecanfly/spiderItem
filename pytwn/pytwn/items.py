# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BulletinItem(scrapy.Item):
    report_id = scrapy.Field()
    country_code = scrapy.Field()
    exchange_market_code = scrapy.Field()
    company_code = scrapy.Field()
    fiscal_year = scrapy.Field()
    disclosure_date = scrapy.Field()
    language_written_code = scrapy.Field()
    doc_type = scrapy.Field()
    doc_source_url = scrapy.Field()
    is_doc_url_direct = scrapy.Field()
    doc_local_path = scrapy.Field()
    doc_downloaded_timestamp = scrapy.Field()
    is_downloaded = scrapy.Field()
    file_original_title = scrapy.Field()
    announcement_detail_type = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()

    data = scrapy.Field()
    file_urls = scrapy.Field()


class ReportItem(BulletinItem):
    financial_statement_season_type_code = scrapy.Field()
    announcement_type = scrapy.Field()
