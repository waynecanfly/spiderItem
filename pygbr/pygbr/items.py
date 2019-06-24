# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CompanyItem(scrapy.Item):
    code = scrapy.Field()
    name_origin = scrapy.Field()
    security_code = scrapy.Field()
    country_code_listed = scrapy.Field()
    country_code_origin = scrapy.Field()
    exchange_market_code = scrapy.Field()
    isin = scrapy.Field()
    currency_code = scrapy.Field()
    website_url = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()


class CompanyDataSourceItem(scrapy.Item):
    company_id = scrapy.Field()
    company_name = scrapy.Field()
    security_code = scrapy.Field()
    download_link = scrapy.Field()
    latest_url = scrapy.Field()
    latest_date = scrapy.Field()
    is_batch = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()


class ProfileDefinitionItem(scrapy.Item):
    name = scrapy.Field()
    display_label = scrapy.Field()
    data_type = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()


class ProfileDetailItem(scrapy.Item):
    label = scrapy.Field()
    parent_label = scrapy.Field()
    parent_id = scrapy.Field()
    company_profile_definition_id = scrapy.Field()
    company_code = scrapy.Field()
    value = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()


class AnnounceItem(scrapy.Item):
    report_id = scrapy.Field()
    country_code = scrapy.Field()
    exchange_market_code = scrapy.Field()
    company_code = scrapy.Field()
    fiscal_year = scrapy.Field()
    financial_statement_season_type_code = scrapy.Field()
    disclosure_date = scrapy.Field()
    doc_type = scrapy.Field()
    doc_source_url = scrapy.Field()
    is_doc_url_direct = scrapy.Field()
    doc_local_path = scrapy.Field()
    doc_downloaded_timestamp = scrapy.Field()
    is_downloaded = scrapy.Field()
    file_original_title = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()

    file_urls = scrapy.Field()
    files = scrapy.Field()
