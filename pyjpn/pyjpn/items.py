# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CompanyItem(scrapy.Item):
    code = scrapy.Field()
    name_origin = scrapy.Field()
    name_en = scrapy.Field()
    security_code = scrapy.Field()
    industry = scrapy.Field()
    country_code_listed = scrapy.Field()
    exchange_market_code = scrapy.Field()
    info_disclosure_id = scrapy.Field()
    isin = scrapy.Field()
    status = scrapy.Field()
    website_url = scrapy.Field()
    remarks = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()


class CompanyDataSourceItem(scrapy.Item):
    company_id = scrapy.Field()
    company_name = scrapy.Field()
    security_code = scrapy.Field()
    info_disclosure_id = scrapy.Field()
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
    label_name = scrapy.Field()
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
    disclosure_date = scrapy.Field()
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

    file_urls = scrapy.Field()
    files = scrapy.Field()


class ReportItem(AnnounceItem):
    financial_statement_season_type_code = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    announcement_type = scrapy.Field()
