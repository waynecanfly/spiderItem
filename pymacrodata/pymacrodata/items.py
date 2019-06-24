# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CountryItem(scrapy.Item):
    code = scrapy.Field()
    name = scrapy.Field()
    url = scrapy.Field()
    profiles = scrapy.Field()
    profile_file_path = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()


class RegionItem(CountryItem):
    country_id = scrapy.Field()


class IndicatorItem(scrapy.Item):
    name = scrapy.Field()
    parent_id = scrapy.Field()  # 用于二级指标
    level = scrapy.Field()  # 指标层级
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()


class IndicatorDataItem(scrapy.Item):
    country_code = scrapy.Field()
    country_id = scrapy.Field()
    region_id = scrapy.Field()
    indicator_id = scrapy.Field()
    url = scrapy.Field()
    data = scrapy.Field()
    data_file_path = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    gmt_update = scrapy.Field()
    user_update = scrapy.Field()
