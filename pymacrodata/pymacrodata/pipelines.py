# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import csv
import json

import pymysql
from scrapy.exceptions import DropItem

from .items import CountryItem, RegionItem, IndicatorItem, IndicatorDataItem
from .utils.dbman import insert_item, query_sql


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        item.setdefault('user_create', 'lq')

        return item


class IndicatorPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if isinstance(item, IndicatorDataItem):
            for it in self.indicators:
                if (it['level'] == item['indicator_id'][0] and
                    it['name'] == item['indicator_id'][1].lower() and
                        it['parent_name'] == item['indicator_id'][2].lower()):
                    item['indicator_id'] = it['id']
                    break
            else:
                spider.logger.error(
                    '%s can\'t find it id.', item['indicator_id'])

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            self.indicators = query_sql(cursor, """\
                select lower(ta.name) parent_name, lower(tb.name) name, tb.id,\
                tb.level from macro_data_indicator ta right join \
                macro_data_indicator tb on ta.id = tb.parent_id\
                """)

    def close_spider(self, spider):
        self.conn.close()


class DumpDataPipeline(object):
    """转储数据至文件"""

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['FILES_STORE'])

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def process_item(self, item, spider):
        if isinstance(item, CountryItem):
            filename = item['code'] + '.json'
            if isinstance(item, RegionItem):
                file_dir = os.path.join(self.base_dir, item['country_id'])
            else:
                file_dir = os.path.join(self.base_dir, item['code'])
            if not os.path.exists(file_dir):
                os.mkdir(file_dir)
            filepath = os.path.join(file_dir, filename)
            item['profile_file_path'] = filepath
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(item['profiles'], f)
        elif isinstance(item, IndicatorDataItem):
            filename = str(item['indicator_id']) + '.csv'
            file_dir = os.path.join(self.base_dir, item['country_code'])
            filepath = os.path.join(file_dir, filename)
            item['data_file_path'] = filepath
            with open(filepath, 'w', encoding='utf-8') as f:
                fcsv = csv.writer(f)
                fcsv.writerows(item['data'])

        return item


class MySQLPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs
        self.indicators = []

    def process_item(self, item, spider):
        with self.conn.cursor() as cursor:
            if isinstance(item, RegionItem):
                result = query_sql(
                    cursor, 'select id from macro_data_country where code=%s',
                    (item['country_id'],), one=True)
                item['country_id'] = result['id']
                insert_item(
                    cursor, 'macro_data_region', item, 'profiles', 'code')
            elif isinstance(item, CountryItem):
                insert_item(cursor, 'macro_data_country', item, 'profiles')
            elif isinstance(item, IndicatorItem):
                self.do_indicator(cursor, item)
            elif isinstance(item, IndicatorDataItem):
                insert_item(
                    cursor, 'macro_data_indicator_data_index', item, 'data',
                    'country_code')

        return item

    def do_indicator(self, cursor, item):
        if (
            item['name'].lower(), item['level'], str(item['parent_id']).lower()
        ) in self.indicators:
            raise DropItem('indicator already existed.')
        else:
            self.indicators.append((
                item['name'].lower(), item['level'],
                str(item['parent_id']).lower()
            ))
            if item['level'] == 1:
                insert_item(cursor, 'macro_data_indicator', item)
            else:
                result = query_sql(
                    cursor, """select id from macro_data_indicator where \
                    name=%s and level=%s""",
                    (item['parent_id'], item['level'] - 1), one=True)
                item['parent_id'] = result['id']
                insert_item(cursor, 'macro_data_indicator', item)

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
