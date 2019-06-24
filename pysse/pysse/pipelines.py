# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import random
from datetime import datetime

import pymysql
import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline

from .items import AnnounItem
from .utils.dbman import query_sql, insert_item, update_item
from .settings import FILES_STORE, DOC_PATH


class DefaultValuePipeline(object):

    def process_item(self, item, spider):
        if isinstance(item, AnnounItem):
            item.setdefault('country_code', 'CHN')
            item.setdefault('exchange_market_code', 'SSE')
            item.setdefault('is_doc_url_direct', True)

        return item


class CompanyCodePipeline(object):

    sql = 'select code from company where name_origin=%s'

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if spider.name == 'update_bond_list':
            with self.conn.cursor() as cursor:
                res = query_sql(cursor, self.sql, item['issuer'], True)
            if res:
                item['company_code'] = res['code']

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()


class MySQLPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):

        cursor = self.conn.cursor()
        if spider.name == 'update_bond_list':
            insert_item(cursor, 'bond_of_china', item)
        elif spider.name.endswith(('daily', 'monthly')):
            update_item(cursor, 'bond_of_china', item, 'bond_code')
        elif spider.name == 'update_bond_docs':
            insert_item(cursor, 'securities_statement_index', item)
        cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()


class AnnounRenamePipeline(object):

    sqls = {
        'query_bond': """\
            select company_code, short_name from bond_of_china where \
            bond_code=%s\
        """,
        'query_announ_type': """\
            select id, name from non_financial_announcement_detail_type where \
            country_code=%s and user_create=%s\
        """,
        'query_announ': """\
            select id from securities_statement_index where bond_code=%s and \
            doc_source_url=%s and file_original_title=%s\
        """
    }

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if isinstance(item, AnnounItem):  # 先判断是否已下载该文件
            if item['detail_type'] in self.announs:
                item['detail_type'] = self.announs[item['detail_type']]

            cursor = self.conn.cursor()
            if self.exist_db(cursor, item):
                raise DropItem('Item already existed.')
            else:
                bond = query_sql(
                    cursor, self.sqls['query_bond'], (item['bond_code'],),
                    True
                )
                if bond:
                    item['bond_short_name'] = bond['short_name']
                item['report_id'] = item['bond_code'] + self.gen_id()
            cursor.close()

        return item

    def exist_db(self, cursor, item):
        "按最近下载时间更新应判断当天数据是否已下载"
        vals = (
            item['bond_code'], item['doc_source_url'],
            item['file_original_title']
        )
        result = query_sql(cursor, self.sqls['query_announ'], vals, True)
        return result

    def gen_id(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            records = query_sql(
                cursor, self.sqls['query_announ_type'], ('CHN', 'lq')
            )
            self.announs = {it['name']: it['id'] for it in records}

    def close_spider(self, spider):
        self.conn.close()


class AnnounDownloaderPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        if isinstance(item, AnnounItem):
            for file_url in item['file_urls']:
                yield scrapy.Request(file_url)

    def item_completed(self, results, item, info):
        if isinstance(item, AnnounItem):
            for success, res in results:
                if success:
                    self.rename_file(res, item)
                else:
                    item['is_downloaded'] = False
                    item['doc_downloaded_timestamp'] = None
                    item['doc_local_path'] = None

        return item

    def rename_file(self, result, item):
        file_path = os.path.join(
            FILES_STORE, os.sep.join(result['path'].split('/'))
        )

        file_year = item['disclosure_date'].split('-')[0]
        file_dir = os.path.join(FILES_STORE, file_year)
        if not os.path.exists(file_dir):
            os.mkdir(file_dir)
        filename = item['report_id'] + '.' + item['doc_type']
        doc_path = os.path.join(file_dir, filename)
        try:  # 处理重复url下载的覆盖问题
            os.rename(file_path, doc_path)
            item['is_downloaded'] = True
            item['doc_downloaded_timestamp'] = datetime.now()
            item['doc_local_path'] = DOC_PATH.format(file_year, filename)
        except FileNotFoundError:
            item['is_downloaded'] = False
            item['doc_downloaded_timestamp'] = None
            item['doc_local_path'] = None

    def close_spider(self, spider):
        try:
            os.rmdir(os.path.join(FILES_STORE, 'full'))
        except OSError:
            pass
