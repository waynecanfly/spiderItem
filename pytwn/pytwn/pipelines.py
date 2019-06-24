# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import random
from datetime import datetime

import scrapy
import pymysql
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from pymysql.err import OperationalError

from .items import BulletinItem, ReportItem
from .utils.dbman import insert_item, update_item
from .settings import DOC_PATH


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        if spider.name.startswith('ifrs'):
            if isinstance(item, BulletinItem):
                item.setdefault('country_code', 'TWN')
                item.setdefault('language_written_code', 'zh')
                if 'doc_type' not in item:
                    item.setdefault('doc_type', 'html')
                item.setdefault('is_doc_url_direct', False)
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')

            if isinstance(item, ReportItem):
                item.setdefault('announcement_type', True)
        else:
            item.setdefault('gmt_update', datetime.now())
            item.setdefault('user_update', 'lq')

        return item


class DocMatePipeline(object):
    """处理文档类型、日期、财报区间等信息，转储文档"""

    sqls = {
        'query_types': (
            "select id, name from financial_announcement_detail_type where "
            "country_code='TWN' union select id, name from "
            "non_financial_announcement_detail_type where country_code='TWN'"
        ),
        'create_type': (
            "insert into {} (name, country_code, gmt_create, user_create) "
            "values (%s, %s, %s, %s)"
        ),
        'query_one': 'select last_insert_id() as id',
        'query_old': (
            "select id from financial_statement_index where country_code='TWN'"
            " and doc_source_url=%s"
        )
    }

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler.settings['DBARGS'], crawler.settings['FILES_STORE'])

    def __init__(self, dbargs, files_store):
        self.dbargs = dbargs
        self.files_store = files_store

    def process_item(self, item, spider):
        if spider.name.startswith('ifrs'):
            item['report_id'] = item['company_code'] + self.gen_id()
            type_id = self.query_type(item)
            if isinstance(item, ReportItem):
                '''
                if self.exist_db(item):
                    raise DropItem('already downloaded.')
                '''
                item['financial_statement_season_type_code'] = type_id
            else:
                item['announcement_detail_type'] = type_id

            # 採IFRSs前民国纪年:
            if item['fiscal_year'] and isinstance(item['fiscal_year'], str):
                item['fiscal_year'] = int(item['fiscal_year']) + 1912 - 1

        if item['is_downloaded']:
            self.store_data(item)

        return item

    def gen_id(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def query_type(self, item):
        """查询公告、报表类型id"""
        title = item['announcement_detail_type']
        try:
            type_id = self.types[title]
        except KeyError:
            if isinstance(item, ReportItem):
                table = 'financial_announcement_detail_type'
            else:
                table = 'non_financial_announcement_detail_type'
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        self.sqls['create_type'].format(table),
                        (title, 'TWN', datetime.now(), 'lq')
                    )
                    cursor.execute(self.sqls['query_one'])
                    result = cursor.fetchone()
            except (BrokenPipeError, OperationalError):
                self.conn = pymysql.connect(**self.dbargs)
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        self.sqls['create_type'].format(table),
                        (title, 'TWN', datetime.now(), 'lq')
                    )
                    cursor.execute(self.sqls['query_one'])
                    result = cursor.fetchone()

            type_id = result['id']
            self.types[title] = result['id']

        return type_id

    def store_data(self, item):
        if item['fiscal_year']:
            year = str(item['fiscal_year'])
        else:
            year = 'newst'

        if item['doc_type'] == 'directory':
            filedir = os.path.join(self.files_store, year, item['report_id'])
            filename = item['report_id'] + '.html'
        else:
            filedir = os.path.join(self.files_store, year)
            filename = item['report_id'] + '.' + item['doc_type']
        if not os.path.exists(filedir):
            os.makedirs(filedir)

        with open(os.path.join(filedir, filename), 'wb') as f:
            f.write(item['data'])

        item['doc_downloaded_timestamp'] = datetime.now()
        if item['doc_type'] == 'directory':
            item['doc_local_path'] = DOC_PATH.format(
                item['fiscal_year'], item['report_id'])
        else:
            item['doc_local_path'] = DOC_PATH.format(
                item['fiscal_year'], filename)

    def exist_db(self, item):
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query_old'], (item['doc_source_url'],))
            if cursor.fetchone():
                return True
            else:
                return False

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query_types'])
            result = cursor.fetchall()
            self.types = {it['name']: it['id'] for it in result}

    def close_spider(self, spider):
        self.conn.close()


class DocDownloaderPipeline(FilesPipeline):
    """下载'財務報告更(補)正查詢作業'附件"""

    def get_media_requests(self, item, info):
        if isinstance(item, BulletinItem):
            if 'file_urls' in item:
                for file_url in item['file_urls']:
                    yield scrapy.Request(
                        file_url,
                        meta={
                            'year': item['fiscal_year'],
                            'report_id': item['report_id'],
                            'filename': file_url.split('/')[-1]
                        }
                    )

    def item_completed(self, results, item, info):
        if isinstance(item, BulletinItem):
            for success, res in results:
                if not success:
                    item['is_downloaded'] = False

        return item

    def file_path(self, request, response=None, info=None):
        return '%s/%s/%s' % (
            request.meta['year'], request.meta['report_id'],
            request.meta['filename']
        )


class MySQLPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        with self.conn.cursor() as cursor:
            if isinstance(item, ReportItem):  # 必须先于BulletinItem
                table = 'financial_statement_index'
            else:
                table = 'non_financial_statement_index'

            if spider.name.startswith('ifrs'):
                insert_item(
                    cursor, table, item, 'data', 'announcement_detail_type',
                    'file_urls'
                )
            else:
                update_item(
                    cursor, table, item, 'report_id', fiscal_year=None,
                    doc_type=None, body=None, data=None
                )

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
