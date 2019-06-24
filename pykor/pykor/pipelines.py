# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import random
from datetime import datetime

import scrapy
import pymysql
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline

from .utils.dbman import insert_item, update_item, query_sql
from .items import CompanyItem, ProfileDetailItem, AnnounceItem
from .settings import DOC_PATH


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        if spider.name == 'update_company_list_en':
            if isinstance(item, CompanyItem):
                item.setdefault('country_code_listed', 'KOR')
                item.setdefault('exchange_market_code', 'KRX')
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_list_ko':
            item.setdefault('gmt_update', datetime.now())
            item.setdefault('user_update', 'lq')
        elif spider.name == 'update_company_info_ko':
            if isinstance(item, CompanyItem):
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
            else:
                item.setdefault('data_type', 'string')
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_info_en':
            item.setdefault('data_type', 'string')
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_announcements_ko':
            item.setdefault('country_code', 'KOR')
            item.setdefault('exchange_market_code', 'KRX')
            item.setdefault('language_written_code', 'ko')
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_announcements_en':
            item.setdefault('country_code', 'KOR')
            item.setdefault('exchange_market_code', 'KRX')
            item.setdefault('language_written_code', 'en')
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_reports_en':
            item.setdefault('country_code', 'KOR')
            item.setdefault('exchange_market_code', 'KRX')
            item.setdefault('announcement_type', 1)
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')

        return item


class CompanyProfilePipeline(object):
    """处理公司详情信息，包括定义、查询profile definition及detail"""

    sqls = {
        'query': """\
            select ta.id, ta.name, tb.id parent_id, tb.name parent_name from \
            company_profile_definition ta left join \
            company_profile_definition tb on ta.parent_id = tb.id where \
            ta.name like '%_kor' escape 'S'\
        """,
        'insert': """\
            insert into company_profile_definition (name, display_label, \
            data_type, parent_id, gmt_create, user_create) values \
            (%s, %s, %s, %s, %s, %s)\
        """,
        'query_detail': """\
            select id from company_profile_detail where company_code=%s and \
            company_profile_definition_id = %s\
        """
    }

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def do_profile_id(self, cursor, item):
        parent_id, parent_name = None, None
        if 'parent' in item:
            for it in self.profiles:
                if item['parent']['name'] == it['name']:
                    parent_id = it['id']
                    parent_name = it['name']
                    break
            else:
                parent_id = self.do_profile_id(cursor, item['parent'])

        cursor.execute(
            self.sqls['insert'], (
                item['name'], item['display_label'], item['data_type'],
                parent_id, datetime.now(), 'lq')
        )
        cursor.execute('select last_insert_id() as id')
        result = cursor.fetchone()
        self.profiles.append({
            'id': result['id'], 'name': item['name'],
            'parent_id': parent_id, 'parent_name': parent_name
        })
        return result['id']

    def process_item(self, item, spider):
        if isinstance(item, ProfileDetailItem):
            cursor = self.conn.cursor()
            profile_name = item['name']
            if 'parent' in item:
                parent_name = item['parent']['name']
            else:
                parent_name = None
            for it in self.profiles:
                if (it['name'] == profile_name and
                        it['parent_name'] == parent_name):
                    item['company_profile_definition_id'] = it['id']
                    self.create_or_update(cursor, item)
                    break
            else:  # 尚无该信息定义，需先创建
                self.do_profile_id(cursor, item)
                # self.do_parent_id(cursor, item)
                for it in self.profiles:
                    if it['name'] == item['name']:
                        item['company_profile_definition_id'] = it['id']
                        break
                self.create_or_update(cursor, item)
            cursor.close()

        return item

    def create_or_update(self, cursor, item):
        """确定创建还是更新该信息项"""
        result = query_sql(
            cursor, self.sqls['query_detail'],
            (item['company_code'], item['company_profile_definition_id']),
            one=True
        )
        if result:
            item['gmt_update'] = datetime.now()
            item['user_update'] = 'lq'
        else:
            item['gmt_create'] = datetime.now()
            item['user_create'] = 'lq'

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query'])
            self.profiles = list(cursor.fetchall())

    def close_spider(self, spider):
        self.conn.close()


class DocRebuildPipeline(object):
    """重构文档信息，如report_id等字段"""

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if isinstance(item, AnnounceItem):
            item['report_id'] = item['company_code'] + self.gen_id()
            '''
            if spider.name in (
                'update_company_announcements_en',
                'update_company_announcements_ko',
                'update_company_reports_en'
            ):
                with self.conn.cursor() as cursor:
                    if self.exist_db(cursor, item):
                        raise DropItem('Already downloaded.')
            '''

        return item

    def gen_id(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def exist_db(self, cursor, item):
        if 'financial_statement_season_type_code' in item:
            table = 'financial_statement_index'
        else:
            table = 'non_financial_statement_index'

        sql = """\
            select id from {} where company_code=%s and doc_source_url=%s \
            and disclosure_date=%s\
        """.format(table)

        return query_sql(cursor, sql, (
            item['company_code'], item['doc_source_url'],
            item['disclosure_date']
        ), True)

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()


class DocDownloaderPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        if isinstance(item, AnnounceItem):
            for file_url in item['file_urls']:
                yield scrapy.Request(
                    file_url, meta={'item': item},
                )

    def item_completed(self, results, item, info):
        if isinstance(item, AnnounceItem):
            for success, res in results:  # 单文件，只遍历一次即可
                if success:
                    filename = item['report_id'] + '.' + item['doc_type']
                    item['is_downloaded'] = True
                    item['doc_downloaded_timestamp'] = datetime.now()
                    item['doc_local_path'] = DOC_PATH.format(
                        item['fiscal_year'], filename)
                else:
                    item['is_downloaded'] = False

        return item

    def file_path(self, request, response=None, info=None):
        file_year = request.meta['item']['fiscal_year']
        doc_id = request.meta['item']['report_id']
        doc_ext = '.' + request.meta['item']['doc_type']
        return '%s/%s%s' % (file_year, doc_id, doc_ext)


class MySQLPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        with self.conn.cursor() as cursor:
            if spider.name == 'update_company_list_en':
                if isinstance(item, CompanyItem):
                    insert_item(cursor, 'company', item)
                else:
                    insert_item(
                        cursor, 'company_profile_detail', item, 'name',
                        'display_label', 'data_type'
                    )
            elif spider.name == 'update_company_list_ko':
                update_item(cursor, 'company', item, 'code')
            elif spider.name == 'update_company_info_ko':
                if isinstance(item, CompanyItem):
                    update_item(cursor, 'company', item, 'code')
                else:
                    if 'gmt_create' in item:
                        insert_item(
                            cursor, 'company_profile_detail', item, 'name',
                            'display_label', 'data_type', 'parent'
                        )
                    else:
                        update_item(
                            cursor, 'company_profile_detail', item,
                            'company_code', 'company_profile_definition_id',
                            parent=None, name=None, display_label=None,
                            data_type=None
                        )
            elif spider.name == 'update_company_info_en':
                if 'gmt_create' in item:
                    insert_item(
                        cursor, 'company_profile_detail', item, 'name',
                        'display_label', 'data_type', 'parent'
                    )
                else:
                    update_item(
                        cursor, 'company_profile_detail', item, 'company_code',
                        'company_profile_definition_id',
                        parent=None, name=None, display_label=None,
                        data_type=None
                    )
            elif spider.name == 'update_company_announcements_ko':
                insert_item(
                    cursor, 'non_financial_statement_index', item, 'file_urls'
                )
            elif spider.name == 'update_company_announcements_en':
                insert_item(
                    cursor, 'non_financial_statement_index', item, 'file_urls'
                )
            elif spider.name == 'update_company_reports_en':
                insert_item(
                    cursor, 'financial_statement_index', item, 'file_urls'
                )

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
