# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import random
from datetime import datetime

import pymysql
import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline

from .items import CompanyItem, ProfileDetailItem, AnnounceItem, ReportItem
from .utils.dbman import insert_item, update_item, query_sql
from .settings import DOC_PATH


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        if spider.name == 'update_company_list_en':
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
            if isinstance(item, CompanyItem):
                item.setdefault('country_code_listed', 'RUS')
                item.setdefault('exchange_market_code', 'MOEX')
            else:
                item.setdefault('data_type', 'string')
        elif spider.name == 'update_company_list_ru':
            if isinstance(item, CompanyItem):
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
            else:
                item.setdefault('data_type', 'string')
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_info_en':
            if 'display_label' in item:  # create
                item.setdefault('data_type', 'string')
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
            else:  # update
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
        elif spider.name == 'update_company_docs':
            if isinstance(item, AnnounceItem):
                item.setdefault('country_code', 'RUS')
                item.setdefault('exchange_market_code', 'MOEX')
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
            ta.name like '%_rus' escape 'S'\
        """,
        'insert': """\
            insert into company_profile_definition (name, display_label, \
            data_type, parent_id, gmt_create, user_create) values \
            (%s, %s, %s, %s, %s, %s)\
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
                    break
            else:  # 尚无该信息定义，需先创建
                self.do_profile_id(cursor, item)
                for it in self.profiles:
                    if (it['name'] == item['name'] and
                            it['parent_name'] == parent_name):
                        item['company_profile_definition_id'] = it['id']
                        break
            cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query'])
            self.profiles = list(cursor.fetchall())

    def close_spider(self, spider):
        self.conn.close()


class BulletinPipeline(object):

    # 财报时期关键字序列
    mates = [
        {'period': 'FY', 'keys': [
            'Annual Financial Report', 'Annual report', 'Годовой отчет'
        ]},
        {'period': 'HY', 'keys': [
            'Half-year Report', 'Interim report', 'Промежуточный отчет'
        ]},
        {'period': 'Q1', 'keys': ['1st Quarter Results']},
        {'period': 'Q3', 'keys': ['3rd Quarter Results']}
    ]

    sqls = {
        'query': """\
            select id, name from non_financial_announcement_detail_type where \
            user_create=%s and country_code=%s\
        """,
        'insert': """\
            insert into non_financial_announcement_detail_type (name, \
            country_code, gmt_create, user_create) values (%s, %s, %s, %s);\
        """
    }

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if isinstance(item, AnnounceItem):
            is_report = False
            for it in self.mates:
                if is_report:
                    break
                for key in it['keys']:
                    if key in item['file_original_title']:
                        item = ReportItem(item)
                        item['financial_statement_season_type_code'] = it[
                            'period']
                        try:
                            item['fiscal_year'] = item['end_date'].year
                        except AttributeError:
                            item['fiscal_year'] = item['disclosure_date'].year
                        is_report = True
                        break
            else:
                item.pop('end_date')
                item['fiscal_year'] = item['disclosure_date'].year

            with self.conn.cursor() as cursor:
                if self.exist_db(cursor, item):
                    raise DropItem('Doc already downloaded.')
                else:
                    item['report_id'] = item['company_code'] + self.gen_id()
                    if isinstance(item, AnnounceItem):
                        for t in self.types:
                            if t['name'] == item['announcement_detail_type']:
                                item['announcement_detail_type'] = t['id']
                                break
                        else:
                            cursor.execute(
                                self.sqls['insert'], (
                                    item['announcement_detail_type'], 'RUS',
                                    datetime.now(), 'lq'
                                )
                            )
                            cursor.execute('select last_insert_id() as id')
                            result = cursor.fetchone()
                            self.types.append({
                                'name': item['announcement_detail_type'],
                                'id': result['id']
                            })
                            item['announcement_detail_type'] = result['id']

        return item

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

    def gen_id(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query'], ('lq', 'RUS'))
            self.types = list(cursor.fetchall())

    def close_spider(self, spider):
        self.conn.close()


class DocDownloaderPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        if isinstance(item, AnnounceItem):
            for file_url in item['file_urls']:
                yield scrapy.Request(
                    file_url, meta={'item': item, 'download_timeout': 300},
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
            elif spider.name == 'update_company_list_ru':
                if isinstance(item, CompanyItem):
                    update_item(cursor, 'company', item, 'code')
                else:
                    insert_item(
                        cursor, 'company_profile_detail', item, 'name',
                        'display_label', 'data_type'
                    )
            elif spider.name == 'update_company_info_en':
                if 'gmt_create' in item:
                    insert_item(
                        cursor, 'company_profile_detail', item, 'name',
                        'display_label', 'data_type'
                    )
                else:
                    update_item(
                        cursor, 'company_profile_detail', item, 'company_code',
                        'company_profile_definition_id', name=None
                    )
            elif spider.name == 'update_company_docs':
                if isinstance(item, ReportItem):
                    insert_item(
                        cursor, 'financial_statement_index', item, 'file_urls',
                        'announcement_detail_type')
                elif isinstance(item, AnnounceItem):
                    insert_item(
                        cursor, 'non_financial_statement_index', item,
                        'file_urls')
                else:
                    update_item(
                        cursor, 'company_data_source', item, 'company_id')

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
