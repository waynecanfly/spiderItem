# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import os
import random
from datetime import datetime

import pymysql
from scrapy.exceptions import DropItem

from .items import CompanyItem, ProfileDetailItem, ReportItem, AnnounceItem
from .utils.dbman import insert_item, update_item, query_sql
from .settings import FILES_STORE, DOC_PATH


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        if spider.name == 'listed_issuers':
            if isinstance(item, CompanyItem):
                item.setdefault('country_code_listed', 'CAN')
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_list':
            if isinstance(item, CompanyItem):
                if 'gmt_update' in item:
                    item.setdefault('user_update', 'lq')
                else:
                    item.setdefault('country_code_listed', 'CAN')
                    item.setdefault('gmt_create', datetime.now())
                    item.setdefault('user_create', 'lq')
            else:
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
        elif spider.name == 'update_latest_ipo_date':
            if isinstance(item, CompanyItem):
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
        elif spider.name == 'update_company_info':
            if 'display_label' in item:  # create profile
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
            else:  # update profile
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
        elif spider.name == 'update_company_docs':
            item.setdefault('country_code', 'CAN')
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
            item.setdefault('is_doc_url_direct', True)
            if isinstance(item, ReportItem):
                item.setdefault('announcement_type', True)
        elif spider.name == 'docs_downloader':
            item.setdefault('gmt_update', datetime.now())
            item.setdefault('user_update', 'lq')

        return item


class CompanyProfilePipeline(object):
    """处理公司详情信息，包括定义、查询profile definition及detail"""

    sqls = {
        'query': """\
            select id, name, parent_id from company_profile_definition where \
            name like '%_can' escape 'S'\
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
        parent_id = None
        if 'parent' in item:
            for it in self.profiles:
                if item['parent']['name'] == it['name']:
                    parent_id = it['id']
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
            'id': result['id'], 'name': item['name'], 'parent_id': parent_id
        })
        return result['id']

    def process_item(self, item, spider):
        if isinstance(item, ProfileDetailItem):
            profile_name = item['name']
            for it in self.profiles:
                if it['name'] == profile_name:
                    item['company_profile_definition_id'] = it['id']
                    break
            else:  # 尚无该信息定义，需先创建
                cursor = self.conn.cursor()
                self.do_profile_id(cursor, item)
                # self.do_parent_id(cursor, item)
                for it in self.profiles:
                    if it['name'] == item['name']:
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


class DocRebuildPipeline(object):
    """重构文档信息，如report_id等字段"""

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if spider.name == 'update_company_docs':
            with self.conn.cursor() as cursor:
                if self.exist_db(cursor, item):
                    raise DropItem('Already downloaded.')

            if isinstance(item, ReportItem):
                if item['file_original_title'] == 'Annual Report':
                    item['financial_statement_season_type_code'] = 'FY'
                else:
                    item['financial_statement_season_type_code'] = 'Q'
                item['report_id'] = item['company_code'] + self.gen_id()
                item['fiscal_year'] = item['disclosure_date'].year
                item['is_downloaded'] = False
            elif isinstance(item, AnnounceItem):
                item['report_id'] = item['company_code'] + self.gen_id()
                item['fiscal_year'] = item['disclosure_date'].year
                item['is_downloaded'] = False

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


class AnnouncementTypePipeline(object):

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
        if (spider.name == 'update_company_docs' and
                type(item) == AnnounceItem):  # 只处理公告
            for it in self.types:
                if it['name'] == item['subject']:  # 已有公告类型
                    item['announcement_detail_type'] = it['id']
                    break
            else:  # 尚无该公告类型，需先创建
                cursor = self.conn.cursor()
                cursor.execute(
                    self.sqls['insert'], (
                        item['subject'], 'CAN', datetime.now(), 'lq'
                    )
                )
                cursor.execute('select last_insert_id() as id')
                result = cursor.fetchone()
                self.types.append({
                    'id': result['id'],
                    'name': item['subject']
                })
                item['announcement_detail_type'] = result['id']
                cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query'], ('lq', 'CAN'))
            self.types = list(cursor.fetchall())

    def close_spider(self, spider):
        self.conn.close()


class DocMatePipeline(object):
    """完善文档下载信息，存储至文件系统"""

    def process_item(self, item, spider):
        if spider.name == 'docs_downloader':
            filename = item['report_id'] + '.' + item['doc_type']
            filedir = os.path.join(FILES_STORE, item['fiscal_year'])
            if not os.path.exists(filedir):
                os.mkdir(filedir)
            filepath = os.path.join(filedir, filename)
            with open(filepath, 'wb') as f:
                f.write(item['data'])

            item['doc_local_path'] = DOC_PATH.format(
                item['fiscal_year'], filename)
            item['doc_downloaded_timestamp'] = datetime.now()
            item['is_downloaded'] = True
            item['gmt_update'] = datetime.now()
            item['user_update'] = 'lq'

        return item


class MySQLPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        cursor = self.conn.cursor()

        if spider.name == 'listed_issuers':
            if isinstance(item, CompanyItem):
                insert_item(cursor, 'company', item)
            else:
                insert_item(
                    cursor, 'company_profile_detail', item, 'name',
                    'display_label', 'data_type')
        elif spider.name == 'update_company_list':
            if isinstance(item, CompanyItem):
                if 'gmt_create' in item:
                    insert_item(cursor, 'company', item, 'instruments')
                else:  # 公司状态有改变
                    update_item(cursor, 'company', item, 'code')
            else:  # 公司的instruments信息
                insert_item(
                    cursor, 'company_profile_detail', item, 'name',
                    'display_label', 'data_type')
        elif spider.name == 'update_latest_ipo_date':
            update_item(cursor, 'company', item, 'code')
        elif spider.name == 'update_company_info':
            if 'gmt_create' in item:
                insert_item(
                    cursor, 'company_profile_detail', item, 'name',
                    'display_label', 'data_type', 'parent')
            else:
                update_item(
                    cursor, 'company_profile_detail', item, 'company_code',
                    'company_profile_definition_id', name=None)
        elif spider.name == 'update_company_docs':
            if isinstance(item, ReportItem):
                insert_item(
                    cursor, 'financial_statement_index', item, 'subject',
                    'file_urls')
            else:
                insert_item(
                    cursor, 'non_financial_statement_index', item, 'subject',
                    'file_urls')
        elif spider.name == 'docs_downloader':
            update_item(
                cursor, item['table'], item, 'report_id', fiscal_year=None,
                doc_type=None, table=None, data=None
            )

        cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
