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

from .items import CompanyItem, ProfileDetailItem, ReportItem, AnnounceItem
from .items import CompanyDataSourceItem
from .utils.dbman import insert_item, update_item
from .settings import FILES_STORE, DOC_PATH


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        if spider.name == 'update_company_list':
            if isinstance(item, CompanyItem):
                item.setdefault('country_code_listed', 'FRA')
            else:
                item.setdefault('latest_date', '2007-01-01')
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_info':
            if 'display_label' in item:
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
            else:
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
        elif spider.name == 'update_company_docs':
            if isinstance(item, CompanyDataSourceItem):
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
            else:
                item.setdefault('is_doc_url_direct', True)
                item.setdefault('country_code', 'FRA')
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
            if isinstance(item, ReportItem):
                item.setdefault('announcement_type', True)

        return item


class CompanyProfilePipeline(object):
    """处理公司详情信息，包括定义、查询profile definition及detail"""

    sqls = {
        'query': """\
            select id, name, parent_id from company_profile_definition where \
            name like '%_fra' escape 'S'\
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

    def process_item(self, item, spider):
        if isinstance(item, ReportItem):
            label = item['file_original_title'].strip(': ').lower()
            period = None
            if label in [
                'annual report', 'consolidated accounts', 'yearly accounts',
                'full year results',
            ]:
                period = 'FY'
            elif label in [
                'half year accounts', 'half year results',
                'half year group management discussion and analysis'
            ]:
                period = 'Q2'
            elif label in ['q1', 'q2', 'q3', 'q4']:
                period = label.upper()

            if period:
                item['report_id'] = item['company_code'] + self.gen_id()
                item['financial_statement_season_type_code'] = period
            else:
                raise DropItem("Can't determine result type")
        elif isinstance(item, AnnounceItem):
            item['report_id'] = item['company_code'] + self.gen_id()

        return item

    def gen_id(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id


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
        if type(item) == AnnounceItem:  # 只处理公告
            for it in self.types:
                if it['name'] == item['caption']:  # 已有公告类型
                    item['announcement_detail_type'] = it['id']
                    break
            else:  # 尚无该公告类型，需先创建
                cursor = self.conn.cursor()
                cursor.execute(
                    self.sqls['insert'], (
                        item['caption'], 'FRA', datetime.now(), 'lq'
                    )
                )
                cursor.execute('select last_insert_id() as id')
                result = cursor.fetchone()
                self.types.append({
                    'id': result['id'],
                    'name': item['caption']
                })
                item['announcement_detail_type'] = result['id']
                cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query'], ('lq', 'FRA'))
            self.types = list(cursor.fetchall())

    def close_spider(self, spider):
        self.conn.close()


class DocDownloaderPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        if isinstance(item, AnnounceItem):
            for file_url in item['file_urls']:
                yield scrapy.Request(file_url)

    def item_completed(self, results, item, info):
        if isinstance(item, AnnounceItem):
            for success, res in results:  # 单文件，只遍历一次即可
                if success:
                    filepath = os.path.join(
                        FILES_STORE, os.sep.join(res['path'].split('/')))
                    file_year_dir = os.path.join(
                        FILES_STORE, str(item['fiscal_year']))
                    if not os.path.exists(file_year_dir):
                        os.mkdir(file_year_dir)
                    filename = item['report_id'] + '.' + item['doc_type']
                    try:
                        os.rename(
                            filepath, os.path.join(file_year_dir, filename))
                        item['is_downloaded'] = True
                        item['doc_downloaded_timestamp'] = datetime.now()
                        item['doc_local_path'] = DOC_PATH.format(
                            item['fiscal_year'], filename)
                    except (FileNotFoundError, FileExistsError):
                        item['is_downloaded'] = False
                else:
                    item['is_downloaded'] = False

        return item

    def close_spider(self, spider):
        try:
            os.rmdir(os.path.join(FILES_STORE, 'full'))
        except OSError:
            pass


class MySQLPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        cursor = self.conn.cursor()

        if spider.name == 'update_company_list':
            if isinstance(item, CompanyItem):
                insert_item(cursor, 'company', item)
            else:
                insert_item(cursor, 'company_data_source', item)
        elif spider.name == 'update_company_info':
            if isinstance(item, CompanyItem):
                update_item(cursor, 'company', item, 'code')
            elif 'display_label' in item:  # 创建
                insert_item(
                    cursor, 'company_profile_detail', item, 'display_label',
                    'name', 'parent', 'data_type'
                )
            else:
                update_item(
                    cursor, 'company_profile_detail', item, 'company_code',
                    'company_profile_definition_id', name=None
                )
        elif spider.name == 'update_company_docs':
            if isinstance(item, CompanyDataSourceItem):
                update_item(cursor, 'company_data_source', item, 'company_id')
            elif isinstance(item, ReportItem):
                insert_item(
                    cursor, 'financial_statement_index', item, 'caption',
                    'file_urls')
            else:
                insert_item(
                    cursor, 'non_financial_statement_index', item, 'caption',
                    'file_urls')

        cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
