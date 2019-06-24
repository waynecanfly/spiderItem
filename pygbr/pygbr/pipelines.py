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

from .items import CompanyItem, ProfileDetailItem, AnnounceItem
from .items import CompanyDataSourceItem
from .utils.dbman import insert_item, update_item, query_sql
from .settings import FILES_STORE, DOC_PATH


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        if spider.name == 'update_company_list':
            if isinstance(item, CompanyItem):
                item.setdefault('country_code_listed', 'GBR')
                item.setdefault('exchange_market_code', 'LSE')
            else:
                item.setdefault('latest_date', '2007-01-01')
                item.setdefault('is_batch', True)
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_info':
            if 'gmt_create' in item:
                item.setdefault('user_create', 'lq')
            else:
                item.setdefault('user_update', 'lq')
        elif spider.name == 'update_company_docs':
            if isinstance(item, AnnounceItem):
                item.setdefault('country_code', 'GBR')
                item.setdefault('exchange_market_code', 'LSE')
                item.setdefault('is_doc_url_direct', True)
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
            else:
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')
        elif spider.name == 'historical_announcement':
            item.setdefault('country_code', 'GBR')
            item.setdefault('exchange_market_code', 'LSE')
            item.setdefault('is_doc_url_direct', True)
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')

        return item


class CompanyProfilePipeline(object):
    """处理公司详情信息，包括定义、查询profile definition及detail"""

    sqls = {
        'query': """\
            select t1.id, t1.name, t2.id as parent_id, t2.name as parent_name \
            from company_profile_definition t1 left join \
            company_profile_definition t2 on t1.parent_id=t2.id where t1.name \
            like '%_gbr'\
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

    def process_item(self, item, spider):
        if isinstance(item, ProfileDetailItem):
            profile_name = item['company_profile_definition_id']
            parent_name = item['parent_id']
            for it in self.profiles:
                if (it['name'] == profile_name and
                        it['parent_name'] == parent_name):
                    item['company_profile_definition_id'] = it['id']
                    break
            else:  # 尚无该信息定义，需先创建
                cursor = self.conn.cursor()
                if parent_name:
                    for it in self.profiles:
                        if it['name'] == parent_name:  # 获取父标签id
                            item['parent_id'] = it['id']
                            break
                    else:  # 先创建父标签信息
                        cursor.execute(
                            self.sqls['insert'], (
                                parent_name, item['parent_label'],
                                'string', None, datetime.now(), 'lq'
                            )
                        )
                        cursor.execute('select last_insert_id() as id')
                        result = cursor.fetchone()
                        self.profiles.append({
                            'id': result['id'], 'name': item['parent_id'],
                            'parent_id': None, 'parent_name': parent_name
                        })
                        item['parent_id'] = result['id']

                cursor.execute(
                    self.sqls['insert'], (
                        profile_name, item['label'], 'string',
                        item['parent_id'], datetime.now(), 'lq'
                    )
                )
                cursor.execute('select last_insert_id() as id')
                result = cursor.fetchone()
                self.profiles.append({
                    'id': result['id'], 'name': profile_name,
                    'parent_id': item['parent_id'], 'parent_name': parent_name
                })
                item['company_profile_definition_id'] = result['id']
                cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query'])
            self.profiles = list(cursor.fetchall())

    def close_spider(self, spider):
        self.conn.close()


class ReportRenamePipeline(object):
    """根据报表标题确定会计区间，并生成report_id"""

    mappings = {
        'Q1': ['1st quarter results', 'q1'],
        'Q2': [
            'half-year report', 'half yearly report', 'interim report',
            'interim results', 'half year report', 'q2', 'hy', '2q results',
            'half-yearly financial report', 'results for the six months',
            '2q results', 'second quarter', 'half year results', 'h1',
            'interim financial results', 'first half', 'half-year results',
            'results for the 6 months', 'report for the six months',
            'second quarter results', 'second interim results',
            'results for the half year'],
        'Q3': [
            'third-quarter', '3rd quarter results', 'quarterly report q3',
            'third quarter results', 'q3'],
        'Q': ['appendix 5b', 'quarterly cashflow report', 'quarterly report'],
        'FY': [
            'annual financial report', 'final results for the year',
            'final results', 'annual report', 'annual results for the year',
            'fy', 'audited annual results', 'financial report for year',
            'financial results', 'audited final results', 'full year results',
            'audited results for the year', 'results for the year',
            'results of financial year']
    }

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if isinstance(item, AnnounceItem):
            item['report_id'] = item['company_code'] + self.gen_id()
            is_report = False
            for period in self.mappings:
                if is_report:
                    break
                for title in self.mappings[period]:
                    if title in item['file_original_title'].lower():
                        is_report = True
                        item['financial_statement_season_type_code'] = period
                        break

            with self.conn.cursor() as cursor:
                if self.exist_db(cursor, item):
                    raise DropItem('Already downloaded.')

        return item

    def exist_db(self, cursor, item):
        "按最近下载时间更新应判断当天数据是否已下载，避免最近重复"
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

    def close_spider(self, spider):
        self.conn.close()


class AnnounceDownloaderPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        if isinstance(item, AnnounceItem):
            for file_url in item['file_urls']:
                yield scrapy.Request(file_url)

    def item_completed(self, results, item, info):
        if isinstance(item, AnnounceItem):
            for success, res in results:  # 单文件，只遍历一次即可
                if success:
                    filename = item['report_id'] + '.' + item['doc_type']
                    filepath = os.path.join(
                        FILES_STORE, os.sep.join(res['path'].split('/')))
                    filename = item['report_id'] + '.' + item['doc_type']
                    try:
                        os.rename(
                            filepath, os.path.join(FILES_STORE, filename))
                        item['is_downloaded'] = True
                        item['doc_downloaded_timestamp'] = datetime.now()
                        item['doc_local_path'] = DOC_PATH.format(filename)
                    except FileNotFoundError:
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
            if 'gmt_create' in item:
                insert_item(
                    cursor, 'company_profile_detail', item,
                    'parent_id', 'label', 'parent_label'
                )
            else:
                update_item(
                    cursor, 'company_profile_detail', item, 'company_code',
                    'company_profile_definition_id', parent_id=None
                )
        elif spider.name == 'update_company_docs':
            if isinstance(item, CompanyDataSourceItem):
                update_item(cursor, 'company_data_source', item, 'company_id')
            elif 'financial_statement_season_type_code' in item:
                insert_item(
                    cursor, 'financial_statement_index', item,
                    'file_urls', 'files')
            else:
                insert_item(
                    cursor, 'non_financial_statement_index', item,
                    'file_urls', 'files')

        cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
