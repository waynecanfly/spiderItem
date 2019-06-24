# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime

import pymysql

from .items import CompanyItem, CompanyDataSourceItem, ProfileDetailItem
from .utils.dbman import insert_item, update_item, delete_item


class DefaultValuePipeline(object):
    """设置不同item的默认值"""

    def process_item(self, item, spider):
        if spider.name == 'update_company_list':
            if isinstance(item, CompanyItem):
                item.setdefault('country_code_listed', 'BRA')
                item.setdefault('exchange_market_code', 'BM&FBOVESPA')
                item.setdefault('user_create', 'lq')
            else:
                item.setdefault('latest_url', None)
                item.setdefault('latest_date', '2007-01-01')
                item.setdefault('is_batch', True)
                item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_info':
            if 'gmt_create' in item:
                item.setdefault('user_create', 'lq')
            else:
                item.setdefault('user_update', 'lq')
        elif spider.name == 'update_company_report':
            if isinstance(item, CompanyDataSourceItem):
                item.setdefault('user_update', 'lq')
            else:
                item.setdefault('country_code', 'BRA')
                item.setdefault('exchange_market_code', 'BM&FBOVESPA')
                item.setdefault('doc_type', 'csv')
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_report_mate':
            if 'parent_id' in item:  # 该报表缺所有内容
                item.setdefault('country_code', 'BRA')
                item.setdefault('exchange_market_code', 'BM&FBOVESPA')
                item.setdefault('doc_type', 'csv')
                item.setdefault('gmt_create', datetime.now())
                item.setdefault('user_create', 'lq')
            else:
                item.setdefault('gmt_update', datetime.now())
                item.setdefault('user_update', 'lq')

        return item


class CompanyProfilePipeline(object):
    """处理公司详情信息，包括定义、查询profile definition及detail"""

    sqls = {
        'query_all': """\
            select id, name from company_profile_definition where name like \
            '%_bra' and user_create='lq'\
        """,
        'insert_def': """\
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
            for it in self.profiles:
                if it['name'] == profile_name:
                    item['company_profile_definition_id'] = it['id']
                    break
            else:  # 尚无该信息定义，需先创建
                cursor = self.conn.cursor()
                for it in self.profiles:
                    if it['name'] == item['parent_id']:  # 父标签名称
                        item['parent_id'] = it['id']
                        break
                else:  # 先创建父标签信息
                    cursor.execute(
                        self.sqls['insert_def'], (
                            item['parent_id'], item['parent_label'], 'string',
                            None, datetime.now(), 'lq'
                        )
                    )
                    cursor.execute('select last_insert_id() as id')
                    result = cursor.fetchone()
                    self.profiles.append({
                        'id': result['id'], 'name': item['parent_id']
                    })
                    item['parent_id'] = result['id']

                cursor.execute(
                    self.sqls['insert_def'], (
                        profile_name, item['label'], 'string',
                        item['parent_id'], datetime.now(), 'lq'
                    )
                )
                cursor.execute('select last_insert_id() as id')
                result = cursor.fetchone()
                self.profiles.append({
                    'id': result['id'], 'name': profile_name
                })
                item['company_profile_definition_id'] = result['id']
                cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query_all'])
            self.profiles = list(cursor.fetchall())

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
        if spider.name == 'update_company_list':
            if isinstance(item, CompanyItem):
                insert_item(cursor, 'company', item)
            else:
                insert_item(cursor, 'company_data_source', item)
        elif spider.name == 'update_company_info':
            if isinstance(item, CompanyItem):
                update_item(cursor, 'company', item, 'code')
            elif 'gmt_create' in item:
                insert_item(
                    cursor, 'company_profile_detail', item, 'parent_id',
                    'parent_label', 'label'
                )
            else:
                update_item(
                    cursor, 'company_profile_detail', item, 'company_code',
                    'company_profile_definition_id'
                )
        elif spider.name == 'update_company_report':
            if isinstance(item, CompanyDataSourceItem):
                update_item(
                    cursor, 'company_data_source', item, 'company_id', 'mark')
            else:
                insert_item(cursor, 'financial_statement_index', item)
        elif spider.name == 'update_company_report_mate':
            if 'parent_id' in item:  # 一条报表变为多张，删除并插入
                delete_item(
                    cursor, 'financial_statement_index', item, 'parent_id')
                insert_item(
                    cursor, 'financial_statement_index', item, 'parent_id')

            if 'is_downloaded' in item:  # 补充下载成功，更新数据库状态
                update_item(
                    cursor, 'financial_statement_index', item, 'report_id')

        cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()
