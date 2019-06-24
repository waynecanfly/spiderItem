# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import random
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED

import pymysql
import scrapy
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem

from .items import AnnounceItem, CompanyItem, ProfileDetailItem, ReportItem
from .utils.dbman import insert_item, update_item
from .utils.mate import profiles_mapping
from .settings import FILES_STORE, ANNOUNCEMENT_PATH, REPORT_PATH


class DefaultValuePipeline(object):

    def process_item(self, item, spider):
        if (spider.name in ['announcement', 'update_company_docs'] and
                isinstance(item, AnnounceItem)):
            item.setdefault('country_code', 'USA')
            item.setdefault('language_written_code', 'en')
            item.setdefault('doc_type', 'zip')
            item.setdefault('is_doc_url_direct', False)
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
        elif spider.name == 'update_company_list':
            item.setdefault('gmt_create', datetime.now())
            item.setdefault('user_create', 'lq')
            if isinstance(item, CompanyItem):
                item.setdefault('country_code_listed', 'USA')
            else:
                item.setdefault('latest_url', None)
                item.setdefault('latest_date', '2007-01-01')  # 默认最早开始下载时间
                item.setdefault('is_batch', True)
        elif spider.name == 'announcement_mate':
            item.setdefault('gmt_update', datetime.now())
            item.setdefault('user_update', 'lq')

        return item


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
                if it['name'] == item['announcement_detail_type']:  # 已有公告类型
                    item['announcement_detail_type'] = it['id']
                    break
            else:  # 尚无该公告类型，需先创建
                cursor = self.conn.cursor()
                cursor.execute(
                    self.sqls['insert'], (
                        item['announcement_detail_type'], 'USA',
                        datetime.now(), 'lq'
                    )
                )
                cursor.execute('select last_insert_id() as id')
                result = cursor.fetchone()
                self.types.append({
                    'id': result['id'],
                    'name': item['announcement_detail_type']
                })
                item['announcement_detail_type'] = result['id']
                cursor.close()

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)
        with self.conn.cursor() as cursor:
            cursor.execute(self.sqls['query'], ('lq', 'USA'))
            self.types = list(cursor.fetchall())

    def close_spider(self, spider):
        self.conn.close()


class CompanyProfilePipeline(object):
    """处理公司详情信息，包括定义、查询profile definition及detail"""

    sqls = {
        'query_all': """\
            select ta.id, ta.name from company_profile_definition ta right \
            join (select distinct company_profile_definition_id id from \
            company_profile_detail where company_code like 'USA%') tb on \
            ta.id = tb.id\
        """,
        'insert_def': """\
            insert into company_profile_definition (name, display_label, \
            data_type, gmt_create, user_create) values (%s, %s, %s, %s, %s)\
        """,
    }

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['DBARGS'])

    def __init__(self, dbargs):
        self.dbargs = dbargs

    def process_item(self, item, spider):
        if isinstance(item, ProfileDetailItem):
            cursor = self.conn.cursor()
            label_name = item['company_profile_definition_id']
            profile_name = label_name + '_usa'
            for it in self.profiles:
                if it['name'] == profile_name:
                    item['company_profile_definition_id'] = it['id']
                    break
            else:  # 尚无该信息定义，需先创建
                cursor.execute(
                    self.sqls['insert_def'], (
                        profile_name, profiles_mapping[label_name],
                        'string', datetime.now(), 'lq'
                    )
                )
                cursor.execute('select last_insert_id() as id')
                result = cursor.fetchone()
                item['company_profile_definition_id'] = result['id']
                self.profiles.append(
                    {'id': result['id'], 'name': profile_name}
                )
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

        with self.conn.cursor() as cursor:
            if spider.name == 'announcement':
                insert_item(cursor, 'non_financial_statement_index', item)
            elif spider.name == 'announcement_mate':
                if item['is_downloaded']:  # 补充下载成功
                    update_item(
                        cursor, 'non_financial_statement_index', item,
                        'report_id', fiscal_year=None, doc_type=None,
                        file_urls=None, files=None
                    )
                else:
                    raise DropItem('Item added download failed')
            elif spider.name == 'update_company_list':
                if isinstance(item, CompanyItem):
                    insert_item(cursor, 'company', item)
                else:
                    insert_item(cursor, 'company_data_source', item)
            elif spider.name == 'update_company_info':
                if isinstance(item, CompanyItem):  # 公司更名
                    update_item(cursor, 'company', item, 'code')
                elif 'user_update' in item:
                    update_item(
                        cursor, 'company_profile_detail', item, 'company_code',
                        'company_profile_definition_id'
                    )
                else:
                    insert_item(cursor, 'company_profile_detail', item)
            elif spider.name == 'update_company_docs':
                if isinstance(item, ReportItem):
                    insert_item(cursor, 'financial_statement_index', item)
                elif isinstance(item, AnnounceItem):
                    insert_item(cursor, 'non_financial_statement_index', item)
                else:
                    update_item(
                        cursor, 'company_data_source', item, 'company_id')

        return item

    def open_spider(self, spider):
        self.conn = pymysql.connect(**self.dbargs)

    def close_spider(self, spider):
        self.conn.close()


class AnnounceRenamePipeline(object):

    def process_item(self, item, spider):
        if isinstance(item, AnnounceItem):
            item['report_id'] = item['company_code'] + self.gen_id()

        return item

    def gen_id(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id


class AnnounceDownloaderPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        if isinstance(item, AnnounceItem):
            for file_url in item['file_urls']:
                yield scrapy.Request(file_url)

    def item_completed(self, results, item, info):
        if isinstance(item, AnnounceItem):
            is_downloaded = all(it[0] for it in results)
            if is_downloaded:
                item['is_downloaded'] = True
                item['doc_downloaded_timestamp'] = datetime.now()
                self.rename_file(results, item)
            else:
                item['is_downloaded'] = is_downloaded
                item['doc_downloaded_timestamp'] = None
                item['doc_local_path'] = None
                for it in results:
                    if it[0]:
                        file = os.path.join(
                            FILES_STORE, os.sep.join(it[1]['path'].split('/')))
                        try:
                            os.remove(file)
                        except FileNotFoundError:
                            pass

        return item

    def rename_file(self, results, item):
        file_paths = [
            os.path.join(FILES_STORE, os.sep.join(it[1]['path'].split('/')))
            for it in results
        ]
        if isinstance(item, ReportItem):
            file_dir = os.path.join(
                FILES_STORE, 'reports', str(item['fiscal_year']))
            item['doc_local_path'] = REPORT_PATH.format(
                item['fiscal_year'],
                item['report_id'] + '.' + item['doc_type']
            )
        else:
            file_dir = os.path.join(
                FILES_STORE, 'announcements', str(item['fiscal_year']))
            item['doc_local_path'] = ANNOUNCEMENT_PATH.format(
                item['fiscal_year'],
                item['report_id'] + '.' + item['doc_type']
            )
        if not os.path.exists(file_dir):
            os.mkdir(file_dir)

        filename = item['report_id'] + '.' + item['doc_type']
        doc_path = os.path.join(file_dir, filename)

        if all(os.path.exists(x) for x in file_paths):  # item所有文件下载成功
            fzip = ZipFile(doc_path, 'w')
            for index, file in enumerate(file_paths):
                name = results[index][1]['url'].split('/')[-1]
                file_new_name = os.path.join(
                    os.path.split(file)[0],
                    item['report_id'] + '-' + name
                )
                os.rename(file, file_new_name)
                fzip.write(
                    file_new_name, os.path.basename(file_new_name),
                    compress_type=ZIP_DEFLATED
                )
                try:
                    os.remove(file_new_name)
                except FileNotFoundError:
                    pass
            fzip.close()
        else:
            item['is_downloaded'] = False
            item['doc_downloaded_timestamp'] = None
            item['doc_local_path'] = None
            for file in file_paths:
                try:
                    os.remove(file)
                except FileNotFoundError:
                    pass

    def close_spider(self, spider):
        try:
            os.rmdir(os.path.join(FILES_STORE, 'full'))
        except OSError:
            pass
