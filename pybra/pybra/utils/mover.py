"""将下载文件移至nas"""

import os
import csv

import pymysql

from ftputil import FTPHost

# Database options
DBARGS = {
    'host': '10.100.4.99',
    'user': 'liqiang',
    'passwd': 'originp123',
    'db': 'opd_common',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True
}


def search_fs_reports(local_dir):
    """搜索已下载到文件系统中的报表信息"""
    fs_reports = {}
    for root, dirs, files in os.walk(local_dir):
        for name in files:
            report_id = name.split('.')[0]
            file_path = os.path.join(root, name)
            fs_reports[report_id] = file_path
    return fs_reports


def search_db_reports(fs_reports):
    """基于文件信息搜索对应数据库记录"""
    db_reports = {}
    conn = pymysql.connect(**DBARGS)
    with conn.cursor() as cursor:
        for it in fs_reports:
            cursor.execute("""\
                select financial_statement_type_code type_, doc_source_url \
                doc_url from financial_statement_index where report_id=%s\
            """, (it,))
            db_reports[it] = cursor.fetchone()
    return db_reports


def merge_bs(bs1, bs2):
    """合并BS表"""
    with open(bs1, newline='') as f:
        reader = csv.reader(f)
        data1 = list(reader)

    with open(bs2, newline='') as fp:
        reader = csv.reader(fp)
        data2 = list(reader)

    data1.extend(data2[1:])
    with open(bs1, 'w', encoding='utf-8', newline='') as f_csv:
        writer = csv.writer(f_csv, quotechar='"')
        writer.writerows(data1)
    os.remove(bs2)
    return bs1


def pick_files(fs_reports, db_reports):
    conn = pymysql.connect(**DBARGS)
    cursor = conn.cursor()

    sqla = """
        update financial_statement_index set financial_statement_type_code=%s \
        where report_id=%s\
    """
    sqlb = """
        delete from financial_statement_index where report_id=%s\
    """

    files = []  # 挑选可转储文件
    for report_id in fs_reports:
        mate = db_reports[report_id]
        if mate['type_'] == 'BS1':
            for db_it in db_reports:
                if (db_reports[db_it]['doc_url'] == mate['doc_url'] and
                        db_reports[db_it]['type_'] == 'BS2'):
                    bs1 = fs_reports[report_id]
                    bs2 = bs1.replace(report_id, db_it)
                    files.append(merge_bs(bs1, bs2))
                    cursor.execute(sqla, ('BS', report_id))
                    cursor.execute(sqlb, (db_it, ))
                    break
        elif mate['type_'] != 'BS2':
            files.append(fs_reports[report_id])
    cursor.close()
    conn.close()

    return files


def move_nas(local_dir, remote_dir):
    fs_reports = search_fs_reports(local_dir)
    db_reports = search_db_reports(fs_reports)
    src_files = pick_files(fs_reports, db_reports)
    with FTPHost('10.100.4.102', 'root', 'originp123') as ftp_host:
        for local_file in src_files:
            *_, file_year, filename = local_file.split(os.sep)
            remote_fdir = os.path.join(remote_dir, file_year)
            if not ftp_host.path.exists(remote_fdir):
                ftp_host.mkdir(remote_fdir)
            ftp_host.upload(local_file, os.path.join(remote_fdir, filename))
            os.remove(local_file)

    # 删除空目录
    for it in local_dir:
        dir_path = os.path.join(local_dir, it)
        try:
            os.rmdir(dir_path)
        except OSError:
            pass


def main():
    local_dir = '/data/spiderData/bra'
    remote_dir = '/data/bra'
    move_nas(local_dir, remote_dir)


if __name__ == '__main__':
    main()
