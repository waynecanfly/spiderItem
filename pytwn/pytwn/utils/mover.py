"""将下载文件移至nas"""

import os

from ftputil import FTPHost


def move_nas(local_dir, remote_dir):
    with FTPHost('10.100.4.102', 'root', 'originp123') as ftp_host:
        for root, dirs, files in os.walk(local_dir, topdown=False):
            for name in files:
                file_year = root.split('/')[-1]
                remote_fdir = os.path.join(remote_dir, file_year)
                if not ftp_host.path.exists(remote_fdir):
                    ftp_host.mkdir(remote_fdir)
                local_file = os.path.join(root, name)
                ftp_host.upload(local_file, os.path.join(remote_fdir, name))
                os.remove(local_file)

            for name in dirs:
                os.rmdir(os.path.join(root, name))


def main():
    move_nas('/data/spiderData/kor', '/homes/KOR')


if __name__ == '__main__':
    main()
