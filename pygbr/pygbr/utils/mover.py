"""将下载文件移至nas"""

import os

from ftputil import FTPHost


def move_nas(local_dir, remote_dir):
    with FTPHost('10.100.4.102', 'root', 'originp123') as ftp_host:
        for root, dirs, files in os.walk(local_dir, topdown=False):
            for name in files:
                local_file = os.path.join(root, name)
                remote_file = remote_dir + '/' + name
                ftp_host.upload(local_file, remote_file)
                os.remove(local_file)


def main():
    pass


if __name__ == '__main__':
    main()
