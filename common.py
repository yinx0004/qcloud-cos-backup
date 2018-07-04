#!/usr/bin/env python
import ConfigParser
import os
import datetime
import MySQLdb as db
from contextlib import contextmanager

daily_backup_date = datetime.date.today() - datetime.timedelta(days=1)

garena_meta_db = {
    'host':'203.116.180.207',
    'user':'backup',
    'passwd':'*************',
    'port':40019,
    'db':'garena_meta_db',
}

shopee_meta_db = {
    'host':'10.65.145.7',
    'user':'backup',
    'passwd':'*************',
    'port':50001,
    'db':'garena_meta_db',
}

def parse_conf(section):
    file_name = 'cos.conf'
    file_dir = 'conf'
    conf_file = os.path.join(file_dir,file_name)

    cf = ConfigParser.ConfigParser(allow_no_value=True)
    cf.read(conf_file)
    conf = dict(cf.items(section))
    return conf

conf = parse_conf('cos')

@contextmanager
def db_conn(**kwargs):
    conn = db.connect(host=kwargs.get('host'),
        user=kwargs.get('user'),
        passwd=kwargs.get('passwd'),
        port=kwargs.get('port'),
        db=kwargs.get('db')
    )
    try:
        yield conn
    finally:
        if conn:
            conn.close()
