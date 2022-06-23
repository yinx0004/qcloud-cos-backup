#!/usr/bin/env python
import ConfigParser
import os
import datetime
import MySQLdb as db
from contextlib import contextmanager

daily_backup_date = datetime.date.today() - datetime.timedelta(days=1)

garena_meta_db = {
    'host':'xxxxxxxx',
    'user':'xxxxxxxx',
    'passwd':'*************',
    'port':xxxx,
    'db':'xxxxxxxx',
}

shopee_meta_db = {
    'host':'xxxxxxxx',
    'user':'backup',
    'passwd':'*************',
    'port':xxxxx,
    'db':'xxxxxxxx',
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
