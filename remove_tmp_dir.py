#!/usr/bin/env python
import os
import re
import datetime
import shutil
import garlogger
import common

def valid_bucket(bucket):
    valid_bucket_list = []
    f = open('/etc/rc.local')
    
    for line in f:
        fields = line.strip().split()
        for field in fields:
            if re.search('/usr/local/bin/cosfs', field):
                valid_bucket_list.append(fields[1].split(':')[1])

    if bucket in valid_bucket_list:
        return 1
    else:
        return 0 
    
def main():
    expire_days = common.conf.get("tmp_dir_expire_days")
    buckets = common.conf.get("buckets")
    bucket_list = [bucket.strip() for bucket in buckets.split(',')]
    logger.info('Will remove tmp backup directory of %s days ago.' % expire_days)
    expire_date = datetime.date.today() - datetime.timedelta(days=int(expire_days)+1)
    logger.info('Tmp files on %s and before will be removed.' % expire_date)
    for bucket_name in bucket_list:
        if valid_bucket(bucket_name):
            des = os.path.join(common.conf.get('tmp_dir'),bucket_name)
            product_list = os.listdir(des)
            for product in product_list:
                logger.info('Check product %s in %s.' % (product, bucket_name))
                backup_list = os.listdir(os.path.join(des, product))
                remove_backup_list = []
                for backup in backup_list:
                    try:
                        backup_date = datetime.datetime.strptime(backup, '%Y-%m-%d').date()
                    except ValueError as e:
                        logger.warning("Directory '%s' under %s is not a date, please check!" % (backup, product))
                        continue
                    else:
                        if backup_date <= expire_date:
                            remove_dir = os.path.join(des, product, backup)
                            remove_backup_list.append(remove_dir)

                if not remove_backup_list:
                    logger.info('Porduct %s has no expired tmp directory.' % product)
                else:
                    for directory in remove_backup_list:
                        try:
                            shutil.rmtree(directory)
                        except e:
                            logger.error(e)
                            continue
                        else:
                            logger.info('Removed %s.' % directory)
        else:
            logger.error('Bucekt name %s is invalid.' % bucket_name)
            continue

if (__name__ == "__main__"):
    log_file = common.conf.get("cleanup_log")
    logger = garlogger.getLogger(__name__, log_file)
    main()
