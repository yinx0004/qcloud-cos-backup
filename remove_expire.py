#!/usr/bin/env python
import os
import argparse
import datetime
import shutil
import garlogger
import common

def main():
    expire_days = common.conf.get("data_expire_days")
    logger.info('Will remove backup of %s days ago in %s %s.' % (expire_days, region, bucket))
    expire_date = datetime.date.today() - datetime.timedelta(days=int(expire_days)+1)
    logger.info('Backup on %s and before will be removed.' % expire_date)
    des = os.path.join(common.conf.get('cos_base_dir'),'qcloud_' + region + '_' +  bucket + '_backup')
    product_list = os.listdir(des)
    for product in product_list:
        logger.info('Check product %s.' % product)
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
            logger.info('Porduct %s has no expired backup.' % product)
        else:
            for directory in remove_backup_list:
                try:
                    shutil.rmtree(directory)
                except e:
                    logger.error(e)
                    continue
                else:
                    logger.info('Removed %s.' % directory)

if (__name__ == "__main__"):
    parser = argparse.ArgumentParser(description='Clear expired data from QCloud COS.')
    parser.add_argument('--region', '-r', required=True, action='store', dest='region', choices=['hk', 'sg'], help='Region of Qcloud')
    parser.add_argument('--bucket', '-b', required=True, action='store', dest='bucket', choices=['shopee','garena'], help='QCloud COS bucket')
    args = parser.parse_args()
    region = args.region
    bucket = args.bucket
    log_file = common.conf.get("cleanup_log")
    logger = garlogger.getLogger(__name__, log_file)
    main()
