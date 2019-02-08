#!/usr/bin/env python
# Name:     cos_backup.py
# Function: Offsite backup to Qcloud COS Standard Storage.
# Author:   yinx 

import garlogger
import argparse
import os
import sys
import re
import commands
import datetime
import common
import backup

def get_local_ip():
    ips = commands.getoutput('hostname -I').split()
    for ip in ips:
        if re.match("10.10.|10.71.",ip):
            return ip

def lock():
    os.mknod(lock_file)

def unlock():
    os.remove(lock_file)

def check_running():
    if os.path.isfile(lock_file):
        logger.error('Offsite backup to %s %s bucket is running, exit!' % (region, bucket))
        sys.exit(1)
    else:
        lock()
        logger.info('COS backup to %s %s start.' % (region, bucket))

def check_env():
    mount_point = '/mnt/qcloud_%s_%s_backup' % (region, bucket)
    if mount_point not in commands.getoutput('df'):
        unlock()
        logger.error('COS %s %s bucket not mounted!' % (region, bucket))
        sys.exit(1)
    else:
        logger.info('Check %s %s bucket mounted, OK.' % (region, bucket))

def get_backup_product():
    with common.db_conn(**meta_db) as conn:
        with conn as cursor:
            row_count = cursor.execute("select ip,product_name from %s where cross_dest_ip = '%s' and dest_ip='%s'" % (policy_tab, cross_dest, get_local_ip()))
            if row_count > 0:
                products = cursor.fetchall()
                logger.debug(products)
                return products
            else:
                return None

def get_product_ip(product):
    with common.db_conn(**meta_db) as conn:
        with conn as cursor:
            row_count = cursor.execute("select ip from %s where cross_dest_ip = '%s' and dest_ip='%s' and product_name='%s' " % (policy_tab, bucket, get_local_ip(), product))
            if row_count > 0:
                product_ip = cursor.fetchall()
                logger.debug(product_ip)
                return product_ip[0][0]
            else:
                return None

def check_daily_backup(product_name):
    with common.db_conn(**meta_db) as conn:
        with conn as cursor:
            cursor.execute("select state from %s where product_name='%s' and begin_time like '%s%%' order by id desc limit 1" % (daily_stats_tab, product_name, daily_backup_date))
            daily_backup_state = cursor.fetchall()
            if daily_backup_state[0][0] == 'success':
                return True
            else:
                return False

def update_db(ip, product_name, daily_backup_date, start_time,  status, reason):
    with common.db_conn(**meta_db) as conn:
        conn.autocommit(True)
        with conn as cursor:
            cursor.execute("insert into %s (ip, product_name, daily_backup_date, start_time, end_time, status, reason) value ('%s', '%s', '%s', '%s', now(), '%s', '%s')" % (cos_stats_tab, ip, product_name, daily_backup_date, start_time, status, reason))

def backup_product(product_name, product_ip, region, bucket):
    start_time = datetime.datetime.now()
    if check_daily_backup(product_name):
        logger.info('%s daily backup on %s success, ready for cos backup.' % (product_name, daily_backup_date))
        backup_status = 'failed'
        reason = 'Encryption or transfer failed'
        try:
            backup.backup(product_name, product_ip, region, bucket)
            backup_status = 'success'
            reason = ''
        except Exception as e:
            logger.error('Backup %s failed: %s.' % (product_name, e))
        finally:
            try:
                logger.debug('product_ip=%s, product_name=%s, daily_backup_date=%s, start_time=%s, backup_status=%s, reason=%s' % (product_ip, product_name, daily_backup_date, start_time, backup_status, reason))
                update_db(product_ip, product_name, daily_backup_date, start_time, backup_status, reason)
            except Exception as e:
                logger.error('Update DB failed %s' % e)
    else:
        logger.warning('%s daily backup on %s failed' % (product_name, daily_backup_date))
        try:
            update_db(product_ip, product_name, daily_backup_date, start_time, 'failed', 'daily backup failed')
        except Exception as e:
            logger.error('Update DB failed: %s' % e)

def main():
    check_running()
    check_env()

    try:
        if product is None:
            logger.info('Get backup product.')
            products = get_backup_product()
            if products:
                for product_ip,product_name in products:
                    try:
                        backup_product(product_name, product_ip, region, bucket)
                    except:
                        continue
            else:
                logger.error('There\'s no backup on this server for %s bucket!' % (bucket))
                logger.error('Exit!')
                sys.exit(1)
        else:
                logger.info('Offsite backup %s only.' % product)
                try:
                    product_ip = get_product_ip(product)
                    logger.debug('Get product ip %s' % product_ip)
                except Exception as e:
                    logger.error('Get ip error: %s.' % e)
                if product_ip:
                    logger.info('Will backup %s.' % product)
                    backup_product(product, product_ip, region, bucket)
                    logger.info('Backup %s end.' % product)
                else:
                    logger.error('There\'s no backup of %s on this server or it is not supposed to backup to %s bucket!' % (product, bucket))
                    logger.error('Exit!')
                    sys.exit(1)
        logger.info('COS backup finished.')
    except Exception as e:
        logger.error(e)
        logger.error('COS backup failed!')
    finally:
        unlock()

if ( __name__ == "__main__" ):
    parser = argparse.ArgumentParser(description='Database offsite backup to QCloud COS.')
    parser.add_argument('--region', '-r', required=True, action='store', dest='region', help='COS region', choices=['hk','sg'])
    parser.add_argument('--bucket', '-b', required=True, action='store', dest='bucket', help='COS bucket', choices=['shopee','garena'])
    parser.add_argument('--product', '-p', action='store', dest='product', help='product to be backuped')
    args = parser.parse_args()
    region = args.region
    bucket = args.bucket
    product = args.product

    lock_file = 'cos_backup_%s_%s.lock' % (region, bucket)
    log_file = '/var/log/qcloud/cos_backup_%s_%s.log' % (region,bucket)
    logger = garlogger.getLogger(__name__, log_file)

    daily_stats_tab = "backup_statistics"
    cos_stats_tab="backup_cos_statistics"
    policy_tab="backup_policy"
    cross_dest = region + '_' + bucket
    daily_backup_date = common.daily_backup_date

    if bucket == 'shopee':
        meta_db = common.shopee_meta_db
    else:
        meta_db = common.garena_meta_db
    main()
