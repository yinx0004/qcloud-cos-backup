#!/usr/bin/env python
# Name:     backup.py
# Function: Encrypt & Transfer files to Qcloud COS Standard Storage.
# Author:   yinx 

import garlogger
import argparse
import os
import sys
import subprocess
import datetime
import shutil
import commands
import common
import multiprocessing
import threading
import Queue

logger = garlogger.getLogger(__name__, common.conf.get('backup_log'))
global date
#queue = Queue.Queue()

def pre_check():
    if not os.path.exists(src):
        logger.error('Daily backup %s does not exist!' % (src))
        sys.exit(1)

    if os.path.exists(enc_dir):
        logger.error('Encrypt directory %s already exists, product may in the process of encryption.' % enc_dir)
        sys.exit(1)

    if os.path.exists(des):
        logger.error('Backup %s already exists!' % (des))
        sys.exit(1)

def aes_256_cbc(src_file_name, queue):
    src_file = os.path.join(src,src_file_name)
    enc_file_name = src_file_name + '.ENC'
    enc_file = os.path.join(enc_dir,enc_file_name)
    key_file = common.conf.get("enc_key")
    logger.debug('Start to encrypt %s' % src_file)
    try:
        res = subprocess.Popen(['openssl', 'aes-256-cbc', '-e', '-kfile', key_file, '-in', src_file, '-out', enc_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = res.communicate()
        if output:
            logger.info(output)
        if error:
            logger.debug(error.strip())
            queue.put(src_file)
        else:
            logger.info('%s encrypted to %s.' % (src_file, enc_file))
    except OSError as e:
        logger.debug(e)
        queue.put(src_file)

def encrypt():
    logger.info('Prepare to encrypt daily backup')
    os.makedirs(enc_dir)
    logger.info('Created encryption directory %s.' % (enc_dir))
    logger.info('Begin to encrypt.')
    src_file_list = os.listdir(src)
    number = len(src_file_list)
    worker = int(common.conf.get('enc_worker'))
    manager = multiprocessing.Manager()
    enc_queue = manager.Queue()
    p = multiprocessing.Pool(processes=worker)
    for i in range(number):
        p.apply_async(aes_256_cbc, args=(src_file_list[i],enc_queue,))
    p.close()
    p.join()

    if not enc_queue.empty():
        print('queue not empty')
        while not enc_queue.empty():
            file_name = enc_queue.get()
            logger.error('Encrypt %s failed' % file_name )
            enc_queue.task_done()
        sys.exit(1)
    else:
        logger.info('Encryption done.')

def copy(file_queue, failed_queue):
    while not file_queue.empty():
        file_name = file_queue.get()
        src_file = os.path.join(enc_dir, file_name)
        des_file = os.path.join(des, file_name)
        logger.info('Start to transfer %s' % src_file)
        try:
            res = subprocess.Popen(['trickle', '-s', '-u', common.conf.get('trans_rate'), 'cp', src_file, des_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = res.communicate()
            if output:
                logger.info(output)
            if error:
                logger.error(error.strip())
                failed_queue.put(src_file)
            else:
                logger.info('%s transfered to %s.' % (src_file, des_file))
        except OSError as e:
            logger.error(e)
            failed_queue.put(src_file)
        file_queue.task_done()

def transfer():
    if os.path.exists(enc_dir):
        logger.info('Prepare to transfer.')
    else:
        logger.error('Encrypt directory %s does not exist!' % (enc_dir))
        sys.exit(1)

    os.makedirs(des)
    logger.info('Created COS backup directory %s.' % (des))
    logger.info('Begin to transfer.')
    enc_file_list = os.listdir(enc_dir)
    trans_queue = Queue.Queue()
    failed_queue = Queue.Queue()
    for enc_file_name in enc_file_list:
        trans_queue.put(enc_file_name)

    worker = int(common.conf.get('trans_worker'))
    for i in range(worker):
        thread = threading.Thread(target=copy, args=(trans_queue,failed_queue,))
        thread.start()
    thread.join()
    trans_queue.join()

    if not failed_queue.empty():
        while not failed_queue.empty():
            file_name = failed_queue.get()
            logger.error('Transfer %s failed' % file_name )
            failed_queue.task_done()
        sys.exit(1)
    else:
        logger.info('Transfer done.')

def backup(product, ip, region, bucket, date=common.daily_backup_date):
    global src, enc_dir, des
    src  = os.path.join(common.conf.get('daily_backup_base_dir'), ip, str(date))
    enc_product_dir = os.path.join(common.conf.get('enc_base_dir'), ip)
    enc_dir = os.path.join(enc_product_dir, str(date))
    des = os.path.join(common.conf.get('cos_base_dir'),'qcloud_' + region + '_' +  bucket + '_backup', product, str(date))
    logger.info('Begin to COS backup the daily backup of %s on %s.' % (product, date))

    pre_check()
    try:
        encrypt()
    except:
        logger.error('Encrypt failed.')
        sys.exit(1)

    try:
        transfer()
    except:
        logger.error('Transfer failed.')
        shutil.rmtree(des)
        logger.info('Removed destination directory %s.' % des)
        sys.exit(1)
    finally:
    #delete encrypt directory
        shutil.rmtree(enc_dir)
        logger.info('Removed encryption date directory %s.' % (enc_dir))

    try:
        os.rmdir(enc_product_dir)
        logger.info('Removed encryption product directory %s' % (enc_product_dir))
    except:
        logger.warning('Directory %s is not empty, will remove it when it\'s empty.' % (enc_product_dir))

    logger.info('Backup %s on %s success.' % (product, date))

if ( __name__ == "__main__" ):
    parser = argparse.ArgumentParser(description='Encrypt and transfer a product daily backup to QCloud COS.')
    parser.add_argument('--product', required=True, action='store', dest='product', help='product name, eg: db-im')
    parser.add_argument('--ip', required=True, action='store', dest='ip', help='backup server ip')
    parser.add_argument('--region', '-r', required=True, action='store', dest='region', choices=['hk'], help='Region of Qcloud')
    parser.add_argument('--bucket', '-b', required=True, action='store', dest='bucket', choices=['shopee','garena'], help='QCloud COS bucket')
    parser.add_argument('--date', action='store', dest='date', help='date of daily backup')
    args = parser.parse_args()
    product = args.product
    ip = args.ip
    region = args.region
    bucket = args.bucket
    date = args.date

    if date is None:
        backup(product, ip, region, bucket)
    else:
        backup(product, ip, region, bucket, date)

