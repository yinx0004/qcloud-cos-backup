# QCloud COS Backup 

Back up daily backup of databases to QCloud COS using COSFS provided by QCloud.

## Getting Started

These instructions will guide you how to deploy COS backup.

Modules:
- common.py: meta db info & config parser
- backup.py: encrypt and upload a daily backup of a specific product on a specific day, default is yesterday.
- cos_backup.py: Read from meta db and backup to specific region of shopee/garena bucket, update backup result in meta db.
- remove_expire.py: Remove the expired backup, keep  configurable 30 days backup in COS bucket.
- remove_tmp_dir.py: COS doesn’t support immediately cleanup upload temporary directory after upload, only support cleanup after unmount bucket.


### Prerequisites

- Need COSFS installed.

- Need trickle to be installed. To limit the transfer speed to cloud.
```
yum install -y trickle
```
- Python packages
```
yum install python-argparse
yum install MySQL-python
```

## Installing
Put the whole directory somewhere(eg: /opt)
```
├── backup.py
├── common.py
├── conf
│   └── cos.conf
├── cos_backup.py
├── garlogger.py
├── remove_expire.py
└── remove_tmp_dir.py
```

## Configuration

- buckets: A list of bucket name separated by comma, used for COS temporary dirtory cleanup
  ```
  buckets = shopee-backup-hk, shopee-backup-sg
  ```

- enc_worker: Worker number of encryption
  ```
  enc_worker = 6
  ```
- trans_worker: Worker number of upload
  ```
  trans_worker = 5
  ```
- trans_rate: Upload rate limit in KB
  ```
  trans_rate = 40000
  ```


## Author
yinx@seagroup.com
