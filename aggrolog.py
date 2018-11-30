import sys
import argparse

import boto3
import botocore

from datetime import datetime
import dataset

import pytz

def split_log_entry(entry):
    split_by_quotes = entry.split(' ')

    merged = []
    in_block = False
    block_close = None

    for entry in split_by_quotes:
        if not in_block:
            if entry[:1] == '"':
                in_block = True
                block_close = '"'
            elif entry[:1] == '[':
                in_block = True
                block_close = ']'

            if in_block:
                if entry[-1] == block_close:
                    to_merge = entry[1:-1] if entry[1:-1] != '-' else None
                    in_block = False
                else:
                    to_merge = entry[1:]

                merged.append(to_merge)
            elif entry == '-':
                merged.append(None)
            else:
                merged.append(entry)

        else: # in_block
            if entry[-1:] == block_close:
                merged[-1] += ' ' + entry[:-1]
                in_block = False
            else:
                merged[-1] += ' ' + entry

    return merged

def parse_log_entry(entry):
    as_list = split_log_entry(entry)

    fields = [
        'bucket_owner',
        'bucket',
        'time',
        'remote_ip',
        'requester',
        'request_id',
        'operation',
        'key',
        'request_uri',
        'http_status',
        'error_code',
        'bytes_sent',
        'object_size',
        'total_time',
        'turn_around_time',
        'referrer',
        'user_agent',
        'version_id'
    ]

    return dict(zip(fields, as_list))

def bot_filter(user_agent):
    return 'S3Console' not in user_agent

def to_local_time(log_date, timezone):
    return datetime.strptime(
        log_date,
        '%d/%b/%Y:%H:%M:%S %z'
    ).astimezone(
        timezone
    )

def download_latest_log_file(bucket, output_db):
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(bucket)
    exists = True

    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = e.response['Error']['Code']
        if error_code == '404':
            exists = False

    print(exists)

    if exists:
        log_count = 0

        last_object = None

        by_key = {}

        db = dataset.connect(f"sqlite:///{output_db}")
        table = db['access_logs']

        for key in bucket.objects.all():
            log_count += 1
            last_object = key.key

            if log_count % 100 == 0:
                print(f"Processed {log_count} logs...")

            tmpfile = "log.txt"
            bucket.download_file(last_object, tmpfile)

            with open(tmpfile) as f:

                content = f.readlines()
                parsed = list(map(lambda e: parse_log_entry(e.rstrip()), content))
                filtered = list(filter(lambda f: bot_filter(f['user_agent']), parsed))

                for log_entry in filtered:
                    key = log_entry['key']
                    keyed = by_key.get(key, [])
                    keyed.append(log_entry)
                    by_key[key] = keyed

                    stored = table.find_one(request_id=log_entry['request_id'])

                    if stored is None:
                        local_date = to_local_time(
                            log_entry['time'],
                            pytz.timezone('America/Los_Angeles')
                        )
                        date_index = local_date.strftime('%Y%m%d')
                        log_entry['date_index'] = date_index
                        table.insert(log_entry)


        indexes = by_key['index.html']

    else:
        print(f"[!] Bucket [{bucket}] was not found")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", help="The S3 bucket where the logs are located")
    parser.add_argument("--db", help="The output sqlite database", default="diag.db")
    args = parser.parse_args()
    bucket = args.bucket
    output_db = args.db

    download_latest_log_file(bucket, output_db)