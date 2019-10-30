import codecs
import csv
import gzip
import json
import re

import pytz
from dateutil import parser

import boto3
import botocore

BUCKET_NAME = 'openaq-data'
FETCHES_BUCKET = 'openaq-fetches'


def get_object_list(bucket_name=BUCKET_NAME, prefix='/', client=None):
    if client is None:
        client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
    result = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    ret = []
    prefixes = []
    if 'Contents' in result:
        ret += result['Contents']

    if 'CommonPrefixes' in result:
        prefixes += list(i['Prefix'] for i in result['CommonPrefixes'])

    while result['IsTruncated']:
        token = result['NextContinuationToken']
        result = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/', ContinuationToken=token)
        if 'Contents' in result:
            ret += result['Contents']

        if 'CommonPrefixes' in result:
            prefixes += list(i['Prefix'] for i in result['CommonPrefixes'])

    return ret, prefixes


# Usages:

def get_all_prefixes():
    # get all prefixes from fetches/realtime
    objects, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix='realtime/')


# {'Key': 'realtime/2013-11-27/2013-11-27.ndjson',
# 'LastModified': datetime.datetime(2017, 12, 28, 18, 9, 14, tzinfo=tzutc()),
# 'ETag': '"0214c4d59b099b041aa651c8221aa4f3"',
# 'Size': 10723,
# 'StorageClass': 'STANDARD'}
def get_all_objects_list(prefixes):
    client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
    # _, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix='realtime/', client=client)

    all_objects = []

    for prefix in prefixes:
        obj, pref2 = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix['Prefix'], client=client)
        all_objects += obj

    return all_objects


def filter_objects(all_objects, start_date, end_date):
    utc = pytz.utc
    if start_date.tzinfo is None:
        start_date = utc.localize(start_date)

    if end_date.tzinfo is None:
        end_date = utc.localize(end_date)

    return (x for x in all_objects if start_date <= x['LastModified'] <= end_date)


def filter_prefixes(prefixes, start_date, end_date):
    patt = re.compile(r'.*/(\d{4}-\d{2}-\d{2})/')
    return (x for x in prefixes if patt.search(x) and start_date <= parser.parse(patt.search(x).group(1)) <= end_date)


def serialize_object(l):
    return ','.join([l['Key'], str(l['Size']), l['ETag'], l['LastModified'].isoformat()]) + '\n'


def get_jsons_from_stream(stream, object_name=''):
    reader = codecs.getreader('utf-8')

    if object_name.endswith('.gz'):
        generator = reader.decode(gzip.decompress(stream.read()))[0].split('\n')
    else:
        generator = reader(stream)

    for line in generator:
        try:
            yield json.loads(line)
        except:
            pass
            #print('Unable to deserialize [%s]' % line)


def get_jsons_from_object(bucket, object_name, client=None):
    if client is None:
        client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    obj = client.get_object(Bucket=bucket, Key=object_name)
    body = obj['Body']

    reader = codecs.getreader('utf-8')

    if object_name.endswith('.gz'):
        generator = reader.decode(gzip.decompress(body.read()))[0].split('\n')
    else:
        generator = reader(body)

    for line in generator:
        try:
            yield json.loads(line)
        except:
            #print(f'Unable to deserialize [{line}]')
            pass

    
    body.close()


def read_object_list(input_file):
    objects = []

    object_reader = csv.DictReader(input_file, fieldnames=['Name', 'Size', 'ETag', 'LastModified'])
    for obj in object_reader:
        obj['LastModified'] = parser.parse(obj['LastModified'])
        obj['Size'] = int(obj['Size'])
        objects.append(obj)

    return objects


def split_record(record):
    """{"date":{"utc":"2018-06-06T23:00:00.000Z","local":"2018-06-07T05:00:00+06:00"},
              "parameter":"pm25",              
              "value":27,
              "unit":"µg/m³",
              "averagingPeriod":{"value":1,"unit":"hours"},
              "city":"Dhaka", --> stationMeta
              "location":"US Diplomatic Post: Dhaka", --> stationName
              "coordinates":{"latitude":23.796373,"longitude":90.424614}, --> stationMeta
              "country":"BD", -->stationMeta
              "sourceName":"StateAir_Dhaka", --> StationId
              "attribution":[{"name":"EPA AirNow DOS","url":"http://airnow.gov/index.cfm?action=airnow.global_summary"}], --> Provider              
              "sourceType":"government", --> Provider
              "mobile":false} --> StationMetaProvider (catchAll for all that does not fit the station core)"""

    if ('coordinates' not in record):
        record['coordinates'] = {'latitude': 0.0, 'longitude': 0.0}

    if  record['coordinates'] is None:
        record['coordinates'] = {'latitude': 0.0, 'longitude': 0.0}

    if 'averagingPeriod' not in record:
        record['averagingPeriod'] = ""

    mes_keys = ['parameter', 'value', 'unit', 'averagingPeriod', 'date', 'sourceName']
    stat_keys = ['location', 'city', 'coordinates', 'country', 'sourceName']

    measurement = {k: v for k, v in record.items() if k in mes_keys}

    station = {k: v for k, v in record.items() if k in stat_keys}
    ext = {k: v for k, v in record.items() if k not in (mes_keys + stat_keys)}

    return station, measurement, ext


def get_objects(prefix):
    objects, _ = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)
    return objects


def process_file(object_name, station_dao, mes_dao):
    records = 0
    for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=object_name):
        station, measurement, ext = split_record(record)
        stat_id = station_dao.store_from_json(station)
        mes_dao.store(station_id=stat_id, parameter=measurement['parameter'],
                      value=measurement['value'], unit=measurement['unit'],
                      averagingPeriod=measurement['averagingPeriod'],
                      date=measurement['date']['utc'])
        records += 1

    return records
