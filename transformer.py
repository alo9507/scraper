import boto3
from decouple import config
import codecs
import csv
import pandas as pd

AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY')

s3 = boto3.client(
    's3',
    region_name='us-east-2',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

s3_resource = boto3.resource(
                            's3', 
                            region_name='us-east-2', 
                            aws_access_key_id=AWS_ACCESS_KEY_ID, 
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


def dateParser(date):
    parts=date.split("-")
    date = {'year': parts[0], 'month': parts[1], 'day': parts[2]}
    return date

def keyParser(key):
    parts = key.split("/")
    date = dateParser(parts[0])

    body = ""
    article_links = []
    comments = []
    shares = []
    reactions = []

    article_object=s3_resource.Object('prop-watch-raw', key)

    if('.txt' in parts[3]):
        body = article_object.get()['Body'].read().decode('utf-8') 
    elif '.csv' in parts[3]:
        for line in csv.DictReader(codecs.getreader("utf-8")(article_object.get()['Body'])):
          print("sdfsdfds", line)
        #   read_csv_from_s3('prop-watch-raw', )
    elif 'like' in parts[4]:
        for line in csv.DictReader(codecs.getreader("utf-8")(article_object.get()['Body'])):
          print(line)

    article_data = {
        'articleId': parts[2], 
        'reactions': reactions,
        'shares': shares,
        'comments': comments,
        'year': date['year'],
        'month': date['month'],
        'day': date['day'],
        'publication': parts[1],
        'article_links': article_links,
        'article_text': body
    }
    
    return article_data

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket='prop-watch-raw')

for page in pages:
    for obj in page['Contents']:
        json = keyParser(obj['Key'])
        # save json to a new location in S3
    
def read_csv_from_s3(bucket_name, key, column):
    data = s3.get_object(Bucket=bucket_name, Key=key)

    for row in csv.DictReader(codecs.getreader("utf-8")(data.get()['Body'])):
        print(row[column])