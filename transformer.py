import boto3
from decouple import config

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
    if('.txt' in parts[3]):
        article_object=s3_resource.Object('prop-watch-raw', key)
        body = article_object.get()['Body'].read().decode('utf-8') 

    article_data = {
        'articleId': parts[2], 
        'reactions': { 'likes': [] },
        'shares': [],
        'comments': [],
        'year': date['year'],
        'month': date['month'],
        'day': date['day'],
        'publication': parts[1],
        'article_link': "",
        'article_text': body
    }
    
    return article_data

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket='prop-watch-raw')

for page in pages:
    for obj in page['Contents']:
        json = keyParser(obj['Key'])
        print(json)
        # save json to a new location in S3
    