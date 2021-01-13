import boto3
from decouple import config
import json

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

    article_object = s3.get_object(Bucket='prop-watch-raw', Key=key)

    article_data = {}

    if '.txt' in parts[3]:
        body = article_object['Body'].read().decode('utf-8')
        article_data = {
            'articleId': parts[2],
            'year': date['year'],
            'month': date['month'],
            'day': date['day'],
            'publication': parts[1],
            'article_text': body
        }
    elif '.csv' in parts[3]:
        lines = article_object['Body'].read().decode("utf-8")

        if 'article_links' in parts[3]:
            article_data = {
                'articleId': parts[2],
                'year': date['year'],
                'month': date['month'],
                'day': date['day'],
                'publication': parts[1],
                'article_link': lines
            }
        elif 'comments' in parts[3]:
            article_data = {
                'articleId': parts[2],
                'year': date['year'],
                'month': date['month'],
                'day': date['day'],
                'publication': parts[1],
                'comments': lines
            }
        elif 'shares' in parts[3]:
            article_data = {
                'articleId': parts[2],
                'year': date['year'],
                'month': date['month'],
                'day': date['day'],
                'publication': parts[1],
                'shares': lines
            }
        elif 'reactions' in parts[3]:
            article_data = {
                'articleId': parts[2],
                'year': date['year'],
                'month': date['month'],
                'day': date['day'],
                'publication': parts[1],
                'reactions': {'likes': lines}
            }

    # OUTPUT FORMAT:
    #    article_data = {
    #        'articleId': parts[2],
    #        'reactions': { 'likes': [] },
    #        'shares': shares,
    #        'comments': comments,
    #        'year': date['year'],
    #        'month': date['month'],
    #        'day': date['day'],
    #        'publication': parts[1],
    #        'article_link': article_links,
    #        'article_text': body
    #    }

    return article_data

def merge_objects(ilist):
    merged_json = []
    for d in ilist:
        aid = d.get('articleId')
        for i in ilist:
            if i.get('articleId') == aid:
                d.update(i)
        merged_json.append(d)
    # remove duplicates
    json_out = [dict(t) for t in {tuple(d.items()) for d in merged_json}]

    return json_out

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket='prop-watch-raw')

aggr_results = []
for page in pages:
    for obj in page['Contents']:
        result = keyParser(obj['Key'])
        aggr_results.append(result)

res = merge_objects(aggr_results)

for x in res:
    with open('aggr_results.txt', 'a') as outfile:
                outfile.write(json.dumps(x)+"\n")