import boto3
from decouple import config
import json
import threading
import pickle

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

paginator = s3.get_paginator('list_objects_v2')


def date_parser(date):
    parts=date.split("-")
    date = {'year': parts[0], 'month': parts[1], 'day': parts[2]}
    return date


def key_parser(key):
    parts = key.split("/")
    date = date_parser(parts[0])

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

    return article_data


def save_output(res, file_name):
    with open(file_name, 'w') as outfile:
        for key, value in res.items():
            outfile.write(json.dumps(value)+"\n")


def unpickle_partial_results():
    partial_aggregated_results = list()
    for index in range(12):
        results_file = open("aggregated-result-"+str(index)+".pickle", 'rb')
        partial_aggregated_results.append(pickle.load(results_file))
    return partial_aggregated_results


def merge_results():
    # unpickle
    partial_aggregated_results = unpickle_partial_results()

    final_results = {}
    # merge on article id key for each of the 12 thread's aggregated results
    for aggregated_result in partial_aggregated_results:
        for key, value in enumerate(aggregated_result):
            articleId = key
            final_results[articleId]={**final_results.get(articleId, {}), **aggregated_result.get(articleId, {})} # spread the new stuff into the old stuff
    
    return final_results


def pickle_aggregated_result(aggregated_result, file_name):
    with open(file_name, 'wb') as outfile:
        pickle.dump(aggregated_result, outfile)


def get_results(thread_number, pages):
    p_num = 1
    obj_num = 0
    aggr_results = {}
    for page in pages:
        p_num +=1
        for obj in page['Contents']:
            print("{}: parsing object {} in thread {}.".format(str(obj_num), obj['Key'], thread_number))
            result = key_parser(obj['Key'])
            if len(result) != 0:
                articleId = result['articleId']
                aggr_results[articleId]={**aggr_results.get(articleId, {}), **result} # spread the new stuff into the old stuff
                if obj_num%1000 == 0:
                    pickle_aggregated_result(aggr_results, "aggregated-result-"+str(index)+".pickle")
                    print('page: ' + str(p_num) + ' obj num: ' + str(obj_num) + '\n')
            obj_num += 1


def divide_page_iterator():
    all_pages = list()
    StartingToken = None
    MaxItems = 53670  # 644,060 objects divided by 12 threads = 53,670 per thread
    PageSize = 500
    NextContinuationTokenIndex = 107  # 53,670 divided by 500 objects per page means the index of the final page occurs at postiion 107
    for x in range(12):
        pages = paginator.paginate(Bucket='prop-watch-raw', PaginationConfig={'MaxItems': MaxItems, 'PageSize': PageSize, 'StartingToken': StartingToken })
        for idx, val in enumerate(pages):
            print("Current Index: " + str(idx))
            if idx == NextContinuationTokenIndex:
                StartingToken = val['NextContinuationToken']
                print("StartingToken: " + StartingToken)
                all_pages.append(pages)
    return all_pages


if __name__ == '__main__':
    all_pages = divide_page_iterator()
    threads = list()
    for index in range(12):
        x = threading.Thread(target=get_results, args=(index, all_pages[index]))
        threads.append(x)
        x.start()
    
    for index, thread in enumerate(threads):
        print("Stopping thread {}".format(str(index)))
        thread.join()
        print("{} stopped.".format(str(index)))

    final_result = merge_results()
    save_output(final_result, "final_result.txt")
