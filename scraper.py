# AWS file structure: date/publicationPage/postId -> {/article, /reactions, shares.csv, comments.csv}

from facebook_scraper import get_posts
from datetime import datetime
import boto3
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import ssl
from urllib.parse import urlsplit
import logging
import threading
import pdb
from botocore.errorfactory import ClientError
from decouple import config

ssl._create_default_https_context = ssl._create_unverified_context

AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY')

s3 = boto3.resource(
    's3',
    region_name='us-east-2',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

days_to_scrape = 1

def get_diffbot_results(token, article_link, keywords_list, logs):
    DIFF_BOT_PARAMS = {'token': token, 'url': article_link}
    diffbot_response = requests.get('https://api.diffbot.com/v3/article', params=DIFF_BOT_PARAMS)
    try:
        obj = diffbot_response.json()
    except ValueError:
        return 'decode_err', False
    if 'error' in obj.keys():
        if (obj['errorCode'] == 429) or (obj['errorCode'] == 401):
            error = "DIFFBOT ERROR: " + obj['error'] + "\n" + article_link + "\n"
            logs.write(error)
            log_to_s3(error, "diffbot_error")
            return "DIFFBOT_ERROR", False
        return "no text", False
    text = obj['objects'][0]['text']
    hasKeyword = False

    for keyword in keywords_list:
        if keyword in text:
            hasKeyword = True

    return text, hasKeyword

def log_to_s3(body, prefix):
    curr_date = datetime.today().strftime("%d/%m/%Y")
    curr_time = datetime.now().strftime("%H:%M:%S")
    propwatch_logs = s3.Object('propwatchlogs', curr_date + '/' + prefix + '/' + curr_time + '/' + prefix + '.txt')
    propwatch_logs.put(Body=str(body))

def scrape(fb_pages_fileName, last_date_to_scrape, keyword_list, diffbot_tokens, thread_num):
    tokens = diffbot_tokens[:]
    token = "FAKE_TOKEN"
    if len(tokens) > 0:
        token = tokens.pop(0)
    else:
        raise Exception("Diffbot keys is empty!")

    f = open(fb_pages_fileName, "r")
    fb_pages_names = f.readline().split(',')
    fb_pages_names[-1] = fb_pages_names[-1].replace('\n', '')

    output_filename = './output/h/huawei' + str(thread_num + 1) + '.txt'
    output = open(output_filename, 'w')
    logs_filename = './output/l/logs' + str(thread_num + 1) + '.txt'
    logs = open(logs_filename, 'w')


    total_num_posts_read = 0
    total_num_pages_read = 0
    total_num_link_skipped = 0

    dead_page_list = []
    timeout_page_list = []
    dead_site_list = []

    links_huawei = []

    for page_name in fb_pages_names:
        total_num_pages_read = total_num_pages_read + 1

        first_post = True  # account for pinned post
        num_posts_read = 0
        num_posts_link = 0
        num_link_skipped = 0

        num_403_err = 0
        num_404_err = 0
        num_525_err = 0
        num_http_err = 0
        num_conn_err = 0
        timeout_err = 0
        num_other_err = 0
        num_uni_err = 0
        num_html_p_err = 0
        num_decode_err = 0
        num_bs4_skip = 0
        num_fb_timeout = 0

        num_huawei = 0
        last_post_id = 0
        dead_FB_page = False
        FB_timeout = False
        page_skipped_on_timeout = True
        page_depth_cut_short = False

        timed_out_sites = {} # keep track of time outs per site
        bs4_skip_sites = {}
        skip_sites = [] # reset for each page, because of bit.ly use
        dead_links = []
        post_ids_huawei = []
        all_links = []
        all_post_text = []
        all_post_ids = []

        post_ids_body_text_huawei = []

        date_last_post_read = '_'

        try:
            for post in get_posts(page_name, extra_info=True, pages=10000): #1800, ~ 6 months min if avg 30 posts / day

                num_posts_read = num_posts_read + 1

                #  print('post_id', post['post_id'])
                # add page name to post dict
                post['page_name'] = page_name

                no_date = False
                post_date_dt = '_'
                if post['time']:
                    post_date_dt = post['time']
                else:
                    no_date = True
                    post_date_dt = 'no_date'

                # if (curr_date_dt - timedelta(days_to_scrape)) <= post_date_dt: # check within valid time range
                #     break

                post_date = '_'
                if not no_date: # check that there's a date value
                    post_date = post['time'].strftime("%Y-%m-%d")
                    date_last_post_read = post_date
                    if (last_date_to_scrape > post_date_dt) and not first_post:  # check within valid time range AND not first post
                        # print('no longer in desired date range:', post_date_dt)
                        break
                    elif (last_date_to_scrape > post_date_dt) and first_post: # outside date range, but pinned post
                        # print('skipped pinned post:', post_date_dt)
                        continue
                else:
                    # TODO - improve logging here (skip because no date)
                    continue # no date

                text = post['text']
                article_title = post['shared_text']

                if (article_title):  # if article title exists, extract the title and concatonate
                    article_title = article_title.splitlines()
                    if len(article_title) >=2:
                        article_title = article_title[1]
                        article_title = '-'.join([w for w in article_title.split()])
                    # print('article_Post_title', article_title)

                postId = '_'
                if post['post_id']:
                    postId = post['post_id']
                    all_post_ids.append(postId)
                
                articleAlreadyPresent = True
                try:
                    s3.Object('prop-watch-raw', post_date + '/' + page_name + '/' + postId).load()
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        articleAlreadyPresent = False

                if (articleAlreadyPresent != True):
                    post_text = ''
                    if post['post_text']:
                        post_text = post['post_text'].replace('\n', ' ')
                        all_post_text.append(post_text)
                    link = post['link'] # post_url
                    likes = post['likes']
                    comments = post['comments']
                    shares = post['shares']
                    last_post_id = postId

                    for keyword in keyword_list:
                        if keyword in post_text:
                            post_ids_body_text_huawei.append(postId)

                    base_url = '_'
                    if link:
                        num_posts_link = num_posts_link + 1
                        all_links.append(link)
                        base_url = urlsplit(link).hostname

                        # if !articleAlreadyPresent [scrape] else continue;
                        result = get_diffbot_results(token, link, keyword_list, logs)
                        
                        article_text = result[0]
                        if article_text == "DIFFBOT_ERROR":
                            if len(tokens) > 0:
                                logs.write("BAD TOKEN: " + token + "\n")
                                log_to_s3("BAD TOKEN: " + token + "\n", "bad_token")
                                token = tokens.pop(0)
                            else:
                                logs.close()
                                raise Exception("No more working DiffBot keys (either expired or out of credits)! Ending scrape!")
                        hasKeyword = result[1]

                        # store web site article content IF it contains keyword
                        if hasKeyword == True:
                            num_huawei = num_huawei + 1
                            post_ids_huawei.append(postId)
                            links_huawei.append(link)
                            print('HUAWEI!', postId, link)
                            article_text = article_text.replace('\n', ' ')
                            output.write('page_name: ' + page_name + ', post_id: ' + postId + ' ' + link + '\n' + article_text + '\n' + '______________________________________')

                        propwatch = s3.Object('prop-watch-raw',
                                            post_date + '/' + page_name + '/' + postId + '/' + 'Web-{}.txt'.format(article_title))
                        propwatch.put(Body=str(article_text))

                        propwatch = s3.Object('prop-watch-raw',
                                            post_date + '/' + page_name + '/' + postId + '/' + 'article_link.csv')
                        propwatch.put(Body=str(link))

                    if 'reactions' in post: # store reactions
                        for key, value in post['reactions'].items():
                            propwatch = s3.Object('prop-watch-raw',
                                                post_date + '/' + page_name + '/' + postId + '/' + 'reactions/' + key + '.csv')
                            propwatch.put(Body=str(value))
                    else:
                        propwatch = s3.Object('prop-watch-raw',
                                            post_date + '/' + page_name + '/' + postId + '/' + 'reactions/' + 'like.csv')
                        propwatch.put(Body=str(likes))

                    # store comments
                    propwatch = s3.Object('prop-watch-raw',
                                        post_date + '/' + page_name + '/' + postId + '/' + 'comments.csv')
                    propwatch.put(Body=str(comments))

                    # store shares
                    propwatch = s3.Object('prop-watch-raw',
                                        post_date + '/' + page_name + '/' + postId + '/' + 'shares.csv')
                    propwatch.put(Body=str(shares))

                    print(str(thread_num), str(num_posts_read), post_date_dt)

                    first_post = False

        except requests.exceptions.HTTPError:
            dead_FB_page = True
            dead_page_list.append(page_name)
            print("HTTP_ERROR", requests.exceptions.HTTPError)
            print('dead FB page')
        except requests.exceptions.ConnectionError:
            FB_timeout = True
            timeout_page_list.append(page_name)
            num_fb_timeout = num_fb_timeout + 1
            print('connection err')
        except requests.exceptions.ReadTimeout:
            FB_timeout = True
            timeout_page_list.append(base_url)
            num_fb_timeout = num_fb_timeout + 1
            print('read timeout')
        except IOError:
            print('Could not find next page. Date of last post read:', post_date)
            page_depth_cut_short = True


        total_num_posts_read = total_num_posts_read + num_posts_read
        total_num_link_skipped = total_num_link_skipped + num_link_skipped
        
        curr_time = datetime.now().strftime("%H:%M:%S")
        print(curr_time + ' page_name:', page_name, 'num_posts_read:', num_posts_read, 'num_posts_link:', num_posts_link, 'num_link_skipped:', num_link_skipped, 'num_huawei:', num_huawei, 'last_post_id:', last_post_id, 'dead_FB_page:', dead_FB_page, 'FB_read_timeout:', FB_timeout, 'page_depth_cut_short:', page_depth_cut_short)
        print('ERR STATS:' + ' num_403_err:', num_403_err, 'num_404_err:', num_404_err, 'num_525_err:', num_525_err, 'num_http_err:', num_http_err, 'num_conn_err:', num_conn_err, 'timeout_err:', timeout_err, ' num_other_err: ', num_other_err, ' num_uni_err: ', num_uni_err, ' num_html_p_err: ', num_html_p_err, ' num_decode_err: ', num_decode_err, ' num_bs4_skip: ', num_bs4_skip, ' num_fb_timeout: ', num_fb_timeout, ' date_last_post_read: ', date_last_post_read)

        error_log = curr_time + ' page_name: ' + str(page_name) + ' num_posts_read: ' + str(num_posts_read) + ' num_posts_link: ' + str(num_posts_link) + ' num_link_skipped: ' + str(num_link_skipped) + ' num_huawei: ' + str(num_huawei) + ' last_post_id: ' + str(last_post_id) + ' dead_FB_page: ' + str(dead_FB_page) + ' FB_read_timeout: ' + str(FB_timeout) + ' page_depth_cut_short: ' + str(page_depth_cut_short) + ' \n' + 'ERR STATS: ' + ' num_403_err: ' + str(num_403_err) + ' num_404_err: ' + str(num_404_err) + ' num_525_err: ' + str(num_525_err) + ' num_http_err: ' + str(num_http_err) + ' num_conn_err: ' + str(num_conn_err) + ' timeout_err: ' + str(timeout_err) + ' num_other_err: ' + str(num_other_err) + ' num_uni_err: ' + str(num_uni_err) + ' num_html_p_err: ' + str(num_html_p_err) + ' num_decode_err: ' + str(num_decode_err) + ' num_bs4_skip: ' + str(num_bs4_skip) + ' num_fb_timeout: ' + str(num_fb_timeout) + ' date_last_post_read: ' + date_last_post_read +  '\n'
        logs.write(error_log)
        log_to_s3(error_log, "error_log")

        log_output = ''
        if dead_links:
            log_output = 'Dead links: '
            for link in dead_links:
                log_output = log_output + link + '\n'
            log_output = log_output + '\n'

        if skip_sites:
            log_output = log_output + 'Base urls skipped b/c too many (>20) timeouts: '
            for base_url in skip_sites:
                log_output = log_output + base_url + ', '
            log_output = log_output + '\n'

        if post_ids_huawei:
            log_output = log_output + 'post ids that link to huewei articles: '
            for post_id in post_ids_huawei:
                log_output = log_output + str(post_id) + ', '
            log_output = log_output + '\n'
        logs.write(log_output)
        log_to_s3(log_output, "log_output")

    final_output='total_num_pages_read: ' + str(total_num_pages_read) + 'total_num_link_skipped: '+ str(total_num_link_skipped) + 'total_num_posts_read: ' + str(total_num_posts_read)
    print('total_num_pages_read: ', total_num_pages_read)
    logs.write('\n total_num_pages_read: ' + str(total_num_pages_read))
    print('total_num_link_skipped: ', total_num_link_skipped)
    logs.write('\n total_num_link_skipped: ' + str(total_num_link_skipped))
    print('total_num_posts_read: ', total_num_posts_read)
    logs.write('\n total_num_posts_read: ' + str(total_num_posts_read))
    
    log_to_s3(final_output, "final_output")

    agg_output = '\n Dead FB pages: '
    for page_name in dead_page_list:
        agg_output = agg_output + page_name + '\n'
    agg_output = agg_output + '\n'

    agg_output = agg_output + 'Timed out FB pages: '
    for page_name in timeout_page_list:
        agg_output = agg_output + page_name + ', '
    agg_output = agg_output + '\n'

    agg_output = agg_output + 'Base URLs of dead sites: '
    for base_url in dead_site_list:
        agg_output = agg_output + base_url + ', '
    agg_output = agg_output + '\n'

    agg_output = agg_output + 'Huawei links: '
    for link in links_huawei:
        agg_output = agg_output + link + ', '
    agg_output = agg_output + '\n'

    agg_output = agg_output + 'Num Huawei articles: ' + str(len(links_huawei)) + '\n'

    agg_output = agg_output + 'Post Ids with Huawei in body text: '
    for post_id in post_ids_body_text_huawei:
        agg_output = agg_output + post_id + ', '
    agg_output = agg_output + '\n'

    agg_output = agg_output + 'All links: '
    for link in all_links:
        agg_output = agg_output + link + ', '
    agg_output = agg_output + '\n'
    logs.write(agg_output)
    log_to_s3(agg_output, "all_links")

    logs.write('\n All post text:' + '\n')
    for text in all_post_text:
        logs.write(text + '\n')
        log_to_s3(text, "all_post_text")

    agg_output = ''
    agg_output = agg_output + 'All post ids: '
    for post_id in all_post_ids:
        agg_output = post_id + text + ', '
    agg_output = agg_output + '\n'

    logs.write(agg_output)
    log_to_s3(agg_output, "all_post_ids")


if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG)
    
    keyword_list = ['Huawei', 'HUAWEI', 'huawei', 'Huawei’s']

    last_date_to_scrape = datetime(2018, 1, 1)

    # NOTE: code required same number of tokens as threads
    diffbot_tokens = ['bf78571f7f6f8cdc925753a79be5c419', '75348af71a165842ee80888487723ae9', 'e245d561ac6983304fdfcafd03e0bbbe', '06998e90bba80aea92e26dfbbbda92cb']
    # NOTE: token values kept here: https://docs.google.com/document/d/1zJ19PTb3usESTN1aLWkGlvHmz6cFCGgskFANoMPGbnA/edit?usp=sharing

    threads = list()
    for index in range(12):
        #logging.info("Main    : create and start thread %d.", index)
        fb_pages_fileName = 'FB_page_names_' + str(index + 1) + '.csv'
        x = threading.Thread(target=scrape, args=(fb_pages_fileName, last_date_to_scrape, keyword_list, diffbot_tokens, index,))
        threads.append(x)
        x.start()

    for index, thread in enumerate(threads):
        #logging.info("Main    : before joining thread %d.", index)
        thread.join()
        #logging.info("Main    : thread %d done", index)
