# -*- coding: utf-8 -*-
'''
Created on 2017-12-11 15:13
---------
@summary: 同步新闻
---------
@author: Administrator
'''
import sys
sys.path.append('..')
import init

import utils.tools as tools
from utils.log import log
from db.elastic_search import ES
from base.article_sync import ArticleSync

ADDRESS = tools.get_conf_value('config.conf', 'elasticsearch', 'data-pool')
IOPM_SERVICE_ADDRESS = tools.get_conf_value('config.conf', 'iopm_service', 'address')
SLEEP_TIME = int(tools.get_conf_value('config.conf', 'sync', 'sleep_time'))
SYNC_TIME_FILE = 'iopm_sync/sync_time.txt'

class NewsSync():
    def __init__(self):
        self._record_time = tools.get_json(tools.read_file(SYNC_TIME_FILE)) or {}
        self._es = ES()
        self._article_sync = ArticleSync()

    def _get_per_record_time(self):
        news_record_time = ''
        news_record_time = self._record_time.get('news_record_time')

        return news_record_time

    def _record_now_record_time(self, record_time):
        self._record_time['news_record_time'] = record_time
        tools.write_file(SYNC_TIME_FILE, tools.dumps_json(self._record_time))

    @tools.log_function_time
    def get_news_article(self):
        '''
        @summary: 目前取的是record_time 为了保证有数据， 正常应该取releast_time TODO
        ---------
        ---------
        @result:
        '''

        news_record_time = self._get_per_record_time()

        today_time = tools.get_current_date('%Y-%m-%d')
        if news_record_time:
            sql = "select * from news_article where record_time > '{record_time}' and release_time >= '{today_time} 00:00:00' and release_time <= '{today_time} 23:59:59' order by record_time".format(record_time = news_record_time, today_time = today_time)
        else:
            sql = "select * from news_article where release_time >= '{today_time} 00:00:00' and release_time <= '{today_time} 23:59:59' order by record_time".format(today_time = today_time)

        url = 'http://{address}/_sql?sql={sql}'.format(address = ADDRESS, sql = sql)
        log.debug(url)

        news = tools.get_json_by_requests(url)
        return news.get('hits', {}).get('hits', [])

    @tools.log_function_time
    def deal_news_article(self, news_article_list):
        '''
        @summary:处理新闻
        ---------
        @param news_article_list:
        # news:
           {
                'author': '',
                'content': '',
                'domain': '31ly.com',
                'position': 11,
                'record_time': '2017-12-10 20:25:10',
                'release_time': '2017-08-24 00:00:00',
                'title': '国家粮食局关于做好政策性粮食销售出库监管工作的紧急通知',
                'url': 'http://31ly.com/news/detail-20170824-110341.html',
                'uuid': '59eecc96-ebe7-3f08-b669-ce60dcc91b84',
                'website': '中国粮油网'
            }
        ---------
        @result:
        '''
        article_infos = []
        max_record_time = ''

        for news_article in news_article_list:
            news = news_article.get('_source')
            # 组装article的值
            article_info = self._article_sync.get_article_info()
            # 使用news信息
            article_info['TITLE'] = news.get('title')
            article_info['CONTENT'] = news.get('content')
            article_info['HOST'] = news.get('domain')
            article_info['RELEASE_TIME'] = news.get('release_time')

            # article_info['RELEASE_TIME'] = tools.get_current_date()
            article_info['URL'] = news.get('url')
            article_info['UUID'] = news.get('uuid')
            article_info['WEBSITE_NAME'] = news.get('website')
            article_info['AUTHOR'] = news.get('author')
            article_info['INFO_TYPE'] = 1
            article_info['ID'] = news.get('uuid')

            article_infos.append(article_info)

            max_record_time = news.get('record_time')

        self._article_sync.deal_article(article_infos)
        self._record_now_record_time(max_record_time)

if __name__ == '__main__':
    while True:
        news_sync = NewsSync()
        news_article_list = news_sync.get_news_article()
        # print(news_article_list)
        if not news_article_list:
            log.debug('同步数据到最新 sleep %ds ...'%SLEEP_TIME)
            tools.delay_time(SLEEP_TIME)
        else:
            news_sync.deal_news_article(news_article_list)