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
from base.article_sync import ArticleSync

SLEEP_TIME = int(tools.get_conf_value('config.conf', 'sync', 'sleep_time'))

class NewsSync(ArticleSync):
    def __init__(self):
        super(NewsSync, self).__init__('news_article')

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
            article_info = self.get_article_info()
            # 使用news信息
            article_info['TITLE'] = news.get('title')
            article_info['CONTENT'] = news.get('content')
            article_info['HOST'] = news.get('domain')
            article_info['RELEASE_TIME'] = news.get('release_time')
            article_info['RECORD_TIME'] = news.get('record_time')

            # article_info['RELEASE_TIME'] = tools.get_current_date()
            article_info['URL'] = news.get('url')
            article_info['UUID'] = news.get('uuid')
            article_info['WEBSITE_NAME'] = news.get('website')
            article_info['AUTHOR'] = news.get('author')
            article_info['INFO_TYPE'] = 1
            article_info['ID'] = news.get('uuid')

            article_infos.append(article_info)

            max_record_time = news.get('record_time')

        self.deal_article(article_infos)
        self.record_now_record_time(max_record_time)

if __name__ == '__main__':
    news_sync = NewsSync()
    while True:
        news_article_list = news_sync.get_article()
        # print(news_article_list)
        if not news_article_list:
            log.debug('同步数据到最新 sleep %ds ...'%SLEEP_TIME)
            tools.delay_time(SLEEP_TIME)
        else:
            news_sync.deal_news_article(news_article_list)