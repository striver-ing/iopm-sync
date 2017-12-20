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
from db.elastic_search import ES

ADDRESS = tools.get_conf_value('config.conf', 'elasticsearch', 'data-pool')
SYNC_TIME_FILE = 'iopm_sync/sync_time.txt'

class NewsSync():
    def __init__(self):
        self._record_time = tools.read_file(SYNC_TIME_FILE) or {}

    def _get_per_record_time(self):
        news_record_time = ''
        news_record_time = tools.get_json(self._record_time).get('news_record_time')

        return news_record_time

    def _record_now_record_time(self, record_time):
        self._record_time['news_record_time'] = record_time
        tools.write_file(SYNC_TIME_FILE, tools.dumps_json(self._record_time))


    def get_news_article(self):
        news_record_time = self._get_per_record_time()
        if news_record_time:
            sql = 'select * from news_article where record_time > {record_time} order by record_time'.format(record_time = news_record_time)
        else:
            sql = 'select * from news_article order by record_time limit 1'

        url = 'http://{address}/_sql?sql={sql}'.format(address = ADDRESS, sql = sql)
        print(url)

        news = tools.get_json_by_requests(url)
        return news.get('hits', {}).get('hits', [])

    def deal_news_article(self, news_article_list):
        '''
        @summary:
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
        for news_article in news_article_list:
            news = news_article.get('_source')
            # 线索关键词比对
            # 计算相关度
            # 情感分析
            # 统计相似文章
            # 入库



if __name__ == '__main__':
    news_sync = NewsSync()
    news_article_list = news_sync.get_news_article()
    # print(news_article_list)
    news_sync.deal_news_article(news_article_list)