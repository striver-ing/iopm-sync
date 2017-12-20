# -*- coding: utf-8 -*-
'''
Created on 2017-12-11 15:13
---------
@summary: 同步微博
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

class WeiboSync():
    def __init__(self):
        self._record_time = tools.get_json(tools.read_file(SYNC_TIME_FILE)) or {}
        self._es = ES()
        self._article_sync = ArticleSync()

    def _get_per_record_time(self):
        weibo_record_time = ''
        weibo_record_time = self._record_time.get('weibo_record_time')

        return weibo_record_time

    def _record_now_record_time(self, record_time):
        self._record_time['weibo_record_time'] = record_time
        tools.write_file(SYNC_TIME_FILE, tools.dumps_json(self._record_time))

    @tools.log_function_time
    def get_weibo_article(self):
        '''
        @summary: 目前取的是record_time 为了保证有数据， 正常应该取releast_time TODO
        ---------
        ---------
        @result:
        '''

        weibo_record_time = self._get_per_record_time()

        today_time = tools.get_current_date('%Y-%m-%d')
        if weibo_record_time:
            sql = "select * from weibo_article where record_time > '{record_time}' and release_time >= '{today_time} 00:00:00' and release_time <= '{today_time} 23:59:59' order by record_time".format(record_time = weibo_record_time, today_time = today_time)
        else:
            sql = "select * from weibo_article where release_time >= '{today_time} 00:00:00' and release_time <= '{today_time} 23:59:59' order by record_time".format(today_time = today_time)

        url = 'http://{address}/_sql?sql={sql}'.format(address = ADDRESS, sql = sql)
        log.debug(url)

        weibo = tools.get_json_by_requests(url)
        return weibo.get('hits', {}).get('hits', [])

    @tools.log_function_time
    def deal_weibo_article(self, weibo_article_list):
        '''
        @summary:处理新闻
        ---------
        @param weibo_article_list:
        # weibo:
             {
            "url": "https://m.weibo.cn/status/4185627450396574",
            "content": "",
            "user_name": "我的胖影啊",
            "up_count": 1,
            "release_time": "2017-12-16 17:00:00",
            "record_time": "2017-12-16 17:56:08",
            "video_url": "",
            "id": "4185627450396574",
            "transmit_count": 0,
            "comment_count": 0
            }
        ---------
        @result:
        '''
        article_infos = []
        max_record_time = ''

        for weibo_article in weibo_article_list:
            weibo = weibo_article.get('_source')
            # 组装article的值
            article_info = self._article_sync.get_article_info()
            # 使用weibo信息
            article_info['TITLE'] = ''
            article_info['CONTENT'] = weibo.get('content')
            article_info['HOST'] = 'weibo.cn'
            article_info['RELEASE_TIME'] = weibo.get('release_time')
            article_info['URL'] = weibo.get('url')
            article_info['UUID'] = weibo.get('id')
            article_info['WEBSITE_NAME'] = '新浪微博'
            article_info['AUTHOR'] = weibo.get('user_name')
            article_info['INFO_TYPE'] = 3
            article_info['ID'] = weibo.get('id')
            article_info['TRANSMIT_COUNT'] = weibo.get('transmit_count')
            article_info['COMMENT_COUNT'] = weibo.get('comment_count')
            article_info['UP_COUNT'] = weibo.get('up_count')

            article_infos.append(article_info)

            max_record_time = weibo.get('record_time')

        self._article_sync.deal_article(article_infos)
        self._record_now_record_time(max_record_time)

if __name__ == '__main__':
    while True:
        weibo_sync = WeiboSync()
        weibo_article_list = weibo_sync.get_weibo_article()
        # print(weibo_article_list)
        if not weibo_article_list:
            log.debug('同步数据到最新 sleep %ds ...'%SLEEP_TIME)
            tools.delay_time(SLEEP_TIME)
        else:
            weibo_sync.deal_weibo_article(weibo_article_list)