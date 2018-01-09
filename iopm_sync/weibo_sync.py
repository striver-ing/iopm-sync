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
import pid
pid.record_pid(__file__)

import utils.tools as tools
from utils.log import log
from base.article_sync import ArticleSync

SLEEP_TIME = int(tools.get_conf_value('config.conf', 'sync', 'sleep_time'))

class WeiboSync(ArticleSync):
    def __init__(self):
        super(WeiboSync, self).__init__('weibo_article')

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
            article_info = self.get_article_info()
            # 使用weibo信息
            article_info['TITLE'] = ''
            article_info['CONTENT'] = weibo.get('content')
            article_info['HOST'] = 'weibo.cn'
            article_info['RELEASE_TIME'] = weibo.get('release_time')
            article_info['RECORD_TIME'] = weibo.get('record_time')
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

        self.deal_article(article_infos)
        self.record_now_record_time(max_record_time)

if __name__ == '__main__':
    weibo_sync = WeiboSync()
    while True:
        weibo_article_list = weibo_sync.get_article()
        # print(weibo_article_list)
        if not weibo_article_list:
            log.debug('同步数据到最新 sleep %ds ...'%SLEEP_TIME)
            tools.delay_time(SLEEP_TIME)
        else:
            weibo_sync.deal_weibo_article(weibo_article_list)
