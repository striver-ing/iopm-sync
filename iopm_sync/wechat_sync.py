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

class WechatSync(ArticleSync):
    def __init__(self):
        super(WechatSync, self).__init__('wechat_article')

    @tools.log_function_time
    def deal_wechat_article(self, wechat_article_list):
        '''
        @summary:处理微信
        ---------
        @param wechat_article_list:
        {
            "summary":"",
            "like_num":2,
            "cover":"",
            "comment":[

            ],
            "record_time":"2017-11-20 22:26:32",
            "author":"",
            "source_url":"",
            "url":"",
            "read_num":2188,
            "article_id":26508963361,
            "title":"祠堂被拆迁，补偿款该归谁？",
            "content":"",
            "__biz":"MzA3NzMwODkyMQ==",
            "account":"直播绍兴",
            "release_time":"2017-11-29 19:40:22"
        }

        ---------
        @result:
        '''
        article_infos = []
        max_record_time = ''

        for wechat_article in wechat_article_list:
            wechat = wechat_article.get('_source')
            # 组装article的值
            article_info = self.get_article_info()
            # 使用wechat信息
            article_info['TITLE'] = wechat.get('title')
            article_info['CONTENT'] = wechat.get('content')
            article_info['HOST'] = "mp.weixin.qq.com"
            article_info['RELEASE_TIME'] = wechat.get('release_time')
            # article_info['RELEASE_TIME'] = tools.get_current_date() # TEST
            article_info['URL'] = wechat.get('url')
            article_info['UUID'] = wechat.get('__biz')
            article_info['WEBSITE_NAME'] = wechat.get('account')
            article_info['AUTHOR'] = wechat.get('author')
            article_info['INFO_TYPE'] = 2
            article_info['ID'] = str(wechat.get('article_id'))
            article_info['UP_COUNT'] = wechat.get('like_num')
            article_info['REVIEW_COUNT'] = wechat.get('read_num')
            article_info['COMMENT_COUNT'] = len(wechat.get('comment', []))
            article_info['IMAGE_URL'] = wechat.get('cover')
            article_info['SUMMARY'] = wechat.get('summary')

            article_infos.append(article_info)

            max_record_time = wechat.get('record_time')

        self.deal_article(article_infos)
        self.record_now_record_time(max_record_time)

if __name__ == '__main__':
    wechat_sync = WechatSync()
    while True:
        wechat_article_list = wechat_sync.get_article()
        # print(tools.dumps_json(wechat_article_list))
        if not wechat_article_list:
            log.debug('同步数据到最新 sleep %ds ...'%SLEEP_TIME)
            tools.delay_time(SLEEP_TIME)

        else:
            wechat_sync.deal_wechat_article(wechat_article_list)
