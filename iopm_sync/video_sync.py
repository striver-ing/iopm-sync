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

class VideoSync(ArticleSync):
    def __init__(self):
        super(VideoSync, self).__init__('video_news')

    @tools.log_function_time
    def deal_video_article(self, video_news_list):
        '''
        @summary:处理视频
        ---------
        @param video_news_list:
        # video_news:
           {
                "time_length":null,
                "praise_count":null,
                "summary":"",
                "record_time":"2017-12-21 12:04:57",
                "image_url":"http://pic9.qiyipic.com/image/20171219/1f/45/v_114319672_m_601_180_101.jpg",
                "content":"",
                "url":"http://www.iqiyi.com/v_19rrf2oqlk.html",
                "play_count":null,
                "release_time":"2017-12-21 00:00:00",
                "author":"",
                "site_name":"爱奇艺",
                "uuid":"cd10f66a-6bc8-3727-86da-55de8b7e4c0a",
                "title":"晚间新闻20171219",
                "domain":"iqiyi.com",
                "comment_count":null
            }
        ---------
        @result:
        '''
        article_infos = []
        max_record_time = ''

        for video_news in video_news_list:
            news = video_news.get('_source')
            # 组装article的值
            article_info = self.get_article_info()
            # 使用news信息
            article_info['TITLE'] = news.get('title')
            article_info['CONTENT'] = ''
            article_info['HOST'] = news.get('domain')
            article_info['RELEASE_TIME'] = news.get('release_time')
            article_info['RECORD_TIME'] = news.get('record_time')

            # article_info['RELEASE_TIME'] = tools.get_current_date()
            article_info['URL'] = news.get('url')
            article_info['UUID'] = news.get('uuid')
            article_info['WEBSITE_NAME'] = news.get('site_name')
            article_info['AUTHOR'] = news.get('author')
            article_info['INFO_TYPE'] = 8
            article_info['ID'] = news.get('uuid')
            article_info['SUMMARY'] = news.get('title')
            article_info['IMAGE_URL'] = news.get('image_url')

            article_infos.append(article_info)

            max_record_time = news.get('record_time')

        self.deal_article(article_infos)
        self.record_now_record_time(max_record_time)

if __name__ == '__main__':
    video_sync = VideoSync()
    while True:
        video_news_list = video_sync.get_article()
        # print(video_news_list)
        if not video_news_list:
            log.debug('同步数据到最新 sleep %ds ...'%SLEEP_TIME)
            tools.delay_time(SLEEP_TIME)
        else:
            video_sync.deal_video_article(video_news_list)