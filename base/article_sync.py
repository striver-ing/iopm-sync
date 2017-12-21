# -*- coding: utf-8 -*-
'''
Created on 2017-12-11 15:13
---------
@summary: 同步article 通用
---------
@author: Administrator
'''
import sys
sys.path.append('..')
import init

import utils.tools as tools
from db.elastic_search import ES
from base.compare_keywords import CompareKeywords
from word_cloud.word_cloud import WordCloud
from base.hot_sync import HotSync
from base.vip_checked import VipChecked
from summary.summary import Summary
from emotion.emotion import Emotion
from utils.log import log

ADDRESS = tools.get_conf_value('config.conf', 'elasticsearch', 'data-pool')
SYNC_TIME_FILE = 'iopm_sync/sync_time.txt'
IOPM_SERVICE_ADDRESS = 'http://localhost:8080/'
SLEEP_TIME = int(tools.get_conf_value('config.conf', 'sync', 'sleep_time'))

class ArticleSync():
    def __init__(self, table):
        self._record_time = tools.get_json(tools.read_file(SYNC_TIME_FILE)) or {}
        self._compare_keywords = CompareKeywords()
        self._summary = Summary()
        self._emotion = Emotion()
        self._word_cloud = WordCloud()
        self._es = ES()
        self._hot_sync = HotSync()
        self._vip_checked = VipChecked()
        self._table = table
        self._per_record_time_key = '{table}_record_time'.format(table = self._table)

    def get_article_info(self):
        '''
        @summary: 取article的结构信息
        ---------
        ---------
        @result:
        '''

        article_info = {
            "EMOTION": None,
            "HOST":"",
            "AUTHOR":"",
            "URL":"",
            "WEBSITE_NAME":"",
            "ACCOUNT":"",
            "REVIEW_COUNT":None,
            "KEYWORDS_COUNT":None,
            "RELEASE_TIME":"",
            "CONTENT":"",
            "ID":None,
            "UUID":"",
            "WEIGHT":None,
            "CLUES_IDS":"",
            "UP_COUNT":None,
            "INTERACTION_COUNT":None,
            "RECORD_TIME":tools.get_current_date(),
            "COMMENT_COUNT":None,
            "IS_VIP":None,
            "INFO_TYPE":None,
            "HOT_ID":None,
            "KEYWORD_CLUES_ID":"",
            "MAY_INVALID":None,
            "TITLE":"",
            "KEYWORDS":"",
            "TRANSMIT_COUNT":None,
            "ZERO_ID":None,
            "FIRST_ID":None,
            "SECOND_ID":None,
            "SUMMARY":"",
            "WORD_CLOUD":"",
            "IMAGE_URL":""
        }

        return article_info

    def get_article_clues_src(self):
        article_clues_src = {
            "CLUES_ID": "",
            "ARTICLE_ID": "",
            "ID": ""
        }

        return article_clues_src

    def get_per_record_time(self):
        per_record_time = self._record_time.get(self._per_record_time_key)

        return per_record_time

    def record_now_record_time(self, record_time):
        self._record_time[self._per_record_time_key] = record_time
        tools.write_file(SYNC_TIME_FILE, tools.dumps_json(self._record_time))

    def get_article(self):
        '''
        @summary: 目前取的是record_time 为了保证有数据， 正常应该取releast_time TODO
        ---------
        ---------
        @result:
        '''

        per_record_time = self.get_per_record_time()

        today_time = tools.get_current_date('%Y-%m-%d')
        if per_record_time:
            sql = "select * from {table} where record_time > '{record_time}' and release_time >= '{today_time} 00:00:00' and release_time <= '{today_time} 23:59:59' order by record_time".format(table = self._table, record_time = per_record_time, today_time = today_time)
        else:
            sql = "select * from {table} where release_time >= '{today_time} 00:00:00' and release_time <= '{today_time} 23:59:59' order by record_time".format(table = self._table, today_time = today_time)

        url = 'http://{address}/_sql?sql={sql}'.format(address = ADDRESS, sql = sql)
        log.debug(url)

        article = tools.get_json_by_requests(url)
        return article.get('hits', {}).get('hits', [])


    def deal_article(self, article_list):
        '''
        @summary:处理article
        ---------
        @param article_list:
        ---------
        @result:
        '''
        article_infos = []
        # 补全剩余的信息
        for article_info in article_list:
            # 互动量
            # print(tools.dumps_json(article_info))
            article_info['INTERACTION_COUNT'] = (article_info['UP_COUNT'] or 0) + (article_info['TRANSMIT_COUNT'] or 0) + (article_info['REVIEW_COUNT'] or 0) + (article_info['COMMENT_COUNT'] or 0)

            # 线索关键词比对
            del_tag_content = tools.del_html_tag(article_info['CONTENT'])
            text = article_info['TITLE'] + del_tag_content
            # print(text)
            keywords, clues_ids, zero_ids, first_id, second_ids, keyword_clues = self._compare_keywords.get_contained_keys(text)

            article_info['KEYWORDS'] = keywords
            article_info['CLUES_IDS'] = clues_ids
            article_info['ZERO_ID'] = zero_ids
            article_info['FIRST_ID'] = first_id
            article_info['SECOND_ID'] = second_ids
            article_info['KEYWORDS_COUNT'] = len(keyword_clues)
            article_info['KEYWORD_CLUES_ID'] = str(keyword_clues)

            # 线索与舆情中间表
            article_clues_srcs = []
            if clues_ids:
                for clues_id in clues_ids.split(','):
                    article_clues_src = self.get_article_clues_src()
                    article_clues_src['ID'] =  tools.get_uuid(clues_id, article_info['ID'])
                    article_clues_src['CLUES_ID'] =  clues_id
                    article_clues_src['ARTICLE_ID'] = article_info['ID']

                    article_clues_srcs.append(article_clues_src)
                    self._es.add_batch(article_clues_srcs, "ID", 'tab_iopm_article_clues_src')

            # 情感分析 (1 正 2 负 3 中立， 百度：0:负向，1:中性，2:正向)
            emotion = self._emotion.get_emotion(del_tag_content)
            if emotion == 0:
                emotion = 2

            elif emotion == 1:
                emotion = 3

            elif emotion == 2:
                emotion = 1

            else:
                emotion = 3

            article_info['EMOTION'] = emotion

            # 主流媒体
            is_vip = self._vip_checked.is_vip(article_info['URL']) or self._vip_checked.is_vip(article_info['WEBSITE_NAME'])
            article_info["IS_VIP"] = is_vip

            # 计算相关度
            if article_info['CLUES_IDS']:
                url = IOPM_SERVICE_ADDRESS + 'related_sort'
                data = {
                    'article_id': article_info['ID'], # 文章id
                    'clues_ids' :article_info['CLUES_IDS'], # 线索ids
                    'may_invalid': 0,  #是否可能无效（微博包含@ 或者#）
                    'vip_count' : article_info['IS_VIP'], # 主流媒体数
                    'negative_emotion_count': 1 if article_info['EMOTION'] == 2 else 0,   # 负面情感数
                    'zero_ids':article_info['ZERO_ID']
                }

                result = tools.get_json_by_requests(url, data = data)
                article_info['WEIGHT'] = result.get('weight', 0)
            else:
                article_info['WEIGHT'] = 0


            # 词语图
            word_cloud = self._word_cloud.get_word_cloud(del_tag_content)
            article_info['WORD_CLOUD'] = tools.dumps_json(word_cloud)

            # 摘要
            if not article_info['SUMMARY']:
                article_info['SUMMARY'] = self._summary.get_summary(del_tag_content)

            # 统计相似文章 热点
            if article_info['INFO_TYPE'] == 3: # 微博
                article_info['TITLE']  = article_info['SUMMARY'][:30]

            article_info['HOT_ID'] = self._hot_sync.get_hot_id(article_info)

            log.debug('''
                title         %s
                release_time  %s
                url           %s
                匹配的关键字：%s
                线索id        %s
                一级分类      %s
                二级分类      %s
                三级分类      %s
                关键词-线索   %s
                '''%(article_info['TITLE'], article_info['RELEASE_TIME'], article_info["URL"], keywords, clues_ids, zero_ids, first_id, second_ids, keyword_clues))


            # print(tools.dumps_json(article_info))
            article_infos.append(article_info)

        # article入库
        print('article入库')
        # print(tools.dumps_json(article_infos))
        self._es.add_batch(article_infos, "ID", 'tab_iopm_article_info')

if __name__ == '__main__':
    pass