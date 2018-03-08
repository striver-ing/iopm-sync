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
from utils.log import log

YQTJ = tools.get_conf_value('config.conf', 'elasticsearch', 'yqtj')
IOPM_SERVICE_ADDRESS = tools.get_conf_value('config.conf', 'iopm_service', 'address') + 'related_sort'

class UpdateWeight():
    """docstring for UpdateWeight"""
    def __init__(self):
        self._yqtj_es = ES(YQTJ)

    def get_articles(self, table, record_time, release_time_begin, release_time_end):
        body = {
            "query": {
                "filtered": {
                  "filter": {
                      "bool":{
                            "must":[
                                {
                                    "range": {
                                       "RECORD_TIME": { # 查询大于该csr_res_id 的信息
                                            "gt": record_time
                                        }
                                    }
                                },
                                {
                                   "range": {
                                       "RELEASE_TIME" : {
                                            "gte": release_time_begin,
                                            "lte": release_time_end
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }

            },
            "size":1500,
            "sort":[{"RECORD_TIME":"asc"}]
        }

        print(tools.dumps_json(body))

        article = self._yqtj_es.search(table, body)
        return article.get('hits', {}).get('hits', [])

    def update_article_weight(self, articles):
        release_time = ''
        for article in articles:
            article_info = article.get('_source')
            if article_info['WEIGHT'] == 0:
                continue

            data = {
                'article_id': article_info['ID'], # 文章id
                'clues_ids' :article_info['CLUES_IDS'], # 线索ids
                'may_invalid': 0,  #是否可能无效（微博包含@ 或者#）
                'vip_count' : article_info['IS_VIP'], # 主流媒体数
                'negative_emotion_count': article_info['EMOTION'],   # 负面情感数
                'zero_ids':article_info['ZERO_ID']
            }
            print(article_info["TITLE"])
            print(article_info["RELEASE_TIME"])

            result = tools.get_json_by_requests(IOPM_SERVICE_ADDRESS, data = data)
            weight = result.get('weight', 0)# * weight_factor 没有考虑到地域
            tools.print_one_line("修改相关度 %s -> %s"%(article_info['WEIGHT'], weight))

            if self._yqtj_es.update_by_id('tab_iopm_article_info', article_info['ID'], {"WEIGHT": weight}):
                release_time, record_time = article_info["RELEASE_TIME"], article_info["RECORD_TIME"]

        return release_time, record_time

    def update_hot_weight(self, articles):
        release_time = ''
        for article in articles:
            article_info = article.get('_source')
            if article_info['WEIGHT'] == 0:
                continue

            data = {
                'hot_id': article_info['ID'], # 文章id
                'hot_value' :article_info['HOT'], # 热度值
                'clues_ids': article_info['CLUES_IDS'],  #相关舆情匹配到的线索id
                'article_count' : article_info['ARTICLE_COUNT'], # 文章总数
                'vip_count': article_info["VIP_COUNT"],   # 主流媒体数
                'negative_emotion_count': article_info["NEGATIVE_EMOTION_COUNT"],  # 负面情感数
                'zero_ids':article_info['ZERO_ID']
            }
            print('''
                release_time %s
                record_time  %s
                '''%(article_info["RELEASE_TIME"], article_info["RECORD_TIME"]))

            result = tools.get_json_by_requests(IOPM_SERVICE_ADDRESS, data = data)
            weight = result.get('weight', 0)# * weight_factor 没有考虑到地域
            tools.print_one_line("修改相关度 %s -> %s"%(article_info['WEIGHT'], weight))

            if self._yqtj_es.update_by_id('tab_iopm_hot_info', article_info['ID'], {"WEIGHT": weight}):
                record_time = article_info['RECORD_TIME']

        return record_time


def update_hot(release_time_begin, release_time_end):
    # 更新热点
    print('更新热点权重')
    update_weight = UpdateWeight()
    record_time = '2000-01-01 00:00:00'
    while True:
        articles = update_weight.get_articles('tab_iopm_hot_info', record_time, release_time_begin, release_time_end)
        if not articles:
            print('无数据 退出')
            break

        record_time = update_weight.update_hot_weight(articles)

def update_article(release_time_begin, release_time_end):
    print('更新文章权重')
    update_weight = UpdateWeight()
    record_time = '2000-01-01 00:00:00'
    while True:
        articles = update_weight.get_articles('tab_iopm_article_info', record_time, release_time_begin, release_time_end)
        if not articles:
            print('无数据 退出')
            break

        record_time = update_weight.update_article_weight(articles)

if __name__ == '__main__':
    release_time_begin = '2018-03-05 00:00:00'
    release_time_end = tools.get_current_date()
    # release_time_end = '2018-03-05 00:00:00'
    update_hot(release_time_begin, release_time_end)
    # update_article(release_time_begin, release_time_end)


