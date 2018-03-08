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
from cluster.compare_text import compare_text
from copy import deepcopy
import random
from base.event_filter import EventFilter

MIN_SIMILARITY = 0.5 # 相似度阈值
IOPM_SERVICE_ADDRESS = 'http://localhost:8080/'

class HotWeekSync():
    def __init__(self):
        self._es = ES()
        self._event_filter = EventFilter()
        self._event_filter.start()

    def _get_week_hots(self, text, release_time):
        before_week = tools.get_before_date(release_time, -7)

        body = {
        "size":1,
          "query": {
            "filtered": {
              "filter": {
                "range": {
                   "RELEASE_TIME": { # 当日发布的新闻
                        "gte": before_week,
                        "lte": release_time
                    }
                }
              },
              "query": {
                "multi_match": {
                    "query": text,
                    "fields": [
                        "TITLE"
                    ],
                    "operator": "or",
                    "minimum_should_match": "{percent}%".format(percent = int(MIN_SIMILARITY * 100)) # 匹配到的关键词占比
                }
              }
            }
          },
          "_source": [
                "ID",
                "TITLE",
                # "CONTENT",
                "HOT",
                "ARTICLE_COUNT",
                "VIP_COUNT",
                "NEGATIVE_EMOTION_COUNT",
                "HOT_DAY_IDS",
                "WEIGHT"
          ],
          # "highlight": {
          #       "fields": {
          #           "TITLE": {}
          #       }
          # }
        }

        # 默认按照匹配分数排序
        hots = self._es.search('tab_iopm_hot_week_info', body)
        # print(tools.dumps_json(hots))

        return hots.get('hits', {}).get('hits', [])


    def cluster_week_hot(self, hot, hot_value = None, article_count = None, vip_count = None, negative_emotion_count = None, weight = None):
        '''
        @summary: 聚类
        ---------
        @param hot:每日热点信息
        @param hot_value: 一条舆情的热度 （不为空时表示该条每日热点为更新热点，那么7日热点已经聚过此热点， 热度应该只加该条舆情的热度）
        @param article_count:
        @param vip_count:
        @param negative_emotion_count:
        @param weight:
        ---------
        @result:
        '''


        article_text = hot.get("TITLE")# + hot.get("CONTENT")
        release_time = hot.get("RELEASE_TIME")

        article_text = tools.del_html_tag(article_text)

        hots = self._get_week_hots(article_text, release_time)

        # 找最相似的热点
        similar_hot = None
        max_similarity = 0
        for hot_info in hots:
            hot = hot_info.get('_source')
            hot_text = hot.get('TITLE')# + hot.get('CONTENT')
            hot_text = tools.del_html_tag(hot_text)

            temp_similarity = compare_text(article_text, hot_text)
            if temp_similarity > MIN_SIMILARITY and temp_similarity > max_similarity:
                similar_hot = hot
                max_similarity = temp_similarity

            break #hots 按照匹配值排序后，第一个肯定是最相似的，无需向后比较

        if similar_hot:# 找到相似的热点
            if similar_hot["ID"] != hot["ID"]: # 防止同一个舆情 比较多次
                data = {}

                # 更新热点的热度与文章数
                data['HOT'] = similar_hot['HOT'] + (hot_value or hot.get('HOT'))
                data["ARTICLE_COUNT"] = similar_hot["ARTICLE_COUNT"] + (article_count or hot.get('ARTICLE_COUNT'))

                # 更新主流媒体数量及负面舆情数量
                data["VIP_COUNT"] = similar_hot['VIP_COUNT'] + (vip_count or hot.get('VIP_COUNT'))
                data["NEGATIVE_EMOTION_COUNT"] = similar_hot['NEGATIVE_EMOTION_COUNT'] + (negative_emotion_count or hot.get('NEGATIVE_EMOTION_COUNT'))

                # 更新相关度
                data['WEIGHT'] = similar_hot['WEIGHT'] + (weight or hot['WEIGHT'])

                # 更新 hot_day_ids
                if not hot_value:
                    data["HOT_DAY_IDS"] = similar_hot['HOT_DAY_IDS'] + ',' + hot['ID']

                # 更新热点
                self._es.update_by_id("tab_iopm_hot_week_info", data_id = similar_hot.get("ID"), data = data)

            # 返回热点id
            return similar_hot.get("ID")
        else:
            # 将该舆情添加为热点
            hot_info = deepcopy(hot)

            # 处理热点类型
            del_tag_content = tools.del_html_tag(hot_info['CONTENT'])
            text = article_info['TITLE'] + del_tag_content
            contain_event_ids = self._event_filter.find_contain_event(text)

            hot_info['EVENT_IDS'] = ','.join(contain_event_ids)
            hot_info['HOT_DAY_IDS'] = hot.get("ID")

            self._es.add('tab_iopm_hot_week_info', hot_info, data_id = hot_info['ID'])

            # 返回热点id
            return hot_info['ID']

if __name__ == '__main__':
    hot_sync = HotWeekSync()