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

MIN_SIMILARITY = 0.5 # 相似度阈值
IOPM_SERVICE_ADDRESS = 'http://localhost:8080/'

class HotSync():
    def __init__(self):
        self._es = ES()

    def _get_today_hots(self, text):
        body = {
          "query": {
            "filtered": {
              "filter": {
                "range": {
                   "RELEASE_TIME": { # 今日发布的数据汇聚的热点（正常应该按照release_time）TODO
                        "gt": tools.get_current_date('%Y-%m-%d') + ' 00:00:00' # 今日
                    }
                    # "RECORD_TIME": { # 今日爬取到的数据汇聚的热点
                    #   "gt": tools.get_current_date('%Y-%m-%d') + ' 00:00:00' # 今日
                    # }
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
                "CONTENT",
                "HOT",
                "CLUES_IDS",
                "VIP_COUNT",
                "NEGATIVE_EMOTION_COUNT"
          ],
          "highlight": {
                "fields": {
                    "TITLE": {}
                }
          }
        }

        # 默认按照匹配分数排序
        hots = self._es.search('tab_iopm_hot_info', body)
        # print(tools.dumps_json(hots))

        return hots.get('hits', {}).get('hits', [])


    def get_hot_id(self, article_info):
        article_text = article_info.get("TITLE")# + article_info.get("CONTENT")
        article_text = tools.del_html_tag(article_text)

        hots = self._get_today_hots(article_text)

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
            if similar_hot["ID"] != article_info["ID"]: # 防止同一个舆情 比较多次
                data = {}

                # 更新热点的热度
                data["HOT"] = similar_hot["HOT"] + 1

                # 更新主流媒体数量及负面舆情数量
                data["VIP_COUNT"] = similar_hot['VIP_COUNT'] + (1 if article_info["IS_VIP"] else 0)
                data["NEGATIVE_EMOTION_COUNT"] = similar_hot['NEGATIVE_EMOTION_COUNT'] + (1 if article_info['EMOTION'] == 2 else 0)

                # 更新相关度
                if similar_hot['CLUES_IDS']:
                    url = IOPM_SERVICE_ADDRESS + 'related_sort'
                    data_args = {
                        'hot_id': similar_hot['ID'], # 文章id
                        'hot_value' :data['HOT'], # 热度值
                        'clues_id': similar_hot['CLUES_IDS'],  #相关舆情匹配到的线索id
                        'article_count' : data['HOT'], # 文章总数
                        'vip_count': data["VIP_COUNT"],   # 主流媒体数
                        'negative_emotion_count': data["NEGATIVE_EMOTION_COUNT"],  # 负面情感数
                        'zero_ids':article_info['ZERO_ID']
                    }

                    result = tools.get_json_by_requests(url, data = data_args)
                    if result:
                        data['WEIGHT'] = result.get('weight', 0)

                # 更新热点
                self._es.update_by_id("tab_iopm_hot_info", data_id = similar_hot.get("ID"), data = data)

            # 返回热点id
            return similar_hot.get("ID")
        else:
            # 将该舆情添加为热点
            hot_info = deepcopy(article_info)
            hot_info.pop('HOT_ID') # 热点表中无hot_id

            # 默认用户行为数量为零
            hot_info['ACCEPT_COUNT'] = 0
            hot_info['UNACCEPT_COUNT'] = 0
            hot_info['WATCH_COUNT'] = 0

            # 其他值
            hot_info['VIP_COUNT'] = 1 if article_info["IS_VIP"] else 0
            hot_info['NEGATIVE_EMOTION_COUNT'] = 1 if article_info['EMOTION'] == 2 else 0

            hot_info['HOT'] = 1
            hot_info['ID'] = article_info.get("ID")

            self._es.add('tab_iopm_hot_info', hot_info, data_id = hot_info['ID'])

            # 返回热点id
            return hot_info['ID']

if __name__ == '__main__':
    hot_sync = HotSync()
    hot_sync._get_today_hots('直播')