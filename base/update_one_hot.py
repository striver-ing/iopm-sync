#!/usr/bin/env python
# _#_ coding:utf-8 _*_

"""
#获取一年中每个星期的起始时间
#author:yqj@fccs.com
"""
import sys
sys.path.append('..')
import init

from db.elastic_search import ES
import utils.tools as tools

es = ES()

def get_data(text):

    body = {
        "size":1,
          "query": {
            "filtered": {
              "query": {
                "multi_match": {
                    "query": text,
                    "fields": [
                        "TITLE"
                    ],
                    "operator": "or",
                    "minimum_should_match": "{percent}%".format(percent = int(0.5 * 100)) # 匹配到的关键词占比
                }
              }
            }
          },
          "_source": [
                "ID",
                "TITLE",
          ],
        }

    # 默认按照匹配分数排序
    hots = es.search('tab_iopm_hot_info', body)
    # print(tools.dumps_json(hots))

    return hots.get('hits', {}).get('hits', [])

def update_hot(data):
    data = data[0].get('_source')
    data_id = data['ID']
    data_title = data['TITLE']
    print(data_title)
    es.update_by_id('tab_iopm_hot_info', data_id, {"HOT": 0})
    es.update_by_id('tab_iopm_article_info', data_id, {"HOT": 0, "WEIGHT":0})

if __name__ == '__main__':
    text = '招商中證銀行B(150250)'
    data = get_data(text)
    print(data)
    # update_hot(data)
