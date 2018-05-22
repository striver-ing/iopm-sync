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

def get_data(text, record_time, release_time_begin, release_time_end):

    body = {
        "size":1,
          "query": {
            "filtered": {
              "query": {
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
                        },
                        {
                            "multi_match": {
                                "query": text,
                                "fields": [
                                    "WEBSITE_NAME"
                                ],
                                "operator": "or",
                                "minimum_should_match": "{percent}%".format(percent = int(0.5 * 100)) # 匹配到的关键词占比
                            }
                        }
                    ]
                }
              }
            }
          },
          "sort": [{
                "RECORD_TIME": "asc"
          }],
          "_source": [
                "ID",
                "TITLE",
                "WEBSITE_NAME",
                "RECORD_TIME"
          ],
        }

    # 默认按照匹配分数排序
    hots = es.search('tab_iopm_hot_info', body)
    # print(tools.dumps_json(hots))

    return hots.get('hits', {}).get('hits', [])

def update_hot(datas):
    record_time = ''
    for data in datas:
        data = data.get('_source')
        data_id = data['ID']
        data_title = data['TITLE']
        record_time = data['RECORD_TIME']
        print(data_title, record_time)
        es.update_by_id('tab_iopm_hot_info', data_id, {"HOT": 0, "WEIGHT":0})
        # es.update_by_id('tab_iopm_article_info', data_id, {"HOT": 0, "WEIGHT":0})
        # es.update_by_id('tab_iopm_hot_week_info', data_id, {"HOT": 0, "WEIGHT":0})

    return record_time

if __name__ == '__main__':
    text = '中国网'
    record_time = '2000-01-01 00:00:00'
    release_time_begin = '2018-05-07 00:00:00'
    release_time_end = tools.get_current_date()
    while True:
        datas = get_data(text, record_time, release_time_begin, release_time_end)
        if not datas:
            break
        # print(data)
        record_time = update_hot(datas)
