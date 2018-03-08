import sys
sys.path.append('..')
import init

import utils.tools as tools
from db.elastic_search import ES
from utils.log import log

YQTJ = tools.get_conf_value('config.conf', 'elasticsearch', 'yqtj')
IOPM_SERVICE_ADDRESS = tools.get_conf_value('config.conf', 'iopm_service', 'address') + 'related_sort'
# IOPM_SERVICE_ADDRESS = 'http://localhost:8080/'+'related_sort'
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
                    "minimum_should_match": "{percent}%".format(percent = int(0.95 * 100)) # 匹配到的关键词占比
                }
              }
            }
          }#,
          # "_source": [
          #       "ID",
          #       "TITLE",
          # ],
        }

    # 默认按照匹配分数排序
    hots = es.search('tab_iopm_hot_info', body)

    return hots.get('hits', {}).get('hits', [])

def update_weight(data):
    data = data[0].get('_source')
    print(tools.dumps_json(data))
    body = {
        'hot_id': data['ID'], # 文章id
        'hot_value' :data['HOT'], # 热度值
        'clues_ids': data['CLUES_IDS'],  #相关舆情匹配到的线索id
        'article_count' : data['ARTICLE_COUNT'], # 文章总数
        'vip_count': data["VIP_COUNT"],   # 主流媒体数
        'negative_emotion_count': data["NEGATIVE_EMOTION_COUNT"],  # 负面情感数
        'zero_ids':data['ZERO_ID']
    }
    print('''
        title        %s
        release_time %s
        record_time  %s
        '''%(data["TITLE"], data["RELEASE_TIME"], data["RECORD_TIME"]))


    print(tools.dumps_json(body))

    result = tools.get_json_by_requests(IOPM_SERVICE_ADDRESS, data = body)
    weight = result.get('weight', 0)# * weight_factor 没有考虑到地域
    tools.print_one_line("修改相关度 %s -> %s"%(data['WEIGHT'], weight))

    # if es.update_by_id('tab_iopm_hot_info', data['ID'], {"WEIGHT": weight}):
    #     record_time = data['RECORD_TIME']

if __name__ == '__main__':
    # text = '开弹幕、看直播、忙点赞国外网友原来是这样看两会的'
    text = '中央台：精布局细打磨和声浓郁'
    text = '这么多大明星，也救不了浙江卫视综艺的尴尬'
    data = get_data(text)
    update_weight(data)
