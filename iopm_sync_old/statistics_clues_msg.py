# -*- coding: utf-8 -*-
'''
Created on 2017-10-24 15:52
---------
@summary: 统计线索信息
---------
@author: Boris
'''

# -*- coding: utf-8 -*-
'''
Created on 2017-07-26 19:04
---------
@summary:
---------
@author: Boris
'''


import sys
import os
sys.path.append('../')

from db.oracledb import OracleDB
from db.elastic_search import ES
import utils.tools as tools
from utils.log import log

oracledb = OracleDB()
esdb = ES()

###########【拆分词组相关】################
def match_keys(keys_list):
    '''
    @summary: 解析乘积关系的词组
    ---------
    @param keys_list: 词组列表
    ---------
    @result:
    '''

    list_size = len(keys_list)

    if list_size < 2:
        # return ','.join(keys_list[0].split('|'))
        return keys_list[0].split('|')
    else:
        temp_keys_list = keys_list[:2] #截取前两个数组
        keys_list = keys_list[2:] #剩余的数组[[e|f]]

        keys=''
        for temp_keys1 in temp_keys_list[0].split('|'):
            for temp_keys2 in temp_keys_list[1].split('|'):
                keys += temp_keys1 + '&' + temp_keys2 + '|'

        keys = keys[:-1]
        keys_list.extend([keys])
        return match_keys(keys_list)

def match_keyword(keyword):
    '''
    @summary: 拆分乘积关系的keyword词组
    ---------
    @param keyword:关键词
    ---------
    @result:
    '''

    keywords = []
    temp_keywords = keyword.split(',')
    for keyword in temp_keywords:
        keyword = keyword.replace('（',"(").replace('）',')')
        bracket_keys_list = tools.get_info(keyword, '\((.*?)\)', allow_repeat = True) # 括号中的词组
        # print(bracket_keys_list)
        if bracket_keys_list:
            keyword = match_keys(bracket_keys_list)
            keywords.extend(keyword)
        else:
            keywords.append(keyword)

    return keywords

###############【处理中英文 格式化词组】####################
def format_keys(keywords):
    '''
    @summary: &表示与的关系 |表示或的关系，括号括起来表示一组
    ---------
    @param keywords:
    ---------
    @result:
    '''
    keywords = keywords[:-1] if keywords.endswith(',') else keywords

    keywords = keywords.replace('（','(')
    keywords = keywords.replace('）',')')
    keywords = keywords.replace(')(',')&(')

    chinese_word = tools.get_chinese_word(keywords)
    keywords = keywords.split(',')
    for i in range(len(keywords)):
        keywords[i] = keywords[i].strip()
        regex = '[a-zA-Z 0-9:]+'
        english_words = tools.get_info(keywords[i], regex, allow_repeat = True)
        while ' ' in english_words:
            english_words.remove(' ')

        for j in range(len(english_words)):
            english_words[j] = english_words[j].strip()
            if english_words[j]:
                keywords[i] = keywords[i].replace(english_words[j], '%s')

        keywords[i] = tools.replace_str(keywords[i], ' +', '&')
        keywords[i] = keywords[i]%(tuple(english_words)) if '%s' in keywords[i] else keywords[i]

    keywords = '),('.join(keywords)
    keywords = '(' + keywords + ')' if not keywords.startswith('(') and keywords else keywords

    return keywords

#################【取线索信息相关】####################
def get_clues():
    sql = 'select t.id clues_id,to_char(t.keyword1),to_char(t.keyword2),to_char(t.keyword3),t.zero_id, t.first_id, t.second_id  from TAB_IOPM_CLUES t where zero_id != 7' # 7 为传播途径
    results = oracledb.find(sql)

    clues_json = {
        "message":"查询成功",
        "status":1,
        "data":[
        {
            "clues_id":104,
            "keyword1":"",
            "keyword2":"",
            "keyword3":"",
            "zero_id":2,
            "first_id":1,
            "second_id":1
        }]
    }

    clues_json['data'] = []


    for result in results:
        data = {
            "clues_id":result[0] if result[0] else "",
            "keyword1": result[1] if result[1] else "",
            "keyword2":result[2] if result[2] else "",
            "keyword3":result[3] if result[3] else "",
            "zero_id":result[4] if result[4] else "",
            "first_id":result[5] if result[5] else "",
            "second_id":result[6] if result[6] else ""
        }

        data['keyword2'] = format_keys(data['keyword2'])
        data['keyword3'] = format_keys(data['keyword3'])
        clues_json["data"].append(data)

    return clues_json

#################【查询关键词匹配到的舆情数】#####################
def select_msg_count(keyword):
    body = {
        "query":{
                "multi_match":{
                    "query":keyword.replace('&', ' '),
                    "fields":["TITLE","CONTENT"],
                    "operator":"and", # 关键词之间是并且的关系
                    # "type": "phrase"  # 不分词

                }
        }
    }

    result = esdb.search('tab_iopm_article_info', body)
    # log.debug(tools.dumps_json(result))

    total_count = result.get("hits").get("total") if result else -1
    return total_count



def main():
    clues_json = get_clues()
    # print(tools.dumps_json(clues_json))

    # 清空原有的关键词表
    sql = 'delete from TAB_IOPM_CLUES_KEYWORDS t'
    oracledb.delete(sql)

    datas = clues_json.get('data')
    for data in datas:
        keyword2 = data.get('keyword2')
        keyword3 = data.get('keyword3')
        clues_id = data.get("clues_id")
        zero_id = data.get("zero_id")
        first_id = data.get("first_id")
        second_id = data.get("second_id")

        keyword2_list = match_keyword(keyword2) #将词组拆开 匹配

        for keys in keyword2_list:
            msg_count = select_msg_count(keys)
            print('''
            keys      %s
            msg_count %s
            '''%(keys, msg_count))

            sql = '''
                insert into TAB_IOPM_CLUES_KEYWORDS t
                  (t.id,
                   t.keyword2,
                   t.keyword3,
                   t.clues_id,
                   t.msg_count,
                   t.zero_id,
                   t.first_id,
                   t.second_id)
                values
                  (seq_clues_keywords.nextval,'%s','%s',%s,%s,%s,%s,%s)
            '''%(keys, keyword3, clues_id, msg_count, zero_id, first_id, second_id)

            # print(sql)
            oracledb.add(sql)


if __name__ == '__main__':
    main()
    # count = select_msg_count('总局44号令')
    # print(count)


