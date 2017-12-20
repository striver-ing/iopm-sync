# -*- coding: utf-8 -*-
'''
Created on 2017-12-11 17:23
---------
@summary: 关键词比对
---------
@author: Administrator
'''
import sys
sys.path.append('..')
import init

import utils.tools as tools
from db.oracledb import OracleDB
from base.format_keywords import format_keywords

class CompareKeywords():
    def __init__(self):
        self._oracledb = OracleDB()
        self._clues = self.get_clues()

    def get_clues(self):
        sql = 'select t.id clues_id,to_char(t.keyword2),to_char(t.keyword3),t.zero_id, FIRST_ID, second_id  from TAB_IOPM_CLUES t where zero_id != 7' # 7 为传播途径
        clues = self._oracledb.find(sql)
        return clues

    def get_contained_keys(self, text):
        '''
        @summary:
        ---------
        @param text:比较的文本
        @param keys:关键词列表
        ---------
        @result:
        '''
        keywords = []
        clues_ids = []
        zero_ids = []
        first_ids = []
        second_ids = []
        keyword_clues = {}

        for clue in self._clues:
            clue_id = clue[0]
            key2 = clue[1]
            key3 = clue[2]
            zero_id = clue[3]
            first_id = clue[4]
            second_id = clue[5]

            keys = format_keywords(key2) # 格式化线索词
            for key in keys: #['新闻节目', '总理&主席', 'the xi factor']
                # 获取单元key 如 总理&主席 必须全包含
                unit_keys = key.split('&') # [总理, 主席]
                for unit_key in unit_keys:
                    if unit_key not in text:
                        break
                else:
                    keywords.extend(unit_keys)
                    clues_ids.append(str(clue_id))
                    zero_ids.append(str(zero_id))
                    first_ids.append(str(first_id))
                    second_ids.append(str(second_id))
                    for unit_key in unit_keys:
                        keyword_clues[unit_key] = clue_id

        return ','.join(set(keywords)), ','.join(set(clues_ids)), ','.join(set(zero_ids)), ','.join(set(first_ids)), ','.join(set(second_ids)), keyword_clues

if __name__ == '__main__':
    compare_keywords = CompareKeywords()
    text = '聂辰席是中央宣传部的聂辰席&国家新闻出版广电总局'

    print(compare_keywords.get_contained_keys(text))