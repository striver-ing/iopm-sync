# -*- coding: utf-8 -*-
'''
Created on 2017-12-29 10:44
---------
@summary: 筛选事件类型
---------
@author: Boris
'''
import sys
sys.path.append('../')
import init

import utils.tools as tools
from utils.log import log
from db.oracledb import OracleDB

class EventFilter():
    def __init__(self):
        print(1)
        self._db = OracleDB()
        self._event_knowledges = self.load_event_knowledges()
        print(self._event_knowledges)

    def load_event_knowledges(self):
        '''
        @summary:
        801 时事政治
        802 社会民生
        803 教育改革
        804 医疗卫生
        805 科技舆情
        806 意识形态（无）
        807 政策法规
        808 经济舆情（无）
        809 生态文明
        810 体育舆情（无）
        811 突发安全（无）
        ---------
        ---------
        @result:
        '''
        sql = 'select t.keyword, t.type from TAB_IOPM_EVENT_KNOWLEDEGE t'
        event_knowledges = self._db.find(sql)
        return event_knowledges

    def find_contain_event(self, text):
        contain_event_type = set()
        for event in self._event_knowledges:
            event_keyword = event[0]
            event_type = event[1]

            if event_keyword in text:
                contain_event_type.add(str(event_type))

        return list(contain_event_type)


if __name__ == '__main__':
    event_filter = EventFilter()
