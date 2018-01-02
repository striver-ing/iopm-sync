# -*- coding: utf-8 -*-
'''
Created on 2017-12-29 10:44
---------
@summary: 筛选符合省内的信息
---------
@author: Boris
'''
import sys
sys.path.append('../')
import init

import utils.tools as tools
from utils.log import log
from db.oracledb import OracleDB

PROVINCE = tools.get_conf_value('config.conf', 'province', 'province')

class ProvinceFilter():
    def __init__(self, province_name = PROVINCE):
        self._province_airs = []
        if province_name:
            self._db = OracleDB()
            self._province_airs.append(province_name)
            province_id = self.load_province_id(province_name)
            if province_id:
                self._province_airs.extend(air[0][:-1] for air in self.load_province_air(province_id))
                self._province_airs.extend(town[0][:-1] for town in self.load_province_town(province_id))

        print(self._province_airs)

    def load_province_id(self, province_name):
        sql = "select t.id from TAB_MANAGE_PROVINCE_INFO t where t.province_name like '%{province_name}%'".format(province_name = province_name)
        result = self._db.find(sql)
        province_id = result[0][0] if result else None
        if not province_id:
            log.debug('TAB_MANAGE_PROVINCE_INFO 无 %s 省份'%province_name)

        return province_id

    def load_province_air(self, province_id):
        sql = "select t.area_name from TAB_MANAGE_AREA_INFO t where t.province_id = %s"%province_id
        province_air = self._db.find(sql)
        return province_air

    def load_province_town(self, province_id):
        sql = "select t.town_name from TAB_MANAGE_TOWN_INFO t where t.province_id = %s"%province_id
        province_town = self._db.find(sql)
        return province_town

    def find_contain_air(self, text):
        contain_airs = []

        for air in self._province_airs:
            if air in text:
                contain_airs.append(air)

        return contain_airs

if __name__ == '__main__':
    province_filter = ProvinceFilter()
    text = '浙江是个好地方'
    contain_airs = province_filter.find_contain_air(text)
    print(contain_airs)
