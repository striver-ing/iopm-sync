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

class ProvinceFilter():
    def __init__(self, province_name = []):
        self._db = OracleDB()

        self._province_air = []
        self._province_air.append(province_name)

        province_id = self.load_province_id(province_name)
        self.load_province_air()

    def load_province_id(self, province_name):
        sql = "select t.id from TAB_MANAGE_PROVINCE_INFO t where t.province_name like '%{province_name}%'".format(province_name = province_name)
        result = self._db.find(sql)
        province_id = result[0] if result else -1

    def load_province_air(self, province_id):
        sql = "select t.area_name from TAB_MANAGE_AREA_INFO t where t.province_id = %s"%province_id
        province_air = self._db.find(sql)
        return province_air

    def load_province_town(self, province_id):
        sql = "select t.town_name from TAB_MANAGE_TOWN_INFO t where t.province_id = %s"%province_id
        province_town = self._db.find(sql)
        return province_town