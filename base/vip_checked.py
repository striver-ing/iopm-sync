# -*- coding: utf-8 -*-
'''
Created on 2017-08-09 15:44
---------
@summary: 判断是不是主流媒体
---------
@author: Boris
'''
import sys
sys.path.append('../')
import utils.tools as tools
from utils.log import log
from db.oracledb import OracleDB

class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls,'_inst'):
            cls._inst=super(Singleton,cls).__new__(cls, *args, **kwargs)

        return cls._inst

class VipChecked(Singleton):
    def __init__(self):
        super(VipChecked, self).__init__()
        if not hasattr(self,'_vip_sites'):
            self._vip_sites = []

            self._oracledb = OracleDB()

            self.load_vip_site()

    def load_vip_site(self):
        sql = 'select to_char(t.keyword2),t.zero_id,t.first_id, t.second_id from TAB_IOPM_CLUES t where zero_id = 7'
        sites = self._oracledb.find(sql)
        for site in sites:
            self._vip_sites.append(site)

        # print(self._vip_sites)

    def is_vip(self, content):
        '''
        @summary: 判断是否是主流媒体
        ---------
        @param content: 网址 / 网站名 / 作者
        ---------
        @result:
        is_vip (0 /1)
        zero_id
        first_id
        second_id
        '''

        is_vip = 0
        zero_id = first_id = second_id =  ''

        if not content:
            return False

        for site in self._vip_sites:
            domains = site[0].split(',')
            temp_zero_id = site[1]
            temp_first_id = site[2]
            temp_second_id = site[3]

            for domain in domains:
                if domain:
                    is_vip = ((domain in content) or (content in domain))

                    if is_vip:
                        # print(is_vip, domain)
                        zero_id = str(temp_zero_id)
                        first_id = str(temp_first_id)
                        second_id = str(temp_second_id)
                        break

            if is_vip:
                break

        return int(is_vip), zero_id, first_id, second_id

if __name__ == '__main__':
    vip_checked = VipChecked()
    is_vip = vip_checked.is_vip('http://mp.weixin.qq.com/s?__biz=MzAxNDI5NTY2NQ==&mid=2650646782&idx=2&sn=00cfa2dac81ec96c1731482bbe7dc821&scene=0#wechat_redirect')
    print(is_vip)