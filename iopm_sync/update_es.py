# -*- coding: utf-8 -*-
'''
Created on 2017-11-13 21:13
---------
@summary:
---------
@author: Boris
'''

import sys
sys.path.append('../')
import init
import utils.tools as tools
from utils.log import log
from db.elastic_search import ES
from db.oracledb import OracleDB

def main():
    oracledb = OracleDB()
    esdb = ES()


    # sql = 'select MSG_ID from TAB_IOPM_USER_ACTION t where action_type=301 and msg_type = 502 and record_time>=sysdate-1'
    # article_ids = oracledb.find(sql)

    article_ids = [8888515,8888293,8891299]
    for article_id in article_ids:
        # article_id = article_id[0]

        body = {
            "WEIGHT" : 0
        }

        print(article_id)
        esdb.update_by_id('tab_iopm_article_info', article_id, body)


if __name__ == '__main__':
    main()