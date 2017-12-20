# -*- coding: utf-8 -*-
'''
Created on 2017-12-13 13:10
---------
@summary: 统计词频 生成词云图用
---------
@author: Administrator
'''
import sys
sys.path.append('../')
import init

from utils.cut_text import CutText

class WordCloud():
    def __init__(self):
        self._cut_text = CutText()
        self._cut_text.set_stop_words('utils/stop_words.txt')

    def get_word_cloud(self, text):
        word_cloud = {}

        words = self._cut_text.cut(text)
        for word in words:
            if word in word_cloud.keys():
                word_cloud[word] += 1
            else:
                word_cloud[word] = 1

        return word_cloud

if __name__ == '__main__':
    text = '央视网消息(新闻联播)新闻联播新闻联播新闻联播新闻联播'
    word_cloud = WordCloud()
    word_cloud = word_cloud.get_word_cloud(text)
    print(word_cloud)
