# -*- coding: utf-8 -*-
'''
Created on 2017-11-01 16:20
---------
@summary:
---------
@author: Boris
'''
import sys
sys.path.append('../')
import init
from numpy import *
import numpy as np
from utils.cut_text import CutText
import utils.tools as tools

cut_text = CutText()
cut_text.set_stop_words('./utils/stop_words.txt')

def cut_words(text):
    return cut_text.cut_for_keyword(text, top_keyword_count = 10)

def dist_meas(vecA, vecB):
    # return sqrt(sum(power(vecA - vecB, 2))) #la.norm(vecA-vecB) # 欧几里得距离
    denominator = (sqrt(sum(vecA**2)) * sqrt(sum(vecB**2))) # 分母可能为零
    if denominator:
        return sum(vecA * vecB) / denominator # 余弦定理(0~1) 余弦值越接近1，就表明夹角越接近0度，也就是两个向量越相似
    else:
        return 0

def get_all_vector(texts):
    '''
    @summary: 词袋模型  返回词频
    用结巴分词 提取文章中前n个关键字 关键字没有重复的 所以词频为 0 或 1
    ---------
    @param texts: 文本集合[(id, text),(id, text),(id, text),(id, text)]
    @param cut_text: 拆词方法
    ---------
    @result:
    '''

    texts_info = [text for text in texts]

    docs = []
    word_set = set()
    for text_info in texts_info:
        doc = cut_words(text_info)
        # print(doc)
        docs.append(doc)
        word_set |= set(doc) # 取并集

    word_set = list(word_set)

    docs_vsm = []
    for doc in docs:
        temp_vector = []
        for word in word_set:
            temp_vector.append(doc.count(word) * 1.0)
        docs_vsm.append(temp_vector)
    docs_matrix = np.array(docs_vsm)

    return texts, docs_matrix, word_set


def compare_text(text1, text2):
    '''
    @summary: 比较文本
    ---------
    @param text1: 文本1
    @param text2: 文本2
    ---------
    @result: 返回文本相似度 0 ~ 1  越接近1 越相似
    '''
    if not text1 and text2:
        return 0

    texts = [
        text1,
        text2
    ]

    texts, tfidf_mat, word_set = get_all_vector(texts)
    # print(word_set)
    # print('tfidf_mat shape', shape(tfidf_mat))
    # print(tfidf_mat)
    similarity = dist_meas(tfidf_mat[0,:],tfidf_mat[1,:])

    return similarity  # 越接近于1 越相似

if __name__ == "__main__":
    text1 = '范冰冰的《赢天下》深陷“退片”风波，“准剧王”们怎么了？'
    text2 = ''

    result = compare_text(text1, text2)
    print(result)
