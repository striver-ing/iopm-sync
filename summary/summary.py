# -*- coding: utf-8 -*-
'''
Created on 2017-12-15 10:34
---------
@summary:
---------
@author: Administrator
'''
import sys
sys.path.append('../')
import init

import codecs
from textrank4zh import TextRank4Sentence

class Summary():
    def __init__(self):
        self._tr4s = TextRank4Sentence('utils/stop_words.txt')

    def get_summary(self, text, sentence_count = 3):
        self._tr4s.analyze(text=text, lower=True, source = 'no_filter')

        sentences = self._tr4s.get_key_sentences(sentence_count)
        sentences = sorted(sentences, key=lambda sentence:sentence['index'])

        summary = ''
        for item in sentences:
            # print(item.index, item.weight, item.sentence)
            summary += item.sentence + '。'

        return summary

if __name__ == '__main__':
    summary = Summary()

    text = '''会议强调，习近平总书记的重要指示切中时弊，要求明确，充分体现了以习近平同志为核心的党中央坚定不移全面从严治党、持之以恒正风肃纪的鲜明态度和坚定决心。我们要认真学习贯彻习近平总书记重要指示精神，以永远在路上的坚韧锲而不舍抓好作风建设。一要把思想统一到习近平总书记的重要指示精神上来，认真贯彻落实中央八项规定精神，驰而不息纠“四风”，使中央八项规定精神成为每个党员干部的自觉行动、行为习惯。二要切实按照习近平总书记重要指示要求，认真查找和纠正“四风”特别是形式主义、官僚主义的突出问题，采取过硬措施，坚决予以整改。各级纪检监察机关要切实履行监督责任，密切注意不正之风的新动向、新表现，继续紧盯元旦、春节等时间节点，抓常抓细抓长，坚决防止“四风”问题反弹回潮。三要坚持从各级领导班子和领导干部做起，以更高标准、更严要求，以上率下，形成“头雁效应”。全省各级领导干部特别是省委常委会班子成员要严格对照我省贯彻落实中央八项规定精神实施办法要求，带头改进作风，为全省党员干部作出表率。各级党委（党组）主要负责同志要加强对班子成员和下级党组织遵守中央八项规定精神的提醒、监督，针对形式主义、官僚主义的各种表现，突出重点、抓早抓小，以钉钉子精神推动党风政风持续好转。（记者/徐林 通讯员/岳宗）'''
    summary = summary.get_summary(text)
    print(summary)