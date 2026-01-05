# -*- coding: utf-8 -*-
import json

def convert_json_in_text(text):
    # 去除{之前的多余内容，这条语句同时适用于{之前没有多余内容的情况
    t = text[len(text.split('{')[0]):]
    suffix_length = len(text.split('}')[-1])
    # 去除}之后的多余内容, }之后没有多余内容时，则不用处理
    if suffix_length:
        t = t[:-suffix_length]
    # 转换成字典返回
    return json.loads(t)
