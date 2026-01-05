# from datetime import datetime
from src.functionTools import *
import datetime

def deal_to_dict(r_json):
    if not isinstance(r_json, dict):
        return {'x':r_json}
    return r_json

def parse_time(time_str):
    try:
        return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return datetime.datetime.strptime(time_str + ' 00:00:00', '%Y-%m-%d %H:%M:%S')

def deal_repeat_data(result_list):
    latest_data = {}
    duplicate_count = 0 #计数
    for item in result_list:
        title = item['title']
        time = item['publish_time']
        time_obj = parse_time(time)

        if title in latest_data:
            logger.info(f"重复标题: {title}, 发布时间: {time}")
            duplicate_count += 1

        if title not in latest_data or datetime.datetime.strptime(latest_data[title]['publish_time'], '%Y-%m-%d %H:%M:%S') < time_obj:
            latest_data[title] = item

    logger.info(f"共有：{duplicate_count}条标题重复的标讯信息")
    return list(latest_data.values())
