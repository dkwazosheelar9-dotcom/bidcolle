#--coding:utf-8--

import requests
from bs4 import BeautifulSoup
import json
from src.functionTools import *
import time
from datetime import datetime, timedelta
from manager import *
import config
from fake_useragent import UserAgent
# from curl_cff import requests

# 获取当前日期
current_date = datetime.now().strftime('%Y-%m-%d')  # 获取明天的日期

# 计算当前日期前9天的日期（保持10天的查询范围）
begin_date = (datetime.now() - timedelta(days=9)).strftime('%Y-%m-%d')  # 从9天前开始

delay = random.uniform(config.min_delay, config.max_delay)

ua = UserAgent()

# ip_pool = [
#     "120.25.1.15:7890",
#     "47.113.151.23:3128",
#     "60.28.59.242:7890",
#     "118.31.1.154:80",
#     "39.105.25.91:80",
#     "39.102.210.12:443",
#     "36.103.167.209:7890",
#     "58.240.211.251:7890"
#
# ]

tunnel = "k704.kdltps.com:15818"

# 用户名密码方式
username = "t13768587959834"
password = "fi1l91xt"
proxies = {
    "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
    "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
}


def ggzy_downloader_single(url, count, dict, source_type="1"):
    """
    单次请求函数，用于发送指定SOURCE_TYPE的请求
    """
    if count >= 5:
        error_logger.error(f'Failed to download HTML from {url}')
        return None
    logger.info("爬取此网页：%s,次数：%d,SOURCE_TYPE：%s", url, count, source_type)
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Connection": "close",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "DNT": "1",
        "Origin": "https://deal.ggzy.gov.cn",
        "Referer": "https://deal.ggzy.gov.cn/ds/deal/dealList.jsp",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": ua.random,
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }
    payload = {
        "TIMEBEGIN_SHOW": begin_date,
        "TIMEEND_SHOW": current_date,
        "TIMEBEGIN": begin_date,
        "TIMEEND": current_date,
        "SOURCE_TYPE": source_type,
        "DEAL_TIME": "03",
        "DEAL_CLASSIFY": "00",
        "DEAL_STAGE": "0000",
        "DEAL_PROVINCE": "0",
        "DEAL_CITY": "0",
        "DEAL_PLATFORM": "0",
        "BID_PLATFORM": "0",
        "DEAL_TRADE": "0",
        "isShowAll": "1",
        "FINDTXT": ""
    }
    if source_type == "2":
        payload = {
            "TIMEBEGIN_SHOW": begin_date,
            "TIMEEND_SHOW": current_date,
            "TIMEBEGIN": begin_date,
            "TIMEEND": current_date,
            "SOURCE_TYPE": source_type,
            "DEAL_TIME": "03",
            "DEAL_CLASSIFY": "01",
            "DEAL_STAGE": "0100",
            "DEAL_PROVINCE": "0",
            "DEAL_CITY": "0",
            "DEAL_PLATFORM": "0",
            "BID_PLATFORM": "0",
            "DEAL_TRADE": "0",
            "isShowAll": "1",
            "FINDTXT": ""
        }
    
    if dict['post_payload1'] is not None:
        logger.info("请求参数设置PAGENUMBER：%s", dict['post_payload1'])
        payload["PAGENUMBER"] = eval(dict['post_payload1'])
        logger.info("完整请求参数：%s", payload)
    else:
        payload = None

    try:
        response = requests.post(url, headers=headers, data=payload, proxies=proxies, timeout=10)
        logger.info("响应内容: %s", response.text)
        
        try:
            resp_json = response.json()
            if not resp_json.get('success', False):
                error_logger.error(f"API返回错误: {resp_json.get('error')}")
                return None
            return resp_json
        except json.JSONDecodeError:
            error_logger.error("JSON解析失败")
            return None
            
    except Exception as e:
        error_logger.error(f"ggzy_downloader_single异常发生{url},{proxies}: {e}")
        time.sleep(config.time_sleep)
        return ggzy_downloader_single(url, count + 1, dict, source_type)


def ggzy_downloader(url, count, dict):
    """
    主下载函数，根据post_payload1的值决定是否进行双重采集
    """
    if count >= 5:
        error_logger.error(f'Failed to download HTML from {url}')
        return None
    
    logger.info("使用的日期范围 - 开始: %s, 结束: %s", begin_date, current_date)
    
    # 检查post_payload1的值，决定采集策略
    try:
        if dict['post_payload1'] is not None:
            # post_payload1是带双引号的数字，需要先eval再转换为int
            page_str = eval(dict['post_payload1'])  # "11" -> 11 (字符串)
            page_num = int(page_str)  # 转换为数字
        else:
            page_num = 100
        logger.info("页码参数: %s (类型: %s)", page_num, type(page_num).__name__)
    except Exception as e:
        page_num = 100  # 默认值，不进行双重采集
        logger.warning("无法解析页码参数 %s，使用默认值100，错误: %s", dict.get('post_payload1'), e)
    
    if page_num < 100:
        logger.info("页码小于100，进行SOURCE_TYPE=1和SOURCE_TYPE=2的双重采集")
        
        # 采集SOURCE_TYPE=1的数据
        result1 = ggzy_downloader_single(url, 0, dict, "1")
        
        # 采集SOURCE_TYPE=2的数据
        result2 = ggzy_downloader_single(url, 0, dict, "2")
        
        # 合并结果
        if result1 and result2:
            # 合并data数组
            if 'data' in result1 and 'data' in result2:
                result1['data'].extend(result2['data'])
                logger.info("成功合并SOURCE_TYPE=1和SOURCE_TYPE=2的数据，总数据量: %d", len(result1['data']))
            return result1
        elif result1:
            logger.warning("只获取到SOURCE_TYPE=1的数据")
            return result1
        elif result2:
            logger.warning("只获取到SOURCE_TYPE=2的数据")
            return result2
        else:
            logger.error("SOURCE_TYPE=1和SOURCE_TYPE=2的数据都获取失败")
            return None
    else:
        logger.info("页码大于等于100，只采集SOURCE_TYPE=1的数据")
        return ggzy_downloader_single(url, 0, dict, "1")


def ggzy_bidcollect(dict):
    result_list = []
    url = dict['href']
    r_json = ggzy_downloader(url, 0, dict)
    if r_json is None:
        error_logger.error(f"list_json_resolver获取网页{url}失败")
        return result_list
    try:
        logger.info(dict)
        items = eval(f"r_json{dict['find_list']}")
    except Exception as e:
        error_logger.error(f"list_json_resolver解析{url}失败: {e}")
        return result_list
    for i in items:
        logger.info(f'list_json_resolver i :{i}')
        page_data = {}
        # print(f"{spider_dict['findhref']}")
        try:
            page_data['href'] = eval(f"{dict['find_href']}").replace('http','https')
            logger.info(f'page_data["href"] is :{page_data["href"]}')
            page_data['title'] = eval(f"{dict['find_title']}")
            logger.info(f'page_data["title"] is :{page_data["title"]}')
            page_data['publish_time'] = eval(f"{dict['find_pub_time']}")
            logger.info(f'page_data["publish_time"] is :{page_data["publish_time"]}')
            logger.info(f'page_data is :{page_data}')
            logger.info(f'page_data["publish_time"] is :{page_data["publish_time"]}')
            page_data['zbid'] = eval(f"{dict['find_zbid']}")
            logger.info(f'page_data["zbid"] is :{page_data["zbid"]}')
            page_data['post_content_href'] = eval(f"{dict['post_content_href']}")
            logger.info(f'page_data["post_content_href"] is :{page_data["post_content_href"]}')
            page_data['post_payload1'] = dict['post_payload2']
            if dict['post_payload2'] is not None:
                page_data['post_payload1'] = dict['post_payload2']
            logger.info(f'page_data["post_payload1"] is :{page_data["post_payload1"]}')


        except (IndexError, TypeError) as e:
            error_logger.error(f'list_json_resolver定位错误{url}:{e}')
            continue

        result_list.append(page_data)

    logger.info(f'result_list is :{result_list}')
    return result_list

def ggzy_page_downloader(url,count):
    if count >= 5:
        error_logger.error(f'Failed to download HTML from {url}')
        return None
    logger.info("爬取此网页：%s,次数：%d", url, count)
    headers = get_headers()
    headers['Referer'] = url.replace('/b/','/a/')
    logger.info("请求头：%s", headers)
    # proxy = random.choice(ip_pool)
    try:
        response = requests.get(url, headers=headers,proxies=proxies, verify=False, timeout=10)
        response.encoding = response.apparent_encoding
        # logger.info(response.text)


    except Exception as e:
        error_logger.error(f"ggzy_page_downloader异常发生{url},{proxies}: {e}")
        time.sleep(delay)
        return ggzy_page_downloader(url, count + 1)

    if response.status_code != 200:
        print("状态码不正常", response.status_code)
        time.sleep(delay)
        return ggzy_page_downloader(url, count + 1)
    else:
        soup = BeautifulSoup(response.text, 'html.parser')
        if soup is not None:
            logger.info(f'Successfully downloaded HTML from {url}')
            logger.info(soup.prettify())
            return soup
        else:
            error_logger.error(f"ggzy_page_downloader返回的soup为空{url}")
            return ggzy_page_downloader(url, count + 1)


if __name__ == '__main__':
    url = 'https://deal.ggzy.gov.cn/ds/deal/dealList_find.jsp'
    # dict =  {'id': 1156, 'href': 'http://deal.ggzy.gov.cn/ds/deal/dealList_find.jsp', 'method': '15', 'find_list': "['data']", 'find_href': "i['url'].replace('/a/','/b/')", 'find_title': "i['title']", 'find_pub_time': "i['timeShow']", 'find_zbid': None, 'post_content_href': "i['url'].replace('/a/','/b/')", 'find_content': "soup.find('body')", 'post_payload1': '"1"', 'post_payload2': None, 'post_headers': None}
    dict = {'id': 1223, 'webname': '全国公共资源交易平台', 'webadd': 'http://deal.ggzy.gov.cn', 'href': 'https://deal.ggzy.gov.cn/ds/deal/dealList_find.jsp', 'method': '15', 'find_list': "['data']", 'find_href': "i['url'].replace('/a/','/b/')", 'find_title': "i['title']", 'find_pub_time': "i['timeShow']", 'find_zbid': None, 'post_content_href': "i['url'].replace('/a/','/b/')", 'find_content': "soup.find('body')", 'post_payload1': '"11"', 'post_payload2': None, 'post_headers': '{\r\n    "DNT": "1",\r\n    "Referer": "https://www.ggzy.gov.cn/information/html/a/530000/0101/202411/01/0053a1ae8c5926644867a941ad862ab82982.shtml",\r\n    "Upgrade-Insecure-Requests": "1",\r\n    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",\r\n    "sec-ch-ua": "\\"Chromium\\";v=\\"130\\", \\"Microsoft Edge\\";v=\\"130\\", \\"Not?A_Brand\\";v=\\"99\\"",\r\n    "sec-ch-ua-mobile": "?0",\r\n    "sec-ch-ua-platform": "\\"Windows\\""\r\n}', 'list_method': 'get_html', 'detail_method': 'get_html', 'page_params': None}
    r = ggzy_bidcollect(dict)
    url1 = "https://www.ggzy.gov.cn/information/html/b/520000/0102/202502/20/005288661389588344a6a868b3bf74050568.shtml"
    r1 = ggzy_page_downloader(url1,0)
    # r = ggzy_downloader(url,0,dict)



