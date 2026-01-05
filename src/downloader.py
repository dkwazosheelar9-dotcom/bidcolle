#--coding:utf-8--

from src.manager import *
import manager
import requests
from bs4 import BeautifulSoup
from src.functionTools import *
import config
from jsonParse import *
import time
import re
import html
import chardet

# 日志记录器
# logger = setup_logger(script_name())
# error_logger = setup_logger(script_name() + '-error', level=logging.DEBUG)
# 延迟设置

delay = random.uniform(config.min_delay, config.max_delay)

tunnel = "k704.kdltps.com:15818"

# 用户名密码方式
username = "t13768587959834"
password = "fi1l91xt"
proxies = {
    "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
    "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
}

def clean_url(url):
    """清理URL中可能被错误转义的特殊字符"""
    # 处理HTML实体编码
    url = html.unescape(url)
    # 特别处理&curren被错误转义的情况
    url = url.replace('¤', '&curren')
    return url

def get_encoding(content):
    """自动检测内容编码，优先使用chardet，fallback到常见编码"""
    try:
        result = chardet.detect(content)
        encoding = result['encoding']
        # 过滤无效编码，使用兼容性更好的编码集
        if encoding in [None, 'GB2312', 'GBK']:
            return 'GB18030'  # GBK/GB2312的超集
        elif encoding == 'ISO-8859-1':
            return 'UTF-8'  # 常见误判场景修正
        return encoding
    except Exception as e:
        error_logger.warning(f"编码检测失败: {e}，使用默认UTF-8")
        return 'UTF-8'

def safe_decode(response):
    """安全解码响应内容，处理编码异常"""
    try:
        # 优先使用响应头中有效的编码
        if response.encoding and response.encoding.lower() != 'iso-8859-1':
            return response.text
        # 自动检测编码并解码，错误用�替换避免崩溃
        encoding = get_encoding(response.content)
        return response.content.decode(encoding, errors='replace')
    except UnicodeDecodeError as e:
        error_logger.error(f"解码失败: {e}，尝试UTF-8 fallback")
        return response.content.decode('utf-8', errors='replace')

def safe_json_loads(text):
    """安全解析JSON，处理编码相关的解析错误"""
    try:
        return json.loads(text)
    except UnicodeDecodeError:
        # 修复文本编码问题后重试
        encoded_text = text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        return json.loads(encoded_text)
    except json.JSONDecodeError as e:
        error_logger.error(f"JSON解析失败: {e}，清理非法字符后重试")
        clean_text = re.sub(r'[\x00-\x1F\x7F]', '', text)  # 移除控制字符
        return json.loads(clean_text)

def get_html_downloader(url, count, dict):
    if count >= 5:
        error_logger.error(f'Failed to download HTML from {url}')
        return None
    # 清理URL中的特殊字符
    url = clean_url(url)
    logger.info("爬取此网页：%s,次数：%d", url, count)
    headers = get_headers()
    if dict['post_headers'] is not None:
        headers = json.loads(dict['post_headers'])
    if dict['post_payload1'] is not None:
        try:
            payload = eval(dict['post_payload1'])
        except Exception as e:
            payload = json.dumps(dict['post_payload1'])
    else:
        payload = None
    try:
        if 'ccgp.gov' in dict['href']:
            response = requests.get(url, headers=headers, params=payload,timeout=5,proxies=proxies)
        else:
            response = requests.get(url, headers=headers, params=payload,timeout=5)
        html_content = safe_decode(response)
        logger.info(response.text)


    except Exception as e:
        error_logger.error(f"html_downloader异常发生{url}: {e}")
        time.sleep(delay * (count+1))
        return get_html_downloader(url, count + 1,dict)

    if response.status_code != 200 or '频繁访问' in response.text:
        print("状态码不正常或频繁访问", response.status_code)
        time.sleep(delay * (count+1))
        return get_html_downloader(url, count + 1,dict)
    else:
        soup = BeautifulSoup(html_content, 'html.parser')
        # logger.info(soup)
        if soup is not None:
            logger.info(f'Successfully downloaded HTML from {url}')
            return soup
        else:
            error_logger.error(f"html_downloader返回的soup为空{url}")
            return get_html_downloader(url, count + 1,dict)


# def selenium_html_downloader(url,count,dict):
#     if count >= 5:
#         print(f"爬取次数超过5次，停止爬取{url}")
#         return None
#     logger.info("爬取此网页：%s,次数：%d", url, count)
#     # driver = webdriver.Chrome(options=manager.chrome_options, service=manager.service)
#     # driver = webdriver.Chrome(options=manager.chrome_options, service=manager.service)
#     try:
#         driver = webdriver.Chrome(executable_path=config.chrome_driver_path,options=config.chrome_options)
#         driver.get(url)
#         time.sleep(delay * (count+1))
#         html = driver.page_source
#         driver.quit()
#         soup = BeautifulSoup(html, 'html.parser')
#         #logger.debug(f"成功爬取网页：{url}")
#         return soup
#
#     except Exception as e:
#         #logger.error(f"异常发生: {e}")
#         error_logger.error(f"selenium_html_downloader异常发生{url}: {e}")
#         time.sleep(delay * (count+1))
#         return selenium_html_downloader(url, count + 1)
#     finally:
#         driver.quit()

def post_html_downloader(url, count, dict):
    if count >= 5:
        error_logger.error(f"html_downloader爬取次数超过5次，停止爬取{url}")
        return None
    logger.info("爬取此网页：%s,次数：%d", url, count)
    try:
        headers = json.loads(dict['post_headers'])
        headers['User-Agent'] = random.choice(manager.agent_list)
        # logger.info("请求头：%s", headers)
        if dict['post_payload1'] is not None:
            post_payload1 = json.dumps(dict['post_payload1'])
            payload = json.loads(dict['post_payload1'])
        # logger.info("请求参数1：%s", payload)
        response = requests.post(url, headers=headers, json=payload,timeout=5)
        # logger.info(response.text)
        if len(response.text) < 200 or response.status_code != 200:
            response = requests.post(url, headers=headers, data=payload,timeout=5)
        html_content = safe_decode(response)
    except Exception as e:
        error_logger.error(f"post_html_downloader异常发生{url}:{e}")
        time.sleep(delay * (count+1))
        return post_html_downloader(url, count + 1, dict)
    if response.status_code != 200:
        # print("状态码不正常",response.status_code)
        time.sleep(delay * (count+1))
        return post_html_downloader(url, count + 1, dict)
    else:
        soup = BeautifulSoup(html_content, 'html.parser')
        if soup is not None:
            logger.info(f'Successfully downloaded HTML from {url}')
            return soup
        else:
            error_logger.error(f"html_downloader返回的soup为空{url}")
            return post_html_downloader(url, count + 1)

def post_json_downloader(url, count, dict):
    if count >= 5:
        error_logger.error(f"json_downloader爬取次数超过5次，停止爬取{url}")
        return None
    logger.info("爬取此网页：%s,次数：%d", url, count)
    try:
        headers = json.loads(dict['post_headers'])
        headers['User-Agent'] = random.choice(manager.agent_list)
        # logger.info("请求头：%s", headers)
        if dict['post_payload1'] is not None:
            try:
                payload = eval(dict['post_payload1'])
            except Exception as e:
                payload = json.dumps(dict['post_payload1'])
            # logger.info("请求参数1：%s", payload)
        else:
            payload = None
        if 'cfcpn' in dict['href'] or 'ccgp-hunan'in dict['href'] or 'hlyzztb' in dict['href'] or 'shenzhenair' in dict['href']:
            response = requests.post(url, headers=headers, data=payload,timeout=5)
        else:
            response = requests.post(url, headers=headers, data=json.dumps(payload),timeout=5)
            response2 = requests.post(url, headers=headers, data=payload,timeout=5)
    except Exception as e:
        error_logger.error(f"post_json_downloader异常发生{url}:{e}")
        time.sleep(delay * (count+1))
        return post_json_downloader(url, count + 1, dict)
    if response.status_code != 200 and response2.status_code == 200:
        response = response2
    if response.status_code != 200:
        # print("状态码不正常",response.status_code)
        error_logger.error(f"状态码不正常{response.status_code}")
        time.sleep(delay * (count+1))
        return post_json_downloader(url, count + 1, dict)
    else:
        try:
            json_text = safe_decode(response)
            r_json = safe_json_loads(json_text)
            print(r_json)
        except:
            r_json = convert_json_in_text(json_text)
        return r_json

def get_json_downloader(url, count, dict):
    if count >= 5:
        error_logger.error(f"json_downloader爬取次数超过5次，停止爬取{url}")
        return None
    logger.info("爬取此网页：%s,次数：%d", url, count)
    try:
        headers = json.loads(dict['post_headers'])
        headers['User-Agent'] = random.choice(manager.agent_list)
        # logger.info("请求头：%s", headers)
        # logger.info("请求url：%s", url)
        if dict['post_payload1'] is not None:
            # logger.info("请求参数1：%s", dict['post_payload1'])
            try:
                payload = eval(dict['post_payload1'])
            except Exception as e:
                payload = json.dumps(dict['post_payload1'])
            # logger.info("请求参数：%s", payload)
            response = requests.get(url, headers=headers, params=payload,timeout=5)
        else:
            response = requests.get(url, headers=headers,timeout=5)
        logger.info(response.text)
    except Exception as e:
        error_logger.error(f"get_json_downloader异常发生{url}:{e}")
        time.sleep(delay * (count+1))
        return get_json_downloader(url, count + 1, dict)
    if response.status_code != 200:
        # print("状态码不正常",response.status_code)
        time.sleep(delay * (count+1))
        return get_json_downloader(url, count + 1, dict)
    else:
        try:
            json_text = safe_decode(response)
            r_json = safe_json_loads(json_text)
        except:
            r_json = convert_json_in_text(json_text)
        # print("返回结果：", r_json)
        # logger.info("返回结果：%s", r_json)
        return r_json


def params_post_json_downloader(url, count, dict):
    if count >= 5:
        error_logger.error(f"params_post_json_downloader爬取次数超过5次，停止爬取{url}")
        return None
    logger.info("爬取此网页：%s,次数：%d", url, count)
    try:
        headers = json.loads(dict['post_headers'])
        headers['User-Agent'] = random.choice(manager.agent_list)
        # logger.info("请求头：%s", headers)
        if dict['post_payload1'] is not None:
            payload = json.loads(dict['post_payload1'])
        else:
            payload = None
        # logger.info("请求参数：%s", payload)
        response = requests.post(url, headers=headers, params=payload,timeout=5)
        # logger.info(response.text)
    except Exception as e:
        error_logger.error(f"params_post_json_downloader异常发生{url}:{e}")
        time.sleep(delay * (count+1))
        return params_post_json_downloader(url, count + 1, dict)
    if response.status_code != 200:
        # print("状态码不正常",response.status_code)
        time.sleep(delay * (count+1))
        return params_post_json_downloader(url, count + 1, dict)
    else:
        try:
            json_text = safe_decode(response)
            r_json = safe_json_loads(json_text)
            # logger.info("返回结果：%s", r_json)
        except:
            r_json = json_text
        # print("返回结果：", r_json)
        #logger.info("返回结果：%s", r_json)
        return r_json


def is_valid_url(url):
   # 正则表达式，用于匹配网址
   regex = r'^(https?://)?'  # 协议部分，http或https，可选
   regex += r'(([A-Za-z0-9-]+\.)+[A-Za-z]{2,}|'  # 域名部分，例如www.example.com
   regex += r'localhost|'  # 允许localhost
   regex += r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # 或者IP地址
   regex += r'(:\d+)?'  # 端口号，可选
   regex += r'(/[^\s]*)?'  # 路径部分，可选
   pattern = re.compile(regex)
   if re.match(pattern, url) is not None:
       return True
   return False
if __name__ == '__main__':
    # dict ={'href': 'https://www.bidradar.com.cn/api/subscribeV2/list', 'flag': '57', 'method': '4', 'find_list': "['data']['datalist']", 'find_href': "'https://www.bidradar.com.cn/pc/homeInfoDetail?recordId='+i['bidId']", 'find_title': "i['oriBidTitle']", 'find_pub_time': "i['publishTimeInfo']", 'find_zbid': "i['bidId']", 'post_content_href': 'https://www.bidradar.com.cn/api/bidInfoDetail/queryABTestBidInfoDetail', 'find_content': "['data']['html']", 'post_payload1': '{\r\n    "pageSize": 20,\r\n    "pageNo": 0,\r\n        "sortType": 0,\r\n    "inviteBidType": None,\r\n    "winBidType": [],\r\n    "platform": "web",\r\n    "bidType": "invite",\r\n    "userId": "2284289c32044fd48bfc3c0ea82a34ea"\r\n}', 'post_payload2': 'data = {\r\n    "recordId": dict[\'zbid\'],\r\n    "userId": "2284289c32044fd48bfc3c0ea82a34ea"\r\n}', 'post_headers': '{\r\n    "Accept": "application/json, text/plain, */*",\r\n    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",\r\n    "Authorization": "Bearer bcd2fee8-2033-4d41-a066-5b6c14a066f9",\r\n    "Connection": "keep-alive",\r\n    "Content-Type": "application/json",\r\n    "DNT": "1",\r\n    "Origin": "https://www.bidradar.com.cn",\r\n    "Sec-Fetch-Dest": "empty",\r\n    "Sec-Fetch-Mode": "cors",\r\n    "Sec-Fetch-Site": "same-origin",\r\n    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",\r\n    "sec-ch-ua": "\\"Chromium\\";v=\\"128\\", \\"Not;A=Brand\\";v=\\"24\\", \\"Microsoft Edge\\";v=\\"128\\"",\r\n    "sec-ch-ua-mobile": "?0",\r\n    "sec-ch-ua-platform": "\\"Windows\\""\r\n}'}
    # url = 'http://www.sizebid.com/002/shanghai/shanghai/2024/e5cfb9ab65de995231a65c054697869b.html'
    # r = get_html_downloader(url, 0, dict)
    url = 'https://search.ccgp.gov.cn/bxsearch?searchtype=1&page_index=16&bidSort=0&pinMu=0&bidType=0&dbselect=bidx&timeType=2&pppStatus=0'
    dict = {'id': 4152, 'webname': '中国政府采购网', 'webadd': 'http://search.ccgp.gov.cn/',
            'href': 'https://search.ccgp.gov.cn/bxsearch?searchtype=1&page_index=16&bidSort=0&pinMu=0&bidType=0&dbselect=bidx&timeType=2&pppStatus=0',
            'method': '14', 'find_list': "soup.find('ul',class_='vT-srch-result-list-bid').find_all('li')",
            'find_href': "item.find('a').get('href')", 'find_title': "item.find('a').text",
            'find_pub_time': "item.find('span').text.split('|')[0].replace('.','-')", 'find_zbid': None,
            'post_content_href': None, 'find_content': "soup.find('div',class_='vF_deail_maincontent')",
            'post_payload1': None, 'post_payload2': None, 'post_headers': None, 'list_method': 'get_html',
            'detail_method': 'get_html', 'page_params': None}
    r = get_html_downloader(url, 0, dict)
    print(r)
