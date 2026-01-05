# -*- coding: utf-8 -*-
import requests
import json
import time
from src.chaojiying import Chaojiying_Client
from src.functionTools import *
from src.manager import get_headers
import config
import random
from bs4 import BeautifulSoup

# 日志记录器
logger = setup_logger('gdgpo_collector')
error_logger = setup_logger('gdgpo_collector-error', level=logging.DEBUG)

# 超级鹰验证码识别配置
CHAOJIYING_USERNAME = 'minami'
CHAOJIYING_PASSWORD = '98&jRwMdNN'
CHAOJIYING_SOFT_ID = '973680'
CHAOJIYING_CODETYPE = '1004'  # 验证码类型

class GDGPOCollector:
    def __init__(self, site_type='guangdong'):
        self.session = requests.Session()
        self.chaojiying = Chaojiying_Client(CHAOJIYING_USERNAME, CHAOJIYING_PASSWORD, CHAOJIYING_SOFT_ID)
        
        # 用于缓存验证码，避免在同网站同列表下翻页时重复识别
        self.cached_captcha = None
        self.cached_site_type = None
        self.cached_channel = None
        self.cached_notice_type = None
        self.captcha_timestamp = 0
        self.CAPTCHA_CACHE_DURATION = 900  # 验证码缓存5分钟
        
        # 根据站点类型设置不同的URL
        if site_type == 'shaanxi':
            self.base_url = 'https://www.ccgp-shaanxi.gov.cn'
            self.captcha_url = 'https://www.ccgp-shaanxi.gov.cn/cms-sx/site/shanxi/xxgg/index.html?xxggType=123&noticeType=00101'  # 验证码页面URL
            self.list_url = 'https://www.ccgp-shaanxi.gov.cn/freecms/rest/v1/notice/selectInfoMoreChannel.do'  # 列表数据接口
            self.captcha_img_url = 'https://www.ccgp-shaanxi.gov.cn/freecms/verify/verifyCode.do'  # 验证码图片URL
        elif site_type == 'sichuan':
            self.base_url = 'https://www.ccgp-sichuan.gov.cn'
            self.captcha_url = 'https://www.ccgp-sichuan.gov.cn/maincms-web/noticeInformation?typeId=ggxx'  # 验证码页面URL
            self.list_url = 'https://www.ccgp-sichuan.gov.cn/gpcms/rest/web/v2/info/selectInfoForIndex'  # 列表数据接口
            self.captcha_img_url = 'https://www.ccgp-sichuan.gov.cn/gpcms/rest/web/v2/index/getVerify'  # 验证码图片URL
        else:  # 默认为广东
            self.base_url = 'https://gdgpo.czt.gd.gov.cn'
            self.captcha_url = 'https://gdgpo.czt.gd.gov.cn/maincms-web/noticeInformationGd'  # 验证码页面URL
            self.list_url = 'https://gdgpo.czt.gd.gov.cn/gpcms/rest/web/v2/info/selectInfoForIndex'  # 列表数据接口
            self.captcha_img_url = 'https://gdgpo.czt.gd.gov.cn/gpcms/rest/web/v2/index/getVerify'  # 验证码图片URL
        
    def get_captcha_image(self):
        """获取验证码图片"""
        try:
            headers = get_headers()
            headers['Referer'] = self.base_url + '/'
            
            # 先访问验证码页面，获取必要的cookie
            logger.info("正在访问验证码页面获取cookie...")
            response = self.session.get(self.captcha_url, headers=headers, timeout=10)
            response.raise_for_status()
            logger.info(f"验证码页面状态码: {response.status_code}")
            
            # 使用对应网站的验证码图片URL
            captcha_img_url = self.captcha_img_url
            
            # 陕西省需要添加时间戳参数
            if 'ccgp-shaanxi.gov.cn' in self.base_url:
                captcha_img_url += f'?createTypeFlag=n&name=notice&d{int(time.time() * 1000)}'
            
            captcha_image = None
            try:
                logger.info(f"尝试获取验证码图片: {captcha_img_url}")
                captcha_response = self.session.get(captcha_img_url, headers=headers, timeout=10)
                logger.info(f"验证码图片请求状态码: {captcha_response.status_code}")
                if captcha_response.status_code == 200 and captcha_response.content:
                    captcha_image = captcha_response.content
                    logger.info(f"成功获取验证码图片，URL: {captcha_img_url}, 大小: {len(captcha_image)} 字节")
                    # 保存验证码图片用于调试
                    with open('debug_captcha.png', 'wb') as f:
                        f.write(captcha_image)
                    logger.info("验证码图片已保存为 debug_captcha.png")
            except Exception as e:
                logger.warning(f"获取验证码图片失败: {e}")
            
            if not captcha_image:
                # 如果直接URL获取失败，尝试从页面中解析验证码图片
                logger.info("尝试从页面HTML中解析验证码图片...")
                soup = BeautifulSoup(response.text, 'html.parser')
                # 根据用户提供的XPath信息查找验证码图片
                # XPath: //*[@id="code_img"]
                captcha_img_elements = soup.select('#code_img')
                logger.info(f"从页面找到 {len(captcha_img_elements)} 个id为code_img的元素")
                
                # 遍历找到的元素，查找img标签
                for element in captcha_img_elements:
                    # 查找该元素下的img标签
                    img_tags = element.find_all('img')
                    logger.info(f"在code_img元素下找到 {len(img_tags)} 个img标签")
                    
                    for img in img_tags:
                        if img.get('src'):
                            captcha_img_url = img['src']
                            logger.info(f"找到验证码图片URL: {captcha_img_url}")
                            # 如果是相对路径，转换为绝对路径
                            if not captcha_img_url.startswith('http'):
                                captcha_img_url = self.base_url + captcha_img_url
                            try:
                                captcha_response = self.session.get(captcha_img_url, headers=headers, timeout=10)
                                if captcha_response.status_code == 200 and captcha_response.content:
                                    captcha_image = captcha_response.content
                                    logger.info(f"从页面解析获取验证码图片，URL: {captcha_img_url}, 大小: {len(captcha_image)} 字节")
                                    # 保存验证码图片用于调试
                                    with open('debug_captcha_parsed.png', 'wb') as f:
                                        f.write(captcha_image)
                                    logger.info("解析的验证码图片已保存为 debug_captcha_parsed.png")
                                    break
                            except Exception as e:
                                logger.warning(f"获取页面解析的验证码图片失败: {e}")
                                continue
                    if captcha_image:
                        break
            
            if not captcha_image:
                # 如果上面的方法都失败了，尝试直接查找页面中的img标签
                logger.info("尝试直接查找页面中的验证码图片...")
                soup = BeautifulSoup(response.text, 'html.parser')
                # 查找所有img标签
                all_imgs = soup.find_all('img')
                logger.info(f"页面中共有 {len(all_imgs)} 个img标签")
                
                # 查找可能的验证码图片（通常验证码图片没有alt属性或者alt属性包含"验证码"等字样）
                for img in all_imgs:
                    src = img.get('src', '')
                    # 检查src是否包含验证码相关关键词
                    if 'verify' in src.lower() or 'code' in src.lower() or 'captcha' in src.lower():
                        captcha_img_url = src
                        logger.info(f"根据关键词匹配找到验证码图片URL: {captcha_img_url}")
                        # 如果是相对路径，转换为绝对路径
                        if not captcha_img_url.startswith('http'):
                            captcha_img_url = self.base_url + captcha_img_url
                        try:
                            captcha_response = self.session.get(captcha_img_url, headers=headers, timeout=10)
                            if captcha_response.status_code == 200 and captcha_response.content:
                                captcha_image = captcha_response.content
                                logger.info(f"通过关键词匹配获取验证码图片，URL: {captcha_img_url}, 大小: {len(captcha_image)} 字节")
                                # 保存验证码图片用于调试
                                with open('debug_captcha_keyword.png', 'wb') as f:
                                    f.write(captcha_image)
                                logger.info("关键词匹配的验证码图片已保存为 debug_captcha_keyword.png")
                                break
                        except Exception as e:
                            logger.warning(f"获取关键词匹配的验证码图片失败: {e}")
                            continue
            
            if not captcha_image:
                error_logger.error("无法获取验证码图片")
                return None
                
            return captcha_image
            
        except Exception as e:
            error_logger.error(f"获取验证码图片失败: {e}")
            return None

    def recognize_captcha(self, site_type=None, channel=None, notice_type=None):
        """使用超级鹰识别验证码，增加缓存机制"""
        # 检查是否可以复用缓存的验证码
        current_time = time.time()
        if (self.cached_captcha and 
            self.cached_site_type == site_type and
            (channel is None or self.cached_channel == channel) and
            (notice_type is None or self.cached_notice_type == notice_type) and
            (current_time - self.captcha_timestamp) < self.CAPTCHA_CACHE_DURATION):
            logger.info(f"复用缓存的验证码，验证码: {self.cached_captcha}")
            return self.cached_captcha
        
        try:
            # 获取验证码图片
            captcha_image = self.get_captcha_image()
            if not captcha_image:
                return None
                
            # 检查图片大小
            logger.info(f"验证码图片大小: {len(captcha_image)} 字节")
            if len(captcha_image) < 100:  # 可能不是有效的图片
                error_logger.error("验证码图片可能无效，大小过小")
                return None
                
            # 重试机制，最多尝试3次
            for attempt in range(3):
                try:
                    logger.info(f"尝试使用超级鹰识别验证码 (尝试 {attempt+1}/3)...")
                    # 使用超级鹰识别验证码
                    result = self.chaojiying.PostPic(captcha_image, CHAOJIYING_CODETYPE)
                    logger.info(f"验证码识别结果: {result}")
                    
                    if result.get('err_str') == 'OK':
                        captcha_code = result['pic_str']
                        logger.info(f"识别到验证码: {captcha_code}")
                        
                        # 缓存验证码
                        self.cached_captcha = captcha_code
                        self.cached_site_type = site_type
                        self.cached_channel = channel
                        self.cached_notice_type = notice_type
                        self.captcha_timestamp = current_time
                        
                        return captcha_code
                    else:
                        error_logger.warning(f"验证码识别失败 (尝试 {attempt+1}/3): 错误码 {result.get('err_no')}, 错误信息 {result.get('err_str')}")
                        if attempt < 2:  # 不是最后一次尝试，等待一段时间再重试
                            time.sleep(2)
                except Exception as e:
                    error_logger.warning(f"验证码识别异常 (尝试 {attempt+1}/3): {e}")
                    if attempt < 2:
                        time.sleep(2)
            
            error_logger.error("验证码识别失败，已达到最大重试次数")
            return None
                
        except Exception as e:
            error_logger.error(f"识别验证码失败: {e}")
            return None

    def submit_captcha_and_get_list(self, verify_code, site_type='guangdong', notice_type="00101", region_code="610001", channel=None, page=1):
        """提交验证码并获取列表数据"""
        try:
            # 根据站点类型设置不同的请求头和参数
            if site_type == 'shaanxi':
                api_headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                    "Connection": "keep-alive",
                    "Content-Type": "application/json;charset=utf-8",
                    "Referer": f"https://www.ccgp-shaanxi.gov.cn/cms-sx/site/shanxi/xxgg/index.html?xxggType=123&noticeType={notice_type}",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
                    "X-Requested-With": "XMLHttpRequest",
                    "nsssjss": "GeKi1sSJFUpyRBXDeoEckK0ydUApcc8k062uxcIMo1pS4KrupDoAOfnJQ6/EELjOU84fSQisqlqjIu95bZjmKCyf2B+Go9k6CdL4xYUfRZCe+7RWvYdmKy7WwEW5iGPlH1ptPYrB42UDuyp9rzf4tD+qIPPIf3Mma3dBMRX/1aw=",
                    "sec-ch-ua": '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sign": "a80d71fd43f404c61891f123480e9332",
                    "time": str(int(time.time() * 1000)),
                    "url": "/freecms/rest/v1/notice/selectInfoMoreChannel.do"
                }
                
                # 特殊处理：对于noticeType="001054,00100B"且regionCode="611000"的组合，确保正确处理
                city_or_area = "6"  # 保持为"6"
                
                # 构造请求参数（根据陕西省政府采购网的配置）
                # 陕西省使用GET方法
                params = {
                    "": "",
                    "siteId": "a7a15d60-de5b-42f2-b35a-7e3efc34e54f",
                    "channel": "1eb454a2-7ff7-4a3b-b12c-12acc2685bd1",
                    "currPage": "1",
                    "pageSize": "10",
                    "noticeType": notice_type,  # 使用传入的notice_type参数
                    "regionCode": region_code,  # 使用传入的region_code参数
                    "purchaseManner": "",
                    "title": "",
                    "verifyCode": verify_code,  # 使用识别出的验证码
                    "openTenderCode": "",
                    "purchaseNature": "",
                    "operationStartTime": "",
                    "operationEndTime": "",
                    "selectTimeName": "noticeTime",
                    "cityOrArea": city_or_area,  # 使用正确的cityOrArea参数
                    "_t": str(int(time.time() * 1000))
                }
                
                logger.info(f"发送GET请求获取陕西省列表数据，noticeType: {notice_type}, regionCode: {region_code}, cityOrArea: '{city_or_area}'...")
                # 陕西省使用GET方法发送请求
                response = self.session.get(self.list_url, headers=api_headers, params=params, timeout=15)
            elif site_type == 'sichuan':
                api_headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
                    "nsssjss": "U/lPUB8lHsQWLyVSkJf7JN0V4zgrmIi3RvEH9DAYATuM183EBAxf2dtMpyasaRlxrw0KYIcui33IrHXcGwdM0YINy5zLTieHCgIAmf+9bPWf4SBa/BQ2N25U5+WgjgnXNXqW/tmJep96oqjBlaYLHY3kT9Cw+aKeN6qOSJYI+98=",
                    "sec-ch-ua": '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sign": "63ed205307e320191d668de67cc7452a",
                    "time": str(int(time.time() * 1000)),
                    "url": "/gpcms/rest/web/v2/info/selectInfoForIndex"
                }
                
                # 构造请求参数（根据四川省政府采购网的配置）
                params = {
                    "title": "",
                    "region": "",
                    "siteId": "94c965cc-c55d-4f92-8469-d5875c68bd04",
                    "channel": "c5bff13f-21ca-4dac-b158-cb40accd3035",
                    "currPage": "1",
                    "pageSize": "10",
                    "noticeType": notice_type,  # 使用传入的notice_type参数
                    "regionCode": "",
                    "cityOrArea": "",
                    "purchaseManner": "",
                    "openTenderCode": "",
                    "purchaser": "",
                    "agency": "",
                    "purchaseNature": "",
                    "operationStartTime": "",
                    "operationEndTime": "",
                    "verifyCode": verify_code,  # 使用识别出的验证码
                    "_t": str(int(time.time() * 1000))
                }
                
                logger.info(f"发送GET请求获取四川省列表数据，noticeType: {notice_type}...")
                # 使用GET方法发送请求
                response = self.session.get(self.list_url, headers=api_headers, params=params, timeout=15)
            elif site_type == 'guangdong':
                api_headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                    "nsssjss": "IU2MCUs+rpXnvfiM2HydjhZ4bwJMo1rYDf45N3YiwR1x2iqN71dq1PGwb3CJ7MyKCv3YZUY8EnDuKU32BSF4PsSwus6F8NLn/rpsJvVebPeSP0B7BtjGm+Dj1y33c/do3KkPDgj33I3pETokkOxseqcYUaQD8pCqnFU5yzsh/G4=",
                    "sec-ch-ua": '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sign": "4fb4e19392c9b4fc678dacbd55f13d27",
                    "time": str(int(time.time() * 1000)),
                    "url": "/gpcms/rest/web/v2/info/selectInfoForIndex"
                }
                
                # 构造请求参数（根据数据库配置）
                params = {
                    "title": "",
                    "region": "",
                    "siteId": "cd64e06a-21a7-4620-aebc-0576bab7e07a",  # 保持不变
                    "channel": channel if channel else "fca71be5-fc0c-45db-96af-f513e9abda9d",  # 使用传入的channel参数或默认值
                    "currPage": str(page),  # 使用传入的页码参数
                    "pageSize": "10",
                    "noticeType": "",
                    "regionCode": "",
                    "cityOrArea": "",
                    "purchaseManner": "",
                    "openTenderCode": "",
                    "purchaser": "",
                    "agency": "",
                    "purchaseNature": "",
                    "operationStartTime": "2025-01-01 00:00:00",
                    "operationEndTime": "2025-12-31 23:59:59",
                    "verifyCode": verify_code,  # 使用识别出的验证码
                    "subChannel": "false",
                    "_t": str(int(time.time() * 1000))
                }
                
                logger.info(f"发送GET请求获取广东省列表数据，channel: {params['channel']}, 第{page}页...")
                # 使用GET方法发送请求
                response = self.session.get(self.list_url, headers=api_headers, params=params, timeout=15)
            else:  # 其他网站保持原有逻辑
                api_headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                    "nsssjss": "IU2MCUs+rpXnvfiM2HydjhZ4bwJMo1rYDf45N3YiwR1x2iqN71dq1PGwb3CJ7MyKCv3YZUY8EnDuKU32BSF4PsSwus6F8NLn/rpsJvVebPeSP0B7BtjGm+Dj1y33c/do3KkPDgj33I3pETokkOxseqcYUaQD8pCqnFU5yzsh/G4=",
                    "sec-ch-ua": '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sign": "4fb4e19392c9b4fc678dacbd55f13d27",
                    "time": str(int(time.time() * 1000)),
                    "url": "/gpcms/rest/web/v2/info/selectInfoForIndex"
                }
                
                # 构造请求参数（根据数据库配置）
                params = {
                    "title": "",
                    "region": "",
                    "siteId": "cd64e06a-21a7-4620-aebc-0576bab7e07a",
                    "channel": "fca71be5-fc0c-45db-96af-f513e9abda9d",
                    "currPage": "1",
                    "pageSize": "10",
                    "noticeType": "",
                    "regionCode": "",
                    "cityOrArea": "",
                    "purchaseManner": "",
                    "openTenderCode": "",
                    "purchaser": "",
                    "agency": "",
                    "purchaseNature": "",
                    "operationStartTime": "2025-01-01 00:00:00",
                    "operationEndTime": "2025-12-31 23:59:59",
                    "verifyCode": verify_code,  # 使用识别出的验证码
                    "subChannel": "false",
                    "_t": str(int(time.time() * 1000))
                }
                
                logger.info("发送GET请求获取列表数据...")
                # 使用GET方法发送请求
                response = self.session.get(self.list_url, headers=api_headers, params=params, timeout=15)
            
            logger.info(f"列表数据请求状态码: {response.status_code}")
            
            # 检查响应内容
            logger.info(f"响应头: {dict(response.headers)}")
            if response.status_code != 200:
                logger.error(f"请求失败，响应内容: {response.text[:500]}")
                # 保存响应内容用于调试
                with open('debug_list_response.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info("列表响应已保存为 debug_list_response.html")
                
            response.raise_for_status()
            
            # 解析JSON响应
            data = response.json()
            logger.info(f"成功获取列表数据，响应数据结构: {list(data.keys()) if isinstance(data, dict) else '非字典格式'}")
            return data
            
        except Exception as e:
            error_logger.error(f"提交验证码并获取列表数据失败: {e}")
            return None

    def parse_list_data(self, list_data, site_type='guangdong'):
        """解析列表数据"""
        # 增强错误处理
        if not list_data:
            error_logger.error("列表数据为空")
            return []
            
        if not isinstance(list_data, dict):
            error_logger.error(f"列表数据格式不正确，期望字典格式，实际类型: {type(list_data)}")
            return []
            
        # 检查响应码
        code = list_data.get('code')
        msg = list_data.get('msg')
        logger.info(f"响应码: {code}, 消息: {msg}")
        
        # 检查data字段
        data = list_data.get('data')
        if data is None:
            error_logger.error("列表数据中的data字段为None")
            # 检查是否有其他可能包含列表数据的字段
            logger.info(f"检查其他可能的字段: {list(list_data.keys())}")
            # 如果没有data字段，但有其他字段，尝试直接解析
            if 'rows' in list_data:
                data = list_data
            else:
                return []
            
        # 根据站点类型处理不同的数据结构
        if site_type == 'shaanxi':
            # 陕西省政府采购网的data字段直接为列表
            if isinstance(data, list):
                rows = data
                logger.info(f"陕西省政府采购网数据结构，data字段本身就是列表，包含 {len(rows)} 条记录")
            else:
                error_logger.error(f"陕西省政府采购网数据格式不正确，期望列表格式，实际类型: {type(data)}")
                return []
        else:
            # 其他省份（广东、四川）的数据结构
            if not isinstance(data, dict):
                error_logger.error(f"data字段格式不正确，期望字典格式，实际类型: {type(data)}")
                return []
                
            # 检查rows字段
            rows = data.get('rows') if 'rows' in data else (data if isinstance(data, list) else None)
            if rows is None:
                error_logger.error("列表数据中的rows字段为None")
                # 尝试查看data中有哪些字段
                logger.info(f"data字段包含的键: {list(data.keys()) if isinstance(data, dict) else '非字典格式'}")
                # 如果data本身就是列表，直接使用
                if isinstance(data, list):
                    rows = data
                    logger.info(f"data字段本身就是列表，包含 {len(rows)} 条记录")
                else:
                    return []
                
            if not isinstance(rows, list):
                error_logger.error(f"rows字段格式不正确，期望列表格式，实际类型: {type(rows)}")
                return []
            
        logger.info(f"找到 {len(rows)} 条记录")
        
        result_list = []
        for i in rows:
            try:
                if not isinstance(i, dict):
                    logger.warning(f"跳过非字典格式的记录: {type(i)}")
                    continue
                    
                page_data = {}
                # 根据站点类型设置不同的数据提取规则
                if site_type == 'shaanxi':
                    # 陕西省政府采购网的数据提取规则
                    page_data['href'] = f'https://www.ccgp-shaanxi.gov.cn/freecms/site/shaanxi/ggxx/info/2025/{i.get("noticeId", "")}.html?noticeId={i.get("id", "")}'
                    page_data['title'] = i.get('shorttitle', '')
                    page_data['publish_time'] = i.get('noticeTime', '')[:10] if i.get('noticeTime') and len(i.get('noticeTime', '')) >= 10 else ''
                    page_data['zbid'] = i.get('openTenderCode', '')
                    page_data['post_content_href'] = 'https://www.ccgp-shaanxi.gov.cn/freecms/rest/v1/notice/selectInfoByOpenTenderCode.do'
                    
                    # 构造详情页请求参数，按照统一要求设置
                    page_data['post_payload1'] = '{\r\n    "": "",\r\n    "channel": "1eb454a2-7ff7-4a3b-b12c-12acc2685bd1",\r\n    "site": "a7a15d60-de5b-42f2-b35a-7e3efc34e54f",\r\n    "openTenderCode": dict[\'zbid\']\r\n}'
                elif site_type == 'sichuan':
                    # 四川省政府采购网的数据提取规则
                    page_data['href'] = f'https://www.ccgp-sichuan.gov.cn/maincms-web/article?id={i.get("id", "")}'
                    page_data['title'] = i.get('title', '')
                    page_data['publish_time'] = i.get('noticeTime', '')[:10] if i.get('noticeTime') and len(i.get('noticeTime', '')) >= 10 else ''
                    page_data['zbid'] = i.get('noticeId', '')
                    page_data['post_content_href'] = 'https://www.ccgp-sichuan.gov.cn/gpcms/rest/web/v2/info/getInfoById'
                    
                    # 构造详情页请求参数
                    page_data['post_payload1'] = '{\n    "id": dict[\'zbid\'],\n    "_t": "1760775987213"\n}'
                else:  # 默认为广东
                    # 广东省政府采购网的数据提取规则
                    page_data['href'] = f'https://gdgpo.czt.gd.gov.cn/maincms-web/articleGd?id={i.get("id", "")}'
                    page_data['title'] = i.get('title', '')
                    page_data['publish_time'] = i.get('noticeTime', '')[:10] if i.get('noticeTime') and len(i.get('noticeTime', '')) >= 10 else ''
                    page_data['zbid'] = i.get('id', '')
                    page_data['post_content_href'] = 'https://gdgpo.czt.gd.gov.cn/gpcms/rest/web/v2/info/getInfoById'
                    
                    # 构造详情页请求参数，按照统一要求设置
                    page_data['post_payload1'] = '{\n    "id": dict[\'zbid\'],\n    "_t": "1760767926888"\n}'
                
                result_list.append(page_data)
                
            except Exception as e:
                error_logger.error(f"解析单条数据失败: {e}")
                continue
                
        logger.info(f"成功解析{len(result_list)}条数据")
        return result_list

    def save_to_database(self, parsed_data, site_type='guangdong'):
        """将解析后的数据保存到数据库"""
        try:
            # 导入数据库操作模块
            from src.bid_mysql import get_db_connection
            
            if not parsed_data:
                logger.warning("没有数据需要保存到数据库")
                return 0
                
            connection = None
            try:
                connection = get_db_connection()
                cursor = connection.cursor()
                
                # 批量插入数据到t_bidcollect_end_level_page表
                insert_count = 0
                for data in parsed_data:
                    try:
                        # 构造SQL语句，添加isexec字段并设置默认值为0
                        sql = """
                        INSERT INTO t_bidcollect_end_level_page 
                        (webname, href, p_href, publish_time, method, find_content, zbid, 
                         post_content_href, post_payload1, post_headers, detail_method, one_level_page_id, isexec)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        # 根据站点类型设置不同的保存规则
                        if site_type == 'shaanxi':
                            # 陕西省政府采购网的保存规则（根据新要求修改）
                            values = (
                                data['title'],  # webname - 使用数据中的标题
                                data['href'],  # href
                                self.captcha_url,  # p_href (父链接)
                                data['publish_time'],  # publish_time
                                '',  # method
                                'soup.find(\'div\',class_=\'protect\')',  # find_content - 根据要求写死
                                None,  # zbid - 根据要求设置为NULL
                                None,  # post_content_href - 根据要求设置为NULL
                                None,  # post_payload1 - 根据要求设置为NULL
                                '{\n    "Accept": "*/*",\n    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",\n    "Connection": "keep-alive",\n    "Content-Type": "application/json;charset=utf-8",\n    "Referer": "https://www.ccgp-shaanxi.gov.cn/cms-sx/site/shanxi/xxgg/index.html?xxggType=123&noticeType=00101",\n    "Sec-Fetch-Dest": "empty",\n    "Sec-Fetch-Mode": "cors",\n    "Sec-Fetch-Site": "same-origin",\n    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",\n    "X-Requested-With": "XMLHttpRequest",\n    "nsssjss": "GeKi1sSJFUpyRBXDeoEckK0ydUApcc8k062uxcIMo1pS4KrupDoAOfnJQ6/EELjOU84fSQisqlqjIu95bZjmKCyf2B+Go9k6CdL4xYUfRZCe+7RWvYdmKy7WwEW5iGPlH1ptPYrB42UDuyp9rzf4tD+qIPPIf3Mma3dBMRX/1aw=",\n    "sec-ch-ua": "\\"Microsoft Edge\\";v=\\"141\\", \\"Not?A_Brand\\";v=\\"8\\", \\"Chromium\\";v=\\"141\\"",\n    "sec-ch-ua-mobile": "?0",\n    "sec-ch-ua-platform": "\\"Windows\\"",\n    "sign": "a80d71fd43f404c61891f123480e9332",\n    "time": "1760777425263",\n    "url": "/freecms/rest/v1/notice/selectInfoMoreChannel.do"\n}',  # post_headers
                                'get_html',  # detail_method - 根据要求设置为get_html
                                None,  # one_level_page_id 设置为NULL
                                0   # isexec 设置为0
                            )
                        elif site_type == 'sichuan':
                            # 四川省政府采购网的保存规则
                            values = (
                                data['title'],  # webname - 使用数据中的标题
                                data['href'],  # href
                                self.captcha_url,  # p_href (父链接)
                                data['publish_time'],  # publish_time
                                '',  # method
                                '[\'data\'][\'content\']',  # find_content
                                data['zbid'],  # zbid
                                data['post_content_href'],  # post_content_href
                                '{\n    "id": dict[\'zbid\'],\n    "_t": "1760775987213"\n}',  # post_payload1 - 使用规则中的原始字符串
                                '{\n    "Accept": "*/*",\n    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",\n    "Connection": "keep-alive",\n    "Sec-Fetch-Dest": "empty",\n    "Sec-Fetch-Mode": "cors",\n    "Sec-Fetch-Site": "same-origin",\n    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",\n    "nsssjss": "U/lPUB8lHsQWLyVSkJf7JN0V4zgrmIi3RvEH9DAYATuM183EBAxf2dtMpyasaRlxrw0KYIcui33IrHXcGwdM0YINy5zLTieHCgIAmf+9bPWf4SBa/BQ2N25U5+WgjgnXNXqW/tmJep96oqjBlaYLHY3kT9Cw+aKeN6qOSJYI+98=",\n    "sec-ch-ua": "\\"Microsoft Edge\\";v=\\"141\\", \\"Not?A_Brand\\";v=\\"8\\", \\"Chromium\\";v=\\"141\\"",\n    "sec-ch-ua-mobile": "?0",\n    "sec-ch-ua-platform": "\\"Windows\\"",\n    "sign": "63ed205307e320191d668de67cc7452a",\n    "time": "1760775807242",\n    "url": "/gpcms/rest/web/v2/info/selectInfoForIndex"\n}',  # post_headers
                                'get_json',  # detail_method
                                None,  # one_level_page_id 设置为NULL
                                0   # isexec 设置为0
                            )
                        else:  # 默认为广东
                            # 广东省政府采购网的保存规则，按照统一要求设置post_payload1
                            values = (
                                data['title'],  # webname - 使用数据中的标题
                                data['href'],  # href
                                self.captcha_url,  # p_href (父链接，这里使用验证码页面URL)
                                data['publish_time'],  # publish_time
                                '',  # method
                                '[\'data\'][\'content\']',  # find_content (根据数据库配置)
                                data['zbid'],  # zbid
                                data['post_content_href'],  # post_content_href
                                '{\n    "id": dict[\'zbid\'],\n    "_t": "1760767926888"\n}',  # post_payload1 - 按照统一要求设置
                                '{\n    "Accept": "*/*",\n    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",\n    "Connection": "keep-alive",\n    "Sec-Fetch-Dest": "empty",\n    "Sec-Fetch-Mode": "cors",\n    "Sec-Fetch-Site": "same-origin",\n    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",\n    "sec-ch-ua": "\\"Microsoft Edge\\";v=\\"141\\", \\"Not?A_Brand\\";v=\\"8\\", \\"Chromium\\";v=\\"141\\"",\n    "sec-ch-ua-mobile": "?0",\n    "sec-ch-ua-platform": "\\"Windows\\""\n}',  # post_headers
                                'get_json',  # detail_method
                                None,  # one_level_page_id 设置为NULL
                                0   # isexec 设置为0
                            )
                        
                        cursor.execute(sql, values)
                        insert_count += 1
                        
                    except Exception as e:
                        error_logger.error(f"保存单条数据失败: {e}")
                        continue
                
                connection.commit()
                logger.info(f"成功保存 {insert_count} 条数据到数据库")
                return insert_count
                
            except Exception as e:
                if connection:
                    connection.rollback()
                error_logger.error(f"保存数据到数据库失败: {e}")
                return 0
                
            finally:
                if connection:
                    connection.close()
                    
        except Exception as e:
            error_logger.error(f"数据库操作异常: {e}")
            return 0

    def collect_data(self, site_type='guangdong'):
        """完整的采集流程"""
        site_names = {
            'shaanxi': '陕西省政府采购网',
            'sichuan': '四川省政府采购网',
            'guangdong': '广东省政府采购网'
        }
        site_name = site_names.get(site_type, '未知网站')
        logger.info(f"开始采集{site_name}数据")
        
        # 对于四川省政府采购网，需要采集多个列表页
        if site_type == 'sichuan':
            # 定义需要采集的noticeType列表和对应的页数范围
            notice_types_pages = [
                ("00101", 1, 422),           # 第1个列表，采集1-422页
                ("001052,001053,00105B", 1, 1),  # 第2个列表，采集1页
                ("00102", 1, 368),           # 第3个列表，采集1-368页
                ("00103", 1, 121),           # 第4个列表，采集1-121页
                ("001004,001006", 1, 114),   # 第5个列表，采集1-114页
                ("206011,206012", 1, 4),     # 第6个列表，采集1-4页
                ("206014,206024", 1, 2),     # 第7个列表，采集1-2页
                ("206016", 1, 1),            # 第8个列表，采集1页
                ("206019,206028", 1, 1),     # 第9个列表，采集1页
                ("206015", 1, 1),            # 第10个列表，采集1页
                ("001051,00105F", 1, 354),   # 第11个列表，采集1-354页
                ("59,5E", 1, 1223),          # 第12个列表，采集1-1223页
                ("001054,00100B", 1, 8),     # 第13个列表，采集1-8页
                ("00105A,001009,00100C", 1, 182),  # 第14个列表，采集1-182页
                ("001062", 1, 1),            # 第15个列表，采集1页
                ("001031,001032", 1, 1)      # 第16个列表，采集1页
            ]
            all_parsed_data = []
            
            for i, (notice_type, start_page, end_page) in enumerate(notice_types_pages):
                logger.info(f"开始采集四川省政府采购网，列表 {i+1}: noticeType={notice_type}, 页数={start_page}-{end_page}")
                
                # 遍历所有页数
                for page in range(start_page, end_page + 1):
                    logger.info(f"开始采集四川省政府采购网，列表 {i+1}，第{page}页")
                    
                    # 1. 识别验证码（带缓存机制）
                    verify_code = self.recognize_captcha(site_type, notice_type=notice_type)
                    if not verify_code:
                        error_logger.error(f"验证码识别失败，列表 {i+1}，第{page}页")
                        continue
                        
                    logger.info(f"验证码识别成功: {verify_code}")
                    
                    # 2. 提交验证码并获取列表数据
                    list_data = self.submit_captcha_and_get_list(verify_code, site_type, notice_type, page=page)
                    if not list_data:
                        error_logger.error(f"提交验证码并获取列表数据失败，列表 {i+1}，第{page}页")
                        continue
                        
                    # 3. 解析列表数据
                    parsed_data = self.parse_list_data(list_data, site_type)
                    if not parsed_data:
                        error_logger.error(f"解析列表数据失败，列表 {i+1}，第{page}页")
                        continue
                        
                    # 如果解析到的数据为空，说明已经没有更多数据了，可以跳出循环
                    if len(parsed_data) == 0:
                        logger.info(f"列表 {i+1}，第{page}页没有数据，停止采集此列表")
                        break
                        
                    all_parsed_data.extend(parsed_data)
                    logger.info(f"采集完成，列表 {i+1}，第{page}页，获取{len(parsed_data)}条数据")
            
            # 4. 保存所有数据到数据库
            if all_parsed_data:
                saved_count = self.save_to_database(all_parsed_data, site_type)
                if saved_count > 0:
                    logger.info(f"成功保存 {saved_count} 条数据到数据库")
                else:
                    logger.warning("没有数据被保存到数据库")
                    
                logger.info(f"四川省政府采购网采集完成，共获取{len(all_parsed_data)}条数据，保存{saved_count}条数据")
                return all_parsed_data
            else:
                logger.error("四川省政府采购网采集失败，没有获取到任何数据")
                return None
        # 对于陕西省政府采购网，也需要采集多个列表页
        elif site_type == 'shaanxi':
            # 定义需要采集的noticeType列表和regionCode列表
            notice_types = ["001011,001012,001013,001014,001016,001019", "001021,001022,001023,001024,001025,001026,001029,001006", "001031,001032", "001004,001006", "001053,001052,00105B", "001051,00105F", "59,5E", "001054,00100B", "00105A,001009,00100C", "001062"]
            region_codes = ["610001", "610100", "610200", "610300", "610400", "610500", "610600", "610700", "610800", "610900", "611000", "611100"]
            all_parsed_data = []
            
            # 定义截止日期
            cutoff_date = "2025-09-22"
            
            # 遍历所有noticeType和regionCode的组合
            for notice_type in notice_types:
                for region_code in region_codes:
                    logger.info(f"开始采集陕西省政府采购网，noticeType: {notice_type}, regionCode: {region_code}")
                    
                    # 初始化页码
                    curr_page = 1
                    
                    # 循环采集，直到遇到截止日期或更早的日期
                    while True:
                        logger.info(f"开始采集陕西省政府采购网，noticeType: {notice_type}, regionCode: {region_code}，第{curr_page}页")
                        
                        # 1. 识别验证码（带缓存机制）
                        verify_code = self.recognize_captcha(site_type, notice_type=notice_type)
                        if not verify_code:
                            error_logger.error(f"验证码识别失败，noticeType: {notice_type}, regionCode: {region_code}，第{curr_page}页")
                            # 尝试下一页
                            curr_page += 1
                            continue
                            
                        logger.info(f"验证码识别成功: {verify_code}")
                        
                        # 2. 提交验证码并获取列表数据
                        list_data = self.submit_captcha_and_get_list(verify_code, site_type, notice_type, region_code)
                        if not list_data:
                            error_logger.error(f"提交验证码并获取列表数据失败，noticeType: {notice_type}, regionCode: {region_code}，第{curr_page}页")
                            # 尝试下一页
                            curr_page += 1
                            continue
                            
                        # 3. 解析列表数据
                        parsed_data = self.parse_list_data(list_data, site_type)
                        if not parsed_data:
                            error_logger.error(f"解析列表数据失败，noticeType: {notice_type}, regionCode: {region_code}，第{curr_page}页")
                            # 尝试下一页
                            curr_page += 1
                            continue
                        
                        # 如果解析到的数据为空，说明已经没有更多数据了，可以跳出循环
                        if len(parsed_data) == 0:
                            logger.info(f"noticeType: {notice_type}, regionCode: {region_code}，第{curr_page}页没有数据，停止采集此列表")
                            break
                        
                        # 检查是否有数据的发布日期在截止日期或更早
                        should_stop = False
                        for item in parsed_data:
                            publish_time = item.get('publish_time', '')
                            if publish_time and publish_time <= cutoff_date:
                                logger.info(f"发现发布日期 {publish_time} 在截止日期 {cutoff_date} 或更早，停止采集当前列表")
                                should_stop = True
                                # 只添加发布日期在截止日期之后的数据
                                filtered_data = [item for item in parsed_data if item.get('publish_time', '') > cutoff_date]
                                if filtered_data:
                                    all_parsed_data.extend(filtered_data)
                                    logger.info(f"添加了 {len(filtered_data)} 条在截止日期之后的数据")
                                break
                        
                        # 如果需要停止，则跳出循环
                        if should_stop:
                            break
                        
                        # 添加所有数据
                        all_parsed_data.extend(parsed_data)
                        logger.info(f"采集完成，noticeType: {notice_type}, regionCode: {region_code}，第{curr_page}页，获取{len(parsed_data)}条数据")
                        
                        # 继续下一页
                        curr_page += 1
            
            # 4. 保存所有数据到数据库
            if all_parsed_data:
                saved_count = self.save_to_database(all_parsed_data, site_type)
                if saved_count > 0:
                    logger.info(f"成功保存 {saved_count} 条数据到数据库")
                else:
                    logger.warning("没有数据被保存到数据库")
                    
                logger.info(f"陕西省政府采购网采集完成，共获取{len(all_parsed_data)}条数据，保存{saved_count}条数据")
                return all_parsed_data
            else:
                logger.error("陕西省政府采购网采集失败，没有获取到任何数据")
                return None
        # 对于广东省政府采购网，需要采集多个列表页
        elif site_type == 'guangdong':
            # 定义需要采集的channel参数和对应的页数范围
            channels_pages = [
                ("fca71be5-fc0c-45db-96af-f513e9abda9d", 1, 1000),  # 第一个列表，采集1-1000页
                ("3b49b9ba-48b6-4220-9e8b-eb89f41e9d66", 1, 1000),  # 第二个列表，采集1-1000页
                ("82fad126-7447-43a2-94aa-d42647349ae9", 1, 27)     # 第三个列表，采集1-27页
            ]
            all_parsed_data = []
            
            for i, (channel, start_page, end_page) in enumerate(channels_pages):
                logger.info(f"开始采集广东省政府采购网，列表 {i+1}: channel={channel}, 页数={start_page}-{end_page}")
                
                # 遍历所有页数
                for page in range(start_page, end_page + 1):
                    logger.info(f"开始采集广东省政府采购网，列表 {i+1}，第{page}页")
                    
                    # 1. 识别验证码（带缓存机制）
                    verify_code = self.recognize_captcha(site_type, channel=channel)
                    if not verify_code:
                        error_logger.error(f"验证码识别失败，列表 {i+1}，第{page}页")
                        continue
                        
                    logger.info(f"验证码识别成功: {verify_code}")
                    
                    # 2. 提交验证码并获取列表数据
                    list_data = self.submit_captcha_and_get_list(verify_code, site_type, channel=channel, page=page)
                    if not list_data:
                        error_logger.error(f"提交验证码并获取列表数据失败，列表 {i+1}，第{page}页")
                        continue
                        
                    # 3. 解析列表数据
                    parsed_data = self.parse_list_data(list_data, site_type)
                    if not parsed_data:
                        error_logger.error(f"解析列表数据失败，列表 {i+1}，第{page}页")
                        continue
                        
                    # 如果解析到的数据为空，说明已经没有更多数据了，可以跳出循环
                    if len(parsed_data) == 0:
                        logger.info(f"列表 {i+1}，第{page}页没有数据，停止采集此列表")
                        break
                        
                    all_parsed_data.extend(parsed_data)
                    logger.info(f"采集完成，列表 {i+1}，第{page}页，获取{len(parsed_data)}条数据")
            
            # 4. 保存所有数据到数据库
            if all_parsed_data:
                saved_count = self.save_to_database(all_parsed_data, site_type)
                if saved_count > 0:
                    logger.info(f"成功保存 {saved_count} 条数据到数据库")
                else:
                    logger.warning("没有数据被保存到数据库")
                    
                logger.info(f"广东省政府采购网采集完成，共获取{len(all_parsed_data)}条数据，保存{saved_count}条数据")
                return all_parsed_data
            else:
                logger.error("广东省政府采购网采集失败，没有获取到任何数据")
                return None
        else:
            # 其他网站保持原有逻辑
            # 1. 识别验证码（带缓存机制）
            verify_code = self.recognize_captcha(site_type)
            if not verify_code:
                error_logger.error("验证码识别失败")
                return None
                
            logger.info(f"验证码识别成功: {verify_code}")
            
            # 2. 提交验证码并获取列表数据
            list_data = self.submit_captcha_and_get_list(verify_code, site_type)
            if not list_data:
                error_logger.error("提交验证码并获取列表数据失败")
                return None
                
            # 3. 解析列表数据
            parsed_data = self.parse_list_data(list_data, site_type)
            if not parsed_data:
                error_logger.error("解析列表数据失败")
                return None
                
            # 4. 保存数据到数据库
            saved_count = self.save_to_database(parsed_data, site_type)
            if saved_count > 0:
                logger.info(f"成功保存 {saved_count} 条数据到数据库")
            else:
                logger.warning("没有数据被保存到数据库")
                
            logger.info(f"采集完成，共获取{len(parsed_data)}条数据，保存{saved_count}条数据")
            return parsed_data

def collect_all_sites():
    """采集所有已配置的网站"""
    logger.info("开始采集所有已配置的网站")
    
    # 采集广东省政府采购网
    logger.info("开始采集广东省政府采购网")
    gd_collector = GDGPOCollector('guangdong')
    gd_result = gd_collector.collect_data('guangdong')
    if gd_result:
        logger.info(f"广东省政府采购网采集成功，获取{len(gd_result)}条数据")
    else:
        error_logger.error("广东省政府采购网采集失败")
    
    # 采集四川省政府采购网
    logger.info("开始采集四川省政府采购网")
    sc_collector = GDGPOCollector('sichuan')
    sc_result = sc_collector.collect_data('sichuan')
    if sc_result:
        logger.info(f"四川省政府采购网采集成功，获取{len(sc_result)}条数据")
    else:
        error_logger.error("四川省政府采购网采集失败")
        
    # 采集陕西省政府采购网
    logger.info("开始采集陕西省政府采购网")
    sn_collector = GDGPOCollector('shaanxi')
    sn_result = sn_collector.collect_data('shaanxi')
    if sn_result:
        logger.info(f"陕西省政府采购网采集成功，获取{len(sn_result)}条数据")
    else:
        error_logger.error("陕西省政府采购网采集失败")
    
    logger.info("所有网站采集完成")

def main():
    """主函数 - 采集所有已配置的网站"""
    collect_all_sites()

if __name__ == '__main__':
    main()