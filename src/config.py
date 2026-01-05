# -*- coding: utf-8 -*-
# 蘑菇代理 API appkey 为空代表不用代理直接爬
# appkey = ""
import random
import manager
api_tyle="get_ip_al"
appkey="5ec7ce4740e848d4938c81bac17a7d3c"

# mysql 数据库  配置

host = '127.0.0.1'
port = 3306
user = 'bid'
passwd = '123456'
db = 'app_phantasm'
charset = 'utf8'
# 翻页设置
page_size = 20
# 最大线程数
max_threads = 50  # 可以根据实际需求调整
max_list_threads = 1  # 可以根据实际需求调整
max_detail_threads = 1  # 可以根据实际需求调整
# 数据库连接池配置
DB_MAX_CONNECTIONS = max_threads * 2
DB_MIN_CACHED = max_threads // 2
DB_MAX_CACHED = max_threads
DB_MAX_SHARED = max_threads // 2
DB_MAX_USAGE = 10000

# 爬虫延迟设置
min_delay = 3
max_delay = 6
time_sleep= 5

# 爬虫 错误处理配置
# 爬取一个页面被反爬 的换代理IP重试次数
retries = 3
# 爬取一个页面请求异常 的换代理IP重试次数
html_retries = 5
# 爬取一个页面超时时间 （秒）
request_timeout = 10

# 当没有数据需要爬取时的重试间隔时间（秒）
retry_interval = 1800



