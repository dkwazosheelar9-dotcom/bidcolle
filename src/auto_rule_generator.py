# -*- coding: utf-8 -*-
import json
import time
import requests
import logging
import pymysql
import os

# ================= 1. 修复代理报错 (必须放在最前面) =================
# 强制移除系统代理设置，确保 requests 直连阿里云
os.environ['NO_PROXY'] = '*'
for k in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
    if k in os.environ:
        os.environ.pop(k)

# 导入采集逻辑
from src import config
from src.bid_mysql import get_db_connection
from src.auto_fetch import process_task as run_auto_fetch

# ================= 2. 配置区域 =================
LLM_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
LLM_API_KEY = "sk-4b983791a8cb4693b853b389e5cceebe"
LLM_MODEL = "qwen-plus"

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 白名单方法
VALID_METHODS = ['get_html', 'post_html', 'get_json', 'post_json', 'selenium']

# 3. 定义 JSON Schema (严格模式)
SPIDER_RULE_SCHEMA = {
    "name": "spider_rule_config",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "webname": { "type": "string", "description": "格式：网站名 - 栏目名" },
            "webadd": { "type": "string" },
            "href": { "type": "string" },
            "quchong_weburl": { "type": "string" },
            "is_active": { "type": "integer", "const": 1 },
            "post_headers": {
                "type": "object",
                "properties": {
                    "Accept": {"type": "string"},
                    "Host": {"type": "string"},
                    "Referer": {"type": "string"},
                    "User-Agent": {"type": "string"}
                },
                "required": ["Accept", "Host", "Referer", "User-Agent"],
                "additionalProperties": True
            },
            "list_method": { "type": "string", "enum": ["get_html", "post_html"], "default": "get_html" },
            "detail_method": { "type": "string", "enum": ["get_html", "post_html"], "default": "get_html" },
            "find_list": { "type": "string", "description": "返回ResultSet的eval代码" },
            "find_href": { "type": "string", "description": "详情链接提取规则" },
            "find_title": { "type": "string" },
            "find_pub_time": { "type": "string" },
            "find_content": { "type": "string" },
            "post_payload1": { "type": ["string", "null"] },
            "post_content_href": { "type": ["string", "null"] }
        },
        "required": [
            "webname", "webadd", "href", "quchong_weburl", "is_active",
            "post_headers", "list_method", "detail_method",
            "find_list", "find_href", "find_title", "find_pub_time", "find_content",
            "post_payload1", "post_content_href"
        ],
        "additionalProperties": False
    }
}

# 4. 升级版 Prompt (包含嵌套列表案例)
PROMPT_TEMPLATE = """
# Role
Python 爬虫架构师。你的任务是分析 HTML 生成 BeautifulSoup 提取规则。

# ⚠️ Core Strategy: Pattern Recognition (模式识别)
请注意甄别以下三种常见列表模式：
1. **标准列表 (UL/LI)**: `soup.find('ul', class_='news').find_all('li')`
2. **表格列表 (Table/TR)**: `soup.find('table', id='list').find_all('tr')`
3. **DIV 块级列表**: `soup.find('div', class_='list').find_all('div', class_='item')`

# 🌰 Deep Nested Example (深层嵌套 - 重点关注)
当 HTML 结构为 `li > div > a` 时：
```html
<ul class="list">
  <li class="item">
     <div class="title-box"> <a href="...">标题</a> </div>
     <span class="date">...</span>
  </li>
</ul>

你的规则必须穿透中间层：

    "find_list": "soup.find('ul', class_='list').find_all('li')"

    "find_href": "item.find('div', class_='title-box').find('a')['href']" <-- 注意这里!

    "find_title": "item.find('div', class_='title-box').find('a').get_text(strip=True)"

Constraints (约束)

    find_list: 定位到最小公共父级容器。

    webname: "网站名 - 栏目名"。

    Anti-Interference: 严禁抓取 nav, sidebar, footer, related 区域。

Input Data

"""

def check_db_schema():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DESCRIBE t_bidcollect_one_level_page")
    except Exception:
        pass
    finally:
        conn.close()


def clean_rule_data(rule_data):
    """数据清洗: 空值转None, 强制小写, 移除Method引号"""
    nullable_fields = [
        'post_payload1', 'post_content_href',
        'find_list', 'find_href', 'find_title', 'find_pub_time', 'find_content'
    ]

    # 1. 清洗空字符串
    for key, val in rule_data.items():
        if key in nullable_fields and isinstance(val, str):
            val = val.strip()
            if not val or val.lower() in ['null', 'none']:
                rule_data[key] = None

    # 2. 强制清洗 Method 字段
    for m in ['list_method', 'detail_method']:
        val = str(rule_data.get(m, 'get_html')).strip().lower()
        val = val.replace("'", "").replace('"', "")
        if val not in VALID_METHODS:
            val = 'get_html'
        rule_data[m] = val

    return rule_data


def call_llm_api(input_json_str):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LLM_API_KEY}"
    }

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system",
             "content": "You are a crawler expert. Handle nested tags (e.g. li > div > a). Output JSON."},
            {"role": "user", "content": PROMPT_TEMPLATE + input_json_str}
        ],
        "temperature": 0.01,
        "response_format": {"type": "json_schema", "json_schema": SPIDER_RULE_SCHEMA}
    }

    try:
        response = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=60)
        if response.status_code != 200:
            logger.error(f"API Error: {response.text}")
            return None

        content = response.json()['choices'][0]['message']['content']
        rule_json = json.loads(content)

        # 强制覆盖 URL (防止 AI 幻觉)
        input_data = json.loads(input_json_str)
        if input_data.get('url'):
            for f in ['href', 'webadd', 'quchong_weburl']:
                rule_json[f] = input_data['url']

        return clean_rule_data(rule_json)
    except Exception as e:
        logger.error(f"API Exception: {e}")
        return None


def save_rule_to_db(rule_data, conn):
    """智能入库逻辑: 尝试完整插入，若报Data truncated则降级插入"""
    post_headers_str = json.dumps(rule_data.get('post_headers', {}), ensure_ascii=False)

    # 方案 A: 完整 SQL
    sql_full = """
        INSERT INTO t_bidcollect_one_level_page (
            webname, webadd, href, quchong_weburl, 
            is_active, list_method, detail_method,
            find_list, find_href, find_title, find_pub_time, find_content,
            post_headers, post_payload1, post_content_href,
            create_time, update_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    """
    params_full = (
        rule_data.get('webname'), rule_data.get('webadd'), rule_data.get('href'), rule_data.get('quchong_weburl'),
        1,
        rule_data.get('list_method'), rule_data.get('detail_method'),
        rule_data.get('find_list'), rule_data.get('find_href'), rule_data.get('find_title'),
        rule_data.get('find_pub_time'), rule_data.get('find_content'),
        post_headers_str, rule_data.get('post_payload1'), rule_data.get('post_content_href')
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_full, params_full)
        conn.commit()
        logger.info(f"✅ 入库成功: {rule_data.get('webname')}")
        return True
    except pymysql.err.DataError as e:
        # 捕获 Error 1265: Data truncated
        if e.args[0] == 1265:
            logger.warning(f"⚠️ 捕获截断错误, 正在降级重试 (忽略 Method 字段)...")
            return save_rule_fallback(rule_data, conn, post_headers_str)
        else:
            logger.error(f"❌ 数据错误: {e}")
            conn.rollback()
            return False
    except pymysql.err.IntegrityError:
        logger.warning(f"⚠️ 跳过重复: {rule_data.get('href')}")
        return True
    except Exception as e:
        logger.error(f"❌ 未知错误: {e}")
        conn.rollback()
        return False


def save_rule_fallback(rule_data, conn, post_headers_str):
    # 方案 B: 降级 SQL (不含 list_method, detail_method)
    sql_safe = """
        INSERT INTO t_bidcollect_one_level_page (
            webname, webadd, href, quchong_weburl, 
            is_active,
            find_list, find_href, find_title, find_pub_time, find_content,
            post_headers, post_payload1, post_content_href,
            create_time, update_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    """
    params_safe = (
        rule_data.get('webname'), rule_data.get('webadd'), rule_data.get('href'), rule_data.get('quchong_weburl'),
        1,
        rule_data.get('find_list'), rule_data.get('find_href'), rule_data.get('find_title'),
        rule_data.get('find_pub_time'), rule_data.get('find_content'),
        post_headers_str, rule_data.get('post_payload1'), rule_data.get('post_content_href')
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_safe, params_safe)
        conn.commit()
        logger.info(f"✅ [降级模式] 入库成功")
        return True
    except Exception as e:
        logger.error(f"❌ [降级模式] 依然失败: {e}")
        conn.rollback()
        return False


def main():
    check_db_schema()

    logger.info("Step 1: 运行智能采集 (Auto Fetch)...")
    run_auto_fetch()

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT id, url, list_html, detail_html FROM t_static_sample_task WHERE status = 1"
            cursor.execute(sql)
            tasks = cursor.fetchall()

        logger.info(f"🚀 待生成规则任务数: {len(tasks)}")

        for task in tasks:
            task_id = task['id']
            url = task['url']
            logger.info(f"Processing Task {task_id}: {url}")

            # 准备数据 (截取前 25k 字符)
            input_data = {
                "url": url,
                "list_html": task['list_html'][:25000],
                "detail_html": task['detail_html'][:25000] if task['detail_html'] else ""
            }

            # 调用 AI
            rule_json = call_llm_api(json.dumps(input_data, ensure_ascii=False))

            if rule_json:
                success = save_rule_to_db(rule_json, conn)
                # 只有入库成功(包含跳过重复)才更新状态
                if success:
                    with conn.cursor() as cursor:
                        cursor.execute("UPDATE t_static_sample_task SET status = 2 WHERE id = %s", (task_id,))
                    conn.commit()

            time.sleep(1)  # 避免 API 速率限制

    finally:
        conn.close()
        logger.info("🎉 所有流程执行完毕")


if __name__ == "__main__":
    main()