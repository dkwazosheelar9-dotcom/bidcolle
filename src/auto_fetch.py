import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

# --- 数据库配置 (请修改这里) ---
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'bid',
    'password': '123456',
    'db': 'app_phantasm',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# --- 请求头配置 ---
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def get_connection():
    return pymysql.connect(**DB_CONFIG)


def fetch_html(url, referer=None):
    """通用抓取函数"""
    headers = DEFAULT_HEADERS.copy()
    if referer:
        headers['Referer'] = referer

    # 自动设置 Host
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        headers['Host'] = parsed.netloc
    except:
        pass

    try:
        resp = requests.get(url, headers=headers, timeout=15, verify=False)
        # 自动处理编码
        if resp.encoding == 'ISO-8859-1':
            resp.encoding = resp.apparent_encoding
        return resp.text
    except Exception as e:
        print(f"   [Error] 请求失败 {url}: {e}")
        return None


def extract_first_detail_link(html, base_url):
    """自动提取第一个看起来像详情页的链接"""
    soup = BeautifulSoup(html, 'html.parser')
    # 策略：优先找 li 标签下的 a 标签，且文本长度 > 4 的
    candidates = soup.select('li a')
    if not candidates:
        candidates = soup.select('tr a')  # 表格布局

    for a in candidates:
        href = a.get('href')
        text = a.get_text(strip=True)
        if href and len(text) > 4 and 'javascript' not in href:
            return urljoin(base_url, href)

    # 如果没找到，尝试页面任意正文区域的长链接
    for a in soup.find_all('a'):
        href = a.get('href')
        text = a.get_text(strip=True)
        if href and len(text) > 8 and 'javascript' not in href:
            return urljoin(base_url, href)

    return None


def process_task():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 1. 获取待处理任务
            sql = "SELECT id, url FROM t_static_sample_task WHERE status = 0"
            cursor.execute(sql)
            tasks = cursor.fetchall()

            print(f"🚀 发现 {len(tasks)} 个待采集任务...")

            for task in tasks:
                task_id = task['id']
                list_url = task['url']
                print(f"\n[Task {task_id}] 正在处理: {list_url}")

                # 2. 抓取列表页
                list_html = fetch_html(list_url)
                if not list_html:
                    cursor.execute("UPDATE t_static_sample_task SET status = -1 WHERE id = %s", (task_id,))
                    conn.commit()
                    continue

                # 3. 解析详情页链接
                detail_url = extract_first_detail_link(list_html, list_url)
                detail_html = None

                if detail_url:
                    print(f"   -> 自动识别详情页: {detail_url}")
                    # 4. 抓取详情页
                    detail_html = fetch_html(detail_url, referer=list_url)
                else:
                    print("   -> ⚠️ 未能识别出详情页链接，仅保存列表页")

                # 5. 保存回数据库
                status = 1 if list_html else -1
                update_sql = """
                    UPDATE t_static_sample_task 
                    SET list_html = %s, detail_url = %s, detail_html = %s, status = %s 
                    WHERE id = %s
                """
                cursor.execute(update_sql, (list_html, detail_url, detail_html, status, task_id))
                conn.commit()
                print(f"   ✅ 保存成功！")

                time.sleep(1)  # 礼貌延时

    finally:
        conn.close()


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings()
    process_task()