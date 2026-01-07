# -*- coding: utf-8 -*-
import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import time
import re
import warnings

# 忽略 HTTPS 证书警告
warnings.filterwarnings("ignore")

# --- 数据库配置 ---
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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def get_connection():
    return pymysql.connect(**DB_CONFIG)


def fetch_html(url, referer=None):
    headers = DEFAULT_HEADERS.copy()
    if referer:
        headers['Referer'] = referer
    try:
        parsed = urlparse(url)
        headers['Host'] = parsed.netloc
        # verify=False 关键：防止部分政府网站证书过期报错
        resp = requests.get(url, headers=headers, timeout=25, verify=False)

        # 自动处理乱码
        if resp.encoding == 'ISO-8859-1':
            resp.encoding = resp.apparent_encoding
        return resp.text
    except Exception as e:
        print(f"   [Error] 请求失败 {url}: {e}")
        return None


def extract_first_detail_link(html, base_url):
    """
    V4.0 终极识别算法 (全页扫描 + 权重评分)
    解决: 之前版本因误判 header 区域而遗漏正文链接的问题
    """
    if not html: return None
    soup = BeautifulSoup(html, 'html.parser')

    candidates = []

    # 策略变更：扫描全页所有链接，依靠评分过滤，而不是预先缩小范围
    # 这样可以防止因页面布局奇特（如 phpweb）导致的漏抓
    all_links = soup.find_all('a', href=True)

    for a in all_links:
        href = a['href'].strip()
        text = a.get_text(strip=True)

        # --- A. 基础清洗 ---
        # 排除无效链接
        if not href or href.startswith(('#', 'javascript', 'mailto', 'tel')): continue

        # 拼接绝对路径
        full_url = urljoin(base_url, href)

        # 排除非网页文件
        if full_url.lower().endswith(
                ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.jpg', '.png', '.css', '.js')):
            continue

        # 排除过短文本 (如 "更多", "GO", "More")
        if len(text) < 4: continue

        # --- B. 评分系统 ---
        score = 0

        # 1. 标题长度权重 (标题通常较长)
        score += len(text) * 0.5

        # 2. 结构特征探测 (解决您的 li > div > a 问题)
        # 只要父级包含 title/tit/subject，哪怕没有日期，也是极高概率的详情页
        parents = list(a.parents)[:4]  # 向上查4层
        is_title_structure = False
        for p in parents:
            p_cls = str(p.get('class', [])).lower()
            p_id = str(p.get('id', '')).lower()
            # 您的案例命中 'title'
            if any(k in p_cls or k in p_id for k in ['title', 'tit', 'subject', 'name', 'bt', 'header', 'link']):
                is_title_structure = True
                break

        if is_title_structure:
            score += 30  # 命中结构特征，加高分

        # 3. 区域加分 (软性限制)
        # 如果链接在 content/list/query 这种大容器里，加分
        # 这比直接限制区域更安全
        top_container = a.find_parent('div', id=re.compile('(content|main|list|query|center|news)', re.I))
        if top_container:
            score += 10

        # 4. 列表标签特征
        if a.find_parent(['li', 'tr', 'dd']):
            score += 10

        # 5. 日期特征 (辅助)
        if re.search(r'\d{4}[-/年]\d{1,2}', str(a.parent)) or re.search(r'\d{4}[-/年]\d{1,2}', str(a.parent.parent)):
            score += 15

        # 6. 黑名单减分 (排除导航、页脚、侧栏广告)
        # 您的案例中有 sidebar 链接如 "招标公告"，长度为4，score=2
        # 而正文链接长度>20，score>40，依然稳胜
        if any(k in text for k in ['首页', '主页', '关于', '联系', '更多', '下一页', '上一页', '尾页', '下载', '招聘']):
            score -= 50

        candidates.append((score, full_url))

    if candidates:
        # 按分数降序排列
        candidates.sort(key=lambda x: x[0], reverse=True)
        best_score, best_link = candidates[0]

        # 阈值判断
        if best_score > 10:
            print(f"   -> [智能识别] 最佳详情页 (Score={best_score:.1f}): {best_link}")
            return best_link
        else:
            print(f"   -> [警告] 最高分仅为 {best_score:.1f}，未达到详情页标准 (Text: {best_link})")

    return None


def process_task():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 同样处理 status=-1 (失败) 的任务，重新尝试
            sql = "SELECT id, url FROM t_static_sample_task WHERE status IN (0, -1)"
            cursor.execute(sql)
            tasks = cursor.fetchall()

            print(f"🚀 发现 {len(tasks)} 个待采集任务...")

            for task in tasks:
                task_id = task['id']
                list_url = task['url']
                print(f"\n[Task {task_id}] 正在采集: {list_url}")

                list_html = fetch_html(list_url)
                if not list_html:
                    print("   -> ❌ 列表页请求失败")
                    cursor.execute("UPDATE t_static_sample_task SET status = -1 WHERE id = %s", (task_id,))
                    conn.commit()
                    continue

                detail_url = extract_first_detail_link(list_html, list_url)
                detail_html = None

                if detail_url:
                    detail_html = fetch_html(detail_url, referer=list_url)
                    # 采集成功，status=1
                    status = 1
                else:
                    print("   -> ⚠️ 未能识别出详情页链接")
                    # 只有列表页，status=1 也可以，或者保留 -1 视您业务逻辑而定
                    # 这里设为 1 允许后续只生成列表规则
                    status = 1

                update_sql = """
                    UPDATE t_static_sample_task 
                    SET list_html = %s, detail_url = %s, detail_html = %s, status = %s 
                    WHERE id = %s
                """
                cursor.execute(update_sql, (list_html, detail_url, detail_html, status, task_id))
                conn.commit()
                print(f"   ✅ 样本数据保存完毕")

                time.sleep(1)

    finally:
        conn.close()


if __name__ == "__main__":
    process_task()