from urllib.parse import urljoin, unquote
from downloader import get_html_downloader, post_html_downloader, get_json_downloader, post_json_downloader
from json_post_deal import deal_to_dict
import re
from ggzy_bidcollect import *
# from src.ccgp_bidcollect import *
import time
from datetime import datetime
import datetime
import html
import json
from bs4 import BeautifulSoup, Tag, NavigableString

def time_former(time):
    today = datetime.date.today()
    if time == 'null':
        time = today.strftime('%Y-%m-%d')
    elif '刚刚' in time:
        time = today.strftime('%Y-%m-%d')
    elif '小时前' in time:
        hours_ago = int(time.split('小时前')[0])
        time = (today - datetime.timedelta(hours=hours_ago)).strftime('%Y-%m-%d')
    elif len(time) == 5:
        time = str(today).split('-')[0] + '-' + time
    elif len(time) == 9:
        time = time[:5] + '0' + time[5:]
    return time

def remove_script_tags(html_content):
    """
    去除HTML字符串中所有脚本、样式等非显示内容的标签

    Args:
        html_content (str): 原始HTML字符串

    Returns:
        str: 清理后的HTML内容
    """
    if not html_content:
        return ""

    # 使用正则表达式匹配并移除各种非显示内容标签
    clean_content = re.sub(r'<script.*?>.*?</.*?script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    clean_content = re.sub(r'<noscript.*?>.*?</.*?noscript>', '', clean_content, flags=re.DOTALL | re.IGNORECASE)
    clean_content = re.sub(r'<style.*?>.*?</.*?style>', '', clean_content, flags=re.DOTALL | re.IGNORECASE)
    clean_content = re.sub(r'<select.*?>.*?</.*?select>', '', clean_content, flags=re.DOTALL | re.IGNORECASE)
    clean_content = re.sub(r'<svg.*?>.*?</.*?svg>', '', clean_content, flags=re.DOTALL | re.IGNORECASE)
    clean_content = re.sub(r'<img.*?>', '', clean_content, flags=re.DOTALL | re.IGNORECASE)
    clean_content = re.sub(r'<!--.*?-->', '', clean_content, flags=re.DOTALL | re.IGNORECASE)
    return clean_content


#处理HTML，只保留table标签，去除其他标签
def html_table_deal(html):
    # html5标准<br>,替换<br/>、</br>
    html = html.replace('<br/>', '<br>').replace('</br>', '<br>')
    # 处理HTML，只保留table标签，去除其他标签
    if not html or not html.strip():
        return html

    soup = BeautifulSoup(html, 'html.parser')

    # 移除所有style和script标签
    for tag in soup.find_all(['style', 'script']):
        tag.decompose()

    # 递归解除所有祖先中的div或p嵌套（无论隔了多少层）
    for tag_type in ['form', 'table']:
        for tag in soup.find_all(tag_type):
            # 获取所有需要解嵌套的祖先标签（div或p）
            ancestors_to_unwrap = [
                ancestor for ancestor in tag.parents
                if ancestor.name in ['div', 'p']
            ]

            # 从外到内依次解嵌套（避免层级变化影响）
            for ancestor in reversed(ancestors_to_unwrap):
                ancestor.unwrap()  # 解除嵌套

    # 首先检查是否存在table标签
    tables = soup.find_all('table')
    if not tables:
        # 对特定标签添加换行符
        for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'br', 'div']):
            if tag.name == 'br':
                tag.replace_with('\n')
            else:
                # 特殊处理div和其他块级标签
                text = extract_text_with_newlines(tag)
                if text.strip():
                    tag.replace_with(text + '\n')

        # 获取处理后的文本并压缩换行符
        output = soup.get_text()
        output = compress_newlines(output)
        return output.strip()

    # 处理存在table标签的情况
    # 处理被div嵌套的table标签 - 递归解除所有div嵌套
    result = []

    # 检查是否存在body标签，如果没有则从根节点开始
    root = soup.body if soup.body else soup

    # 处理节点
    if root:
        process_node(root, result, False)

    # 将结果列表拼接成字符串
    output = ''.join(result)

    # 压缩连续的多个换行符为一个
    output = compress_newlines(output)

    return output.strip()

def extract_text_with_newlines(tag):
    """递归提取标签内的文本并保留换行"""
    text = ''
    for content in tag.contents:
        if isinstance(content, NavigableString):
            text += str(content).strip() + ' '
        elif content.name == 'br':
            text += '\n'
        elif isinstance(content, Tag):
            if content.name in ['div', 'p', 'h1', 'h2', 'h3', 'h4']:
                text += extract_text_with_newlines(content) + '\n'
            else:
                text += content.get_text().strip() + ' '
    return text.strip()


def process_node(node, result, is_inside_table):
    if node is None:
        return

    if isinstance(node, Tag):
        if node.name == 'table':
            try:
                # 创建table的深拷贝
                table_copy = BeautifulSoup(str(node), 'html.parser').table

                # 移除table内的所有a标签，只保留其文本内容
                for a_tag in table_copy.find_all('a'):
                    a_tag.replace_with(a_tag.get_text())

                # 移除table及其子元素中的font-size和font-family样式
                remove_font_styles(table_copy)

                table_html = str(table_copy).replace('\n', '')
                result.append(table_html)
                return  # 不处理table的子节点，避免重复
            except Exception as e:
                print(f"Error processing table: {e}")
                return
        elif not is_inside_table:
            if node.name == 'br':
                # 只在table外部时替换br标签为换行符
                result.append('\n')
                return
            elif node.name in ['p', 'h1', 'h2', 'h3', 'h4', 'div']:
                # 只在table外部时，对特定标签添加换行符
                text = extract_text_with_newlines(node)
                if text.strip():
                    result.append(text + '\n')
                return  # 不处理这些标签的子节点，避免重复
    elif isinstance(node, NavigableString) and not is_inside_table:
        # 只在table外部时处理纯文本节点
        text = str(node).strip()
        if text:
            result.append(text)
        return

    # 递归处理子节点
    if hasattr(node, 'children'):
        for child in node.children:
            new_inside_table = is_inside_table or (isinstance(node, Tag) and node.name == 'table')
            process_node(child, result, new_inside_table)


def remove_font_styles(element):
    """移除元素及其子元素中的font-size和font-family样式"""
    if not isinstance(element, Tag):
        return

    if element.has_attr('style'):
        style = element['style']
        # 移除font-size和font-family样式
        style = re.sub(r'font-size\s*:[^;]+;?', '', style)
        style = re.sub(r'font-family\s*:[^;]+;?', '', style).strip()

        # 如果处理后样式为空，则移除style属性
        if not style:
            del element['style']
        else:
            element['style'] = style

    # 递归处理子元素
    for child in element.children:
        remove_font_styles(child)


def compress_newlines(input_str):
    """连续的多个换行符压缩为一个"""
    if not input_str:
        return input_str

    # 使用正则表达式替换多个换行符为一个
    return re.sub(r'\n+', '\n', input_str)





def convert_chinese_punctuation_to_english(text):
    """
    将中文标点符号转换为英文标点符号
    """
    if not text:
        return text

    # 中文符号到英文符号的映射
    punctuation_map = {
        '，': ',',
        '；': ';',
        '：': ':',
        '？': '?',
        '！': '!',
        '（': '(',
        '）': ')',
        '“': '"',
        '”': '"',
        '‘': "'",
        '’': "'",
        '【': '[',
        '】': ']',
        '《': '<',
        '》': '>'
    }

    # 逐个替换符号
    for chinese_punct, english_punct in punctuation_map.items():
        text = text.replace(chinese_punct, english_punct)

    return text

def remove_extra_content(detail_text_content):
    content = re.sub(r'\[(?=.{0,10}\])[^[\]]*?打印[^[\]]*?\]', '', detail_text_content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'\[(?=.{0,10}\])[^[\]]*?分享[^[\]]*?\]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'\[(?=.{0,10}\])[^[\]]*?浏览[^[\]]*?\]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'\[(?=.{0,10}\])[^[\]]*?次数[^[\]]*?\]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'\[(?=.{0,10}\])[^[\]]*?阅读次数[^[\]]*?\]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'\[(?=.{0,10}\])\s*字体\s*[：:]\s*[^[\]]*?\]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'\[(?=.{0,10}\])\s*字号\s*[：:]\s*[^[\]]*?\]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'\[关闭\]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'浏览次数[:/：/ ]\d+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'阅读次数[:/：/ ]\d+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'区块链已存证', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'存证时间[:/：]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'存证哈希值[:/：]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'区块高度[:/：]', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'用户登录后显示完整信息', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'备注声明*?本页面提供的交易合同、履约相关内容由市场主体维护发布,对填写的公示内容的真实性、准确性和一致性负责。本网对其内容概不负责,亦不承担任何法律责任。', '', content, flags=re.DOTALL | re.IGNORECASE)

    content = content.split('上一条')[0] if '上一条' in content  else content
    content = content.split('上一篇')[0] if '上一篇' in content  else content


    content = content.strip()

    if content.startswith('?'):
        content = content[1:]
    if content.startswith('？'):
        content = content[1:]
    if content.startswith('html'):
        content = content[4:]

    return content


def urljoin_wrapper(base_url, url):
    if url.startswith('http'):
        # 处理可能的HTML实体编码
        url = html.unescape(url)
        # 特别处理&curren被错误转义的情况
        url = url.replace('¤', '&curren')
        return url
    else:
        joined_url = urljoin(base_url, url)
        # 处理可能的HTML实体编码
        joined_url = html.unescape(joined_url)
        # 特别处理&curren被错误转义的情况
        joined_url = joined_url.replace('¤', '&curren')
        return joined_url


method_dict = {
    'get_html': get_html_downloader,
    'post_html': post_html_downloader,
    'get_json': get_json_downloader,
    'post_json': post_json_downloader,
}


def resolve_list_html(url, soup, dict):
    logger.info("soup is :", soup)
    items = eval(f"{dict['find_list']}")
    result_list = []
    for item in items:
        logger.info(f'list_html_resolver item :{item}')
        page_data = {}
        try:
            # 去除非法字符如全角括号
            str_item = str(item).replace("（","(").replace("）",")")
            item = BeautifulSoup(str_item, 'html.parser')
            href = eval(f"{dict['find_href']}").replace('\n', '').replace('\t', '').replace("'", '')
            print(href)
            page_data['href'] = urljoin_wrapper(url, href)
            logger.info(f'page_data["href"] is :{page_data["href"]}')
            page_data['title'] = eval(f"{dict['find_title']}").strip().replace('\ue638', '').replace('（','(').replace('）',')')
            page_data['title'] = convert_chinese_punctuation_to_english(page_data['title'])
            page_data['title'] = page_data['title'].replace(' ', '').replace('\n', '').replace('&nbsp;', '')
            logger.info(f'page_data["title"] is :{page_data["title"]}')
            if dict['find_pub_time'] is not None and dict['find_pub_time'] != '' and dict['find_pub_time'] != '0000-00-00':
                page_data['publish_time'] = \
                    eval(f"{dict['find_pub_time']}").replace('发布日期:', '').replace('发布时间：', '').replace(
                        '发布日期：', '').strip().replace('[', '').replace(']', '').replace('\ue638', '').replace('','').split(
                        ' ')[0]
                page_data['publish_time'] = time_former(page_data['publish_time'])
            else:
                page_data['publish_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            if page_data['publish_time'] > datetime.datetime.now().strftime('%Y-%m-%d'):
                page_data['publish_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            logger.info(f'page_data["publish_time"] is :{page_data["publish_time"]}')
            page_data['zbid'] = eval(f"{dict['find_zbid']}")
            logger.info(f'page_data["zbid"] is :{page_data["zbid"]}')
            page_data['post_content_href'] = eval(f"{dict['post_content_href']}")
            logger.info(f'page_data["post_content_href"] is :{page_data["post_content_href"]}')
            # page_data['post_payload1'] = dict['post_payload2']
            # 尝试进行json格式解析
            if dict.get('post_payload2'):
                try:
                    pdict = eval(f"{dict['post_payload2']}")
                    for p in pdict:
                        if isinstance(pdict[p], str):
                            try:
                                pdict[p] = eval(str(pdict[p]))
                            except:
                                # 内容为普通字符串，不做修改
                                pass
                    page_data['post_payload1'] = json.dumps(pdict).replace(" false", " False").replace(" true",
                                                                                                        " True").replace(
                        " null", " None")
                except Exception as e:
                    # 其他类型参数解析
                    page_data['post_payload1'] = dict.get('post_payload2')
            else:
                # 如果post_payload2不存在或为空，设置默认值
                page_data['post_payload1'] = None
            logger.info(f'page_data["post_payload1"] is :{page_data.get("post_payload1")}')
        except Exception as e:
            error_logger.error(f'ExecuteStrategy:resolve_list_html定位错误{url}:{e}')
            continue

        result_list.append(page_data)

    logger.info(f'result_list is :{result_list}')
    return result_list


def resolve_list_json(url, r_json, dict):
    result_list = []
    r_json = deal_to_dict(r_json)
    if r_json is None:
        error_logger.error(f"resolve_list_json获取网页{url}失败")
        return result_list

    try:
        # logger.info(dict)
        items = eval(f"r_json{dict['find_list']}")
    except Exception as e:
        error_logger.error(f"resolve_list_json解析{url}失败: {e}")
        return result_list
    for i in items:
        # logger.info(f'resolve_list_json i :{i}')
        page_data = {}
        # print(f"{spider_dict['findhref']}")
        try:
            page_data['href'] = eval(f"{dict['find_href']}")
            logger.info(f'page_data["href"] is :{page_data["href"]}')
            page_data['title'] = eval(f"{dict['find_title']}").strip().replace('\ue638', '').replace('（','(').replace('）',')')
            page_data['title'] = convert_chinese_punctuation_to_english(page_data['title'])
            page_data['title'] = page_data['title'].replace(' ', '').replace('\n', '').replace('&nbsp;', '')
            logger.info(f'page_data["title"] is :{page_data["title"]}')
            page_data['publish_time'] = eval(f"{dict['find_pub_time']}")
            if dict['find_pub_time'] is not None:
                try:
                    timestamp = int(page_data['publish_time']) / 1000
                    timearry = time.localtime(timestamp)
                    page_data['publish_time'] = time.strftime("%Y-%m-%d", timearry)
                    logger.info(f'page_data["publish_time"] is :{page_data["publish_time"]}')
                except:
                    page_data['publish_time'] = eval(f"{dict['find_pub_time']}")
                    logger.info(f'page_data["publish_time"] is :{page_data["publish_time"]}')
                page_data['publish_time'] = time_former(page_data['publish_time'])
                logger.info(f'page_data is :{page_data}')
                logger.info(f'page_data["publish_time"] is :{page_data["publish_time"]}')
            else:
                page_data['publish_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            if page_data['publish_time'] > datetime.datetime.now().strftime('%Y-%m-%d'):
                page_data['publish_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
            page_data['zbid'] = eval(f"{dict['find_zbid']}")
            logger.info(f'page_data["zbid"] is :{page_data["zbid"]}')
            page_data['post_content_href'] = eval(f"{dict['post_content_href']}")
            logger.info(f'page_data["post_content_href"] is :{page_data["post_content_href"]}')
            # 尝试进行json格式解析
            if dict.get('post_payload2'):
                try:
                    pdict = eval(f"{dict['post_payload2']}")
                    for p in pdict:
                        if isinstance(pdict[p], str):
                            try:
                                pdict[p] = eval(str(pdict[p]))
                            except:
                                # 内容为普通字符串，不做修改
                                pass
                    page_data['post_payload1'] = json.dumps(pdict).replace(" false", " False").replace(" true",
                                                                                                        " True").replace(
                        " null", " None")
                except Exception as e:
                    # 其他类型参数解析
                    page_data['post_payload1'] = dict.get('post_payload2')
            else:
                # 如果post_payload2不存在或为空，设置默认值
                page_data['post_payload1'] = None
            logger.info(f'page_data["post_payload1"] is :{page_data.get("post_payload1")}')
        except (IndexError, TypeError) as e:
            error_logger.error(f'ExecuteStrategy:resolve_list_json定位错误{url}:{e}')
            continue

        result_list.append(page_data)

    # result_list = deal_repeat_data(result_list)

    logger.info(f'result_list is :{result_list}')
    return result_list



class ExecuteStrategy(object):
    def __init__(self, data=dict):
        self.data = data
        self.list_json_res = self.data.get('list_method') is not None and 'json' in self.data.get('list_method')
        self.detail_json_res = self.data.get('detail_method') is not None and 'json' in self.data.get('detail_method')

    def one_level_page_list(self):
        # if self.data['method'] == '14':
            # logger.info(f"end_level_page_list获取招标公告: {self.data['href']}")
            # return ccgp_bidcollect(self.data)
            # items = ccgp_downloader(self.data['href'], 0)
            # return resolve_list_json(self.data['href'], items, self.data) if self.list_json_res else resolve_list_html(
            #     self.data['href'], items, self.data)

        if self.data['method'] == '15':
            logger.info(f"end_level_page_list获取招标公告: {self.data['href']}")
            items = ggzy_downloader(self.data['href'], 0, self.data)
            return resolve_list_json(self.data['href'], items, self.data)
        else:
            items = method_dict[self.data['list_method']](self.data['href'], 0, self.data)
            return resolve_list_json(self.data['href'], items, self.data) if self.list_json_res else resolve_list_html(
            self.data['href'], items, self.data)

    def end_level_page_list(self):
        result = {}
        soup = None
        page = "-1"
        url_pattern = '^(https?|ftp):\/\/[^\s/$.?#].[^\s]*$'
        logger.info(f"end_level_page_list获取招标公告: {self.data['href']}")
        # while page == "-1" or re.match(url_pattern, str(page)):
        # if self.data['method'] == '14':
        #     soup = ccgp_downloader(self.data['href'], 0)
        #     page = eval(f"{self.data['find_content']}")
        if self.data['method'] == '15':
            soup = ggzy_page_downloader(self.data['href'], 0)
            page = eval(f"{self.data['find_content']}")
        elif self.detail_json_res:
            page = self.json_to_html()
            # page = BeautifulSoup(page, 'html.parser')
            print(f'page:{page}')
        else:
            url = self.data['post_content_href'] if self.data['post_content_href'] else self.data['href']
            soup = method_dict[self.data['detail_method']](url, 0, self.data)
            logger.info(f"soup1:{soup}")
            logger.info(f"rule1:{self.data['find_content']}")
            page = eval(f"{self.data['find_content']}")
            logger.info(f"page1:{page}")
        try:
            # 处理段落和标题标签
            for p_tag in page.find_all('p'):
                p_tag.insert_before('\n')
            for h1_tag in page.find_all('h1'):
                h1_tag.insert_before('\n')
            for h2_tag in page.find_all('h2'):
                h2_tag.insert_before('\n')
            for br_tag in page.find_all('br'):
                br_tag.insert_before('\n')
            # for span_tag in page.find_all('span'):
            #     span_tag.insert_before('\n')



            logger.info(f"page:{page}")
        except Exception as e:
            logger.error(f"处理页面内容时出错: {e}")
            pass
        finally:
            result['html'] = remove_script_tags(str(page))
            body_text = html_table_deal(result['html'])
            result['msg'] = re.sub(r'\n\s*\n', '\n', body_text)
            result['msg'] = convert_chinese_punctuation_to_english(result['msg'])
            result['msg'] = remove_extra_content(result['msg'])
        return result

    def request_url(self):
        # 判断请求使用哪个url
        return "post_content_href" if 'json' in self.data['request_url'] else "href"

    def json_to_html(self):
        r_json = method_dict[self.data['detail_method']](self.data['post_content_href'], 0, self.data)
        rjson = eval(f"r_json{self.data['find_content']}")
        if '<' in rjson:
            soup = BeautifulSoup(rjson, 'html.parser')
        else:
            soup = BeautifulSoup(rjson, 'html.parser')
            text = soup.get_text()
            soup = BeautifulSoup(text, 'html.parser')
        logger.info(f"dict['find_content']:{self.data['find_content']}")
        logger.info(f'soup:{soup}')
        return soup

if __name__ == '__main__':
    object = {'webname': '邹平市黛溪街道中心幼儿园资产处置项目', 'href': 'http://ggzyjy.shandong.gov.cn/cqzgysgg/Jl5Oil%5EHdvWYUyumIgf7Aw.jhtml', 'publish_time': datetime.date(2025, 4, 23), 'method': '1', 'find_content': "soup.find('div',class_='div-content clearfix')", 'zbid': None, 'post_content_href': None, 'post_payload1': None, 'post_headers': None, 'detail_method': 'get_html'}
    data = ExecuteStrategy(object).end_level_page_list()
    print(data['msg'])
