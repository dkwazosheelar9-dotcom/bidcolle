# --coding:utf-8--
# from ccgp_bidcollect import *
from bid_mysql import *
from execute_strategy import *
from ggzy_bidcollect import *
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager


# 设置随机延迟，避免被封禁IP
delay = random.uniform(config.min_delay, config.max_delay)

# 线程锁
db_lock = threading.Lock()

# 线程本地存储，每个线程独立的数据库连接
thread_local = threading.local()


@contextmanager
def get_thread_connection():
    """获取线程专属的数据库连接"""
    if not hasattr(thread_local, "connection"):
        thread_local.connection = get_db_connection()
    try:
        yield thread_local.connection
    except Exception as e:
        if thread_local.connection:
            thread_local.connection.rollback()
        raise
    finally:
        if thread_local.connection:
            thread_local.connection.close()
            thread_local.connection = None


def process_one_level_page(one_level_page):
    # 处理单个一级页面的函数
    filter_illegal_characters(one_level_page)
    time.sleep(delay)
    logger.info(one_level_page)

    # 创建监控记录
    create_monitor_record(one_level_page['id'], one_level_page['webname'])

    try:
        logger.info(f"开始爬取未爬的one_level_page：, {one_level_page}")

        execute = ExecuteStrategy(one_level_page)
        result_list = execute.one_level_page_list()

        if len(result_list) == 0:
            fail_one_level_page(one_level_page['id'])
            update_monitor_fail(one_level_page['id'], "获取到空列表")
            return

        # 批量处理数据
        pages_data = []
        for data_dic in result_list:
            try:
                logger.info(f'正在处理: {data_dic.get("title", "未知标题")}')
                # 构建插入数据元组
                page_data = (
                    one_level_page['id'],
                    data_dic['title'],
                    data_dic['href'],
                    one_level_page['href'],
                    data_dic['publish_time'],
                    one_level_page['method'],
                    one_level_page['find_content'],
                    data_dic['zbid'],
                    data_dic['post_content_href'],
                    data_dic['post_payload1'],
                    one_level_page['post_headers'],
                    execute.data.get('detail_method')
                )
                pages_data.append(page_data)
            except Exception as e:
                error_logger.error(f"处理列表项失败: {str(e)}, URL: {data_dic.get('href', '未知')}")

        # 批量插入数据
        with get_thread_connection() as conn:
            total_count = batch_insert_end_level_pages(pages_data)
            logger.info(f'批量插入成功,{one_level_page["id"]}成功插入{total_count}条数据')

            do_one_level_page(one_level_page['id'])
            update_monitor(one_level_page['id'], total_count, 0)

    except Exception as e:
        error_msg = f"爬取列表页{one_level_page['href']}发生错误:{str(e)}"
        error_logger.error(error_msg)
        fail_one_level_page(one_level_page['id'])
        update_monitor_fail(one_level_page['id'], error_msg)


def filter_illegal_characters(dict):
    dict['href'] = dict['href'].replace('\n', '').replace('\r', '')
    if dict.get('post_content_href'):
        dict['post_content_href'] = dict['post_content_href'].replace('\n', '').replace('\r', '')
    if dict.get('find_href'):
        dict['find_href'] = dict['find_href'].replace('\n', '').replace('\r', '')


def main():
    logger.info("开始持续爬取招标列表信息")

    while True:
        try:
            with ThreadPoolExecutor(max_workers=config.max_list_threads) as executor:
                while True:
                    one_level_page_list = get_one_level_page()
                    if not one_level_page_list:
                        logger.info(f"开始执行统计")
                        logger.info(f"当前没有需要爬取的列表页，等待{config.retry_interval}秒后重试")
                        time.sleep(config.retry_interval)
                        continue

                    # 提交任务并使用as_completed获取完成的任务
                    future_to_page = {
                        executor.submit(process_one_level_page, page): page
                        for page in one_level_page_list
                    }

                    for future in as_completed(future_to_page):
                        page = future_to_page[future]
                        try:
                            future.result()
                        except Exception as e:
                            error_logger.error(f"处理页面失败 {page['href']}: {str(e)}")

                    # 添加适当的休息时间，避免过度请求
                    time.sleep(config.time_sleep)

        except Exception as e:
            logger.error(f"列表页爬取发生错误: {e}")
            time.sleep(config.retry_interval)
            continue


if __name__ == '__main__':
    main() 