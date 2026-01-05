# --coding:utf-8--
from execute_strategy import *
from ggzy_bidcollect import *
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from bid_mysql import *
import config

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

def process_end_level_page(end_level_page):
    """处理单个终级页面的函数"""
    filter_illegal_characters(end_level_page)
    try:
        logger.info(f"开始爬取{end_level_page}")
        data = ExecuteStrategy(end_level_page).end_level_page_list()
        with get_thread_connection() as conn:
            insert_bidcollect_info(end_level_page['webname'], end_level_page['href'],
                                 data['msg'], data['html'], end_level_page['publish_time'])
            do_end_level_page(end_level_page['webname'], 
                            end_level_page['href'].replace('\"','\\\"'))
    except Exception as e:
        error_logger.error(f"爬取详情页{end_level_page['href']}失败，原因是{e}")
        with get_thread_connection() as conn:
            fail_end_level_page(end_level_page['webname'], end_level_page['href'])

def filter_illegal_characters(dict):
    dict['href'] = dict['href'].replace('\n', '').replace('\r', '')
    if dict.get('post_content_href'):
        dict['post_content_href'] = dict['post_content_href'].replace('\n', '').replace('\r', '')
    if dict.get('find_href'):
        dict['find_href'] = dict['find_href'].replace('\n', '').replace('\r', '')

def main():
    logger.info("开始持续爬取招标详情信息")
    while True:
        try:
            with ThreadPoolExecutor(max_workers=config.max_detail_threads) as executor:
                while True:
                    end_level_page_list = get_end_level_page()
                    if not end_level_page_list:

                        logger.info(f"当前没有需要爬取的详情页，等待{config.retry_interval}秒后重试")
                        # get_keyword_main(1, 100)
                        # org_classify_main()
                        # get_BU_ID()
                        reset_end_level_page()
                        logger.info(f"重置失败页")
                        # 获取所有需要更新监控记录的one_level_page_id
                        # one_level_pages = get_pending_monitor_records()
                        # 更新每个one_level_page的监控记录
                        # logger.info(f"开始更新监控记录")
                        # for page in one_level_pages:
                        #     stats = get_page_stats(page['id'])
                        #     logger.info(f"更新{page['id']}的监控记录, 总数量: {page['total_count']}, 成功数量: {stats['success_count']}, 失败数量: {stats['fail_count']}")
                        #     update_monitor_success(
                        #         page['id'],
                        #         stats['success_count'],
                        #         stats['fail_count']
                        #     )
                        # logger.info(f"监控记录更新完成")
                        time.sleep(1800)
                        continue
                    
                    # 提交任务并使用as_completed获取完成的任务
                    future_to_page = {
                        executor.submit(process_end_level_page, page): page 
                        for page in end_level_page_list
                    }
                    
                    for future in as_completed(future_to_page):
                        page = future_to_page[future]
                        try:
                            future.result()
                        except Exception as e:
                            error_logger.error(f"处理页面失败 {page['href']}: {str(e)}")
                    
                    # 添加适当的休息时间，避免过度请求
                    # time.sleep(config.min_delay)
                        
        except Exception as e:
            logger.error(f"详情页爬取发生错误: {e}")
            time.sleep(config.retry_interval)
            continue

if __name__ == '__main__':
    main() 