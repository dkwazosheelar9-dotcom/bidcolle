# -*- coding: utf-8 -*-
from contextlib import contextmanager

import pymysql
import time
import config
from src.functionTools import *
from dbutils.pooled_db import PooledDB
# 日志记录器
# logger = setup_logger(script_name())
# error_logger = setup_logger(script_name() + '-error', level=logging.DEBUG)

# 添加连接池配置
db_pool = PooledDB(
    creator=pymysql,
    maxconnections=config.max_threads * 2,  # 最大连接数设为线程数的2倍
    mincached=config.max_threads // 2,      # 初始连接数为线程数的一半
    maxcached=config.max_threads,           # 最大空闲连接数等于线程数
    maxshared=config.max_threads // 2,      # 最大共享连接数为线程数的一半
    blocking=True,                          # 保持阻塞等待
    maxusage=None,                          # 取消单个连接最大使用次数限制
    setsession=['SET AUTOCOMMIT = 1'],      # 默认自动提交
    ping=1,                                 # 自动检测连接是否可用
    host=config.host,
    port=config.port,
    user=config.user,
    password=config.passwd,
    database=config.db,
    charset=config.charset,
    cursorclass=pymysql.cursors.DictCursor
)

def get_db_connection():
    """获取数据库连接并确保连接可用"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            connection = db_pool.connection()
            # 检查连接是否有效
            connection.ping(reconnect=True)
            return connection
        except Exception as e:
            if attempt < max_retries - 1:
                error_logger.warning(f"数据库连接失败，第{attempt + 1}次重试: {str(e)}")
                time.sleep(retry_delay)
            else:
                error_logger.error(f"数据库连接失败，已重试{max_retries}次: {str(e)}")
                raise

@contextmanager
def get_db_cursor():
    """获取数据库游标的上下文管理器"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        yield cursor
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def execute_query(sql, params=None):
    """执行查询SQL"""
    max_retries = 1
    retry_delay = 1
    
    for attempt in range(max_retries):
        connection = None
        try:
            connection = get_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                result = cursor.fetchall()
                return result
        except pymysql.Error as e:
            if attempt < max_retries - 1:
                error_logger.warning(f"查询执行失败，第{attempt + 1}次重试: {str(e)}\nSQL: {sql}")
                time.sleep(retry_delay)
            else:
                error_logger.error(f"查询执行失败，已重试{max_retries}次: {str(e)}\nSQL: {sql}")
                return []
        finally:
            if connection:
                connection.close()


def execute_update(sql, params=None):
    """执行更新SQL"""
    max_retries = 1
    retry_delay = 1
    
    for attempt in range(max_retries):
        connection = None
        try:
            connection = get_db_connection()
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                connection.commit()
                # logger.info(f"更新成功: {sql}")
                return True
        except pymysql.Error as e:
            if connection:
                connection.rollback()
                error_logger.warning(f"更新失败，第{attempt + 1}次重试: {str(e)}\nSQL: {sql}")
            if attempt < max_retries - 1:
                error_logger.warning(f"更新执行失败，第{attempt + 1}次重试: {str(e)}\nSQL: {sql}")
                time.sleep(retry_delay)
            else:
                error_logger.error(f"更新执行失败，已重试{max_retries}次: {str(e)}\nSQL: {sql}")
                return False
        finally:
            if connection:
                connection.close()


def excute(sql, data):
    """执行带参数的SQL语句"""
    max_retries = 1
    retry_delay = 1
    
    for attempt in range(max_retries):
        connection = None
        try:
            connection = get_db_connection()
            with connection.cursor() as cursor:
                data_list = list(data)
                for i in range(len(data_list)):
                    data_list[i] = data_list[i].replace('\'', "\\'")
                data = tuple(data_list)
                sqldata = sql % data
                cursor.execute(sqldata)
                connection.commit()
                return True
        except pymysql.Error as e:
            if connection:
                connection.rollback()
            if attempt < max_retries - 1:
                error_logger.warning(f"SQL执行失败，第{attempt + 1}次重试: {str(e)}\nSQL: {sqldata}")
                time.sleep(retry_delay)
            else:
                error_logger.error(f"SQL执行失败，已重试{max_retries}次: {str(e)}\nSQL: {sqldata}")
                return False
        finally:
            if connection:
                connection.close()


def get_spider_dict():
    """获取爬虫字典配置"""
    sql = "SELECT spider_dict FROM t_bidcollect_one_level_page"
    return execute_query(sql)


def get_one_level_page():
    """获取一级页面列表"""
    # sql = '''SELECT id,webname,webadd,href,method,find_list,find_href,find_title,find_pub_time,find_zbid,
    #          post_content_href,find_content,post_payload1,post_payload2,post_headers,list_method,detail_method,page_params
    #          FROM t_bidcollect_one_level_page
    #          where id = 5464 LIMIT 0,10000'''
    sql = '''SELECT id,webname,webadd,href,method,find_list,find_href,find_title,find_pub_time,find_zbid,
             post_content_href,find_content,post_payload1,post_payload2,post_headers,list_method,detail_method,page_params
             FROM t_bidcollect_one_level_page
             where is_active = 1
             LIMIT 0,4000 '''
    return execute_query(sql)

def get_end_level_page():
    """获取待处理的终端页面列表"""
    sql = '''SELECT webname, href, publish_time, method, find_content, zbid, 
             post_content_href, post_payload1, post_headers, detail_method 
             FROM t_bidcollect_end_level_page 
             WHERE isexec = 0
             LIMIT 0,1000'''
    return execute_query(sql)

def insert_end_level_page(one_level_page_id, webname, href, p_href, publish_time, method, find_content, zbid, post_content_href, post_payload1, post_headers, detail_method):
    """插入终端页面数据"""
    max_retries = 1
    retry_delay = 1
    
    for attempt in range(max_retries):
        connection = None
        try:
            connection = get_db_connection()
            with connection.cursor() as cursor:
                # sql = """INSERT INTO t_bidcollect_end_level_page
                #         (one_level_page_id, webname, href, p_href, publish_time, method,
                #         find_content, zbid, post_content_href, post_payload1, post_headers,
                #         isexec, detail_method)
                #         SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s
                #         FROM dual
                #         WHERE NOT EXISTS (
                #             SELECT 1 FROM t_bidcollect_end_level_page
                #             WHERE webname = %s and href = %s
                #         )"""
                sql = """INSERT INTO t_bidcollect_end_level_page
                        (one_level_page_id, webname, href, p_href, publish_time, method, 
                        find_content, zbid, post_content_href, post_payload1, post_headers, 
                        isexec, detail_method) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s)"""
                cursor.execute(sql, (
                    one_level_page_id, webname, href, p_href, publish_time, method,
                    find_content, zbid, post_content_href, post_payload1, post_headers,
                    detail_method
                    # webname, href
                ))
                connection.commit()
                logger.info('插入%s到end_level_page', href)
                return True
                
        except pymysql.Error as e:
            if connection:
                connection.rollback()
            if attempt < max_retries - 1:
                error_logger.warning(f"插入终端页面失败，第{attempt + 1}次重试: {str(e)}\nURL: {href}")
                time.sleep(retry_delay)
            else:
                error_logger.error(f"插入终端页面失败，已重试{max_retries}次: {str(e)}\nURL: {href}")
                return False
        finally:
            if connection:
                connection.close()

# 添加批量插入方法

def batch_insert_end_level_pages(pages_data):
    """批量插入终端页面数据，遇到重复记录时忽略"""
    max_retries = 1
    retry_delay = 1    
    for attempt in range(max_retries):
        connection = None
        try:
            connection = get_db_connection()
            with connection.cursor() as cursor:
                # 先尝试插入所有记录
                sql = """INSERT IGNORE INTO t_bidcollect_end_level_page
                        (one_level_page_id, webname, href, p_href, publish_time, method, 
                        find_content, zbid, post_content_href, post_payload1, post_headers, 
                        isexec, detail_method) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s)"""
                
                cursor.executemany(sql, pages_data)
                inserted_count = cursor.rowcount
                
                connection.commit()
                
                logger.info(f'批量处理完成: 新增={inserted_count}')
                return inserted_count
                
        except pymysql.Error as e:
            if connection:
                connection.rollback()
            if attempt < max_retries - 1:
                error_logger.warning(f"批量插入终端页面失败，第{attempt + 1}次重试: {str(e)}")
                time.sleep(retry_delay)
            else:
                error_logger.error(f"批量插入终端页面失败，已重试{max_retries}次: {str(e)}")
                return 0
        finally:
            if connection:
                connection.close()

def do_one_level_page(id):
    """更新一级页面状态为已完成"""
    sql = 'UPDATE t_bidcollect_one_level_page SET lastdate = CURRENT_TIMESTAMP WHERE id = %s'
    return execute_update(sql, (id,))


def fail_one_level_page(id):
    """更新一级页面状态为失败"""
    sql = '''UPDATE t_bidcollect_one_level_page 
            SET lastdate = CURRENT_TIMESTAMP, 
                isfail = CURDATE() 
            WHERE id = %s'''
    return execute_update(sql, (id,))


# def get_end_level_page():
#     """获取待处理的终端页面列表"""
#     sql = '''SELECT webname, href, publish_time, method, find_content, zbid,
#              post_content_href, post_payload1, post_headers, detail_method
#              FROM t_bidcollect_end_level_page
#              WHERE isexec = 0
#              LIMIT 0,1000'''
#     return execute_query(sql)


def do_end_level_page(webname, href):
    """更新终端页面状态为已完成"""
    sql = 'UPDATE t_bidcollect_end_level_page SET isexec = true WHERE webname = %s AND href = %s'
    return execute_update(sql, (webname, href))

def reset_end_level_page():
    """重置失败页面状态"""
    sql = """UPDATE t_bidcollect_end_level_page set isexec = false,isfail = null 
    where date(isfail) > DATE_SUB(NOW(), INTERVAL 2 day) 
    AND DATE(publish_time) > DATE_SUB(NOW(), INTERVAL 3 DAY)
    AND find_content IS NOT NULL"""
    return execute_update(sql)

def fail_end_level_page(webname, href):
    """更新终端页面状态为失败"""
    sql = 'UPDATE t_bidcollect_end_level_page SET isexec = true, isfail = CURDATE() WHERE webname = %s AND href = %s'
    return execute_update(sql, (webname, href))


def insert_bidcollect_info(webname, href, msg, html, publish_time):
    """插入招标信息"""
    sql = """INSERT INTO t_bidcollect_info
             (webname, href, msg, html, publish_time,from_auto_script)
             VALUES (%s, %s, %s, %s, %s, 0)
             ON DUPLICATE KEY UPDATE
                href = VALUES(href),
                msg = VALUES(msg),
                html = VALUES(html),
                publish_time = VALUES(publish_time),
                from_auto_script = VALUES(from_auto_script)
                """
    return execute_update(sql, (webname, href, msg, html, publish_time))


def insert_bidcollect_info_p(webname, href, msg, publish_time):
    """插入招标信息（带参数版本）"""
    sql = """INSERT INTO t_bidcollect_info
             (webname, href, msg, publish_time)
             VALUES (%s, %s, %s, %s)
             ON DUPLICATE KEY UPDATE 
                href = VALUES(href), 
                msg = VALUES(msg), 
                publish_time = VALUES(publish_time)"""
    return execute_update(sql, (webname, href, msg, publish_time))


def get_dmx():
    """获取大模型相关数据"""
    sql = """SELECT id,CONCAT(webname,'\n发布日期：',publish_time,'\n',
             'http://140.210.90.48:5001/html/',id,'.html \n') name 
             FROM `t_bidcollect_info`  
             WHERE webname REGEXP '大模型' 
             AND publish_time > DATE_SUB(NOW(), INTERVAL 10 DAY)  
             AND id NOT IN (SELECT id FROM t_send_bid)"""
    return execute_query(sql)


def insert_id(id, flag):
    """插入ID记录"""
    sql = "INSERT INTO t_send_bid(flag,id) VALUES (%s, %s)"
    return execute_update(sql, (flag, id))


def get_to_html():
    """获取待转换HTML的数据"""
    sql = """SELECT id,CONCAT('<meta charset="UTF-8">\n',html) contents 
             FROM t_bidcollect_info 
             WHERE tohtml = 0 order by create_time desc
             LIMIT 100"""
    return execute_query(sql)


def do_to_html(v_id):
    """更新HTML转换状态"""
    sql = "UPDATE t_bidcollect_info SET tohtml = 1 WHERE id = %s"
    return execute_update(sql, (v_id,))


def delete_dup():
    """删除重复数据"""
    sql = """DELETE FROM t_bidcollect_info 
             WHERE msg IS NOT NULL 
             AND webname IN (
                 SELECT * FROM (
                     SELECT webname 
                     FROM t_bidcollect_info 
                     GROUP BY webname 
                     HAVING COUNT(webname)>1
                 ) a
             ) 
             AND id NOT IN (
                 SELECT * FROM (
                     SELECT MAX(id) AS id 
                     FROM t_bidcollect_info 
                     GROUP BY webname 
                     HAVING COUNT(webname)>1
                 ) b
             )"""
    return execute_update(sql)


def timer(func):
    def decor(*args):
        start_time = time.time()
        func(*args)
        end_time = time.time()
        d_time = end_time - start_time
        print("the running time is : ", d_time)

    return decor


def insert_one_level_page(webname, webadd, href, method, lastdate, find_list, find_href, find_title, find_pub_time, find_zbid, post_content_href, find_content, post_payload1, post_payload2, post_headers, list_method, detail_method):
    """插入一级页面数据"""
    sql = """INSERT IGNORE INTO t_bidcollect_one_level_page(
                webname, webadd, href, method, lastdate, find_list, find_href, 
                find_title, find_pub_time, find_zbid, post_content_href, find_content,
                post_payload1, post_payload2, post_headers, list_method, detail_method
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    res = execute_update(sql, (
        webname, webadd, href, method, lastdate, find_list, find_href, find_title,
        find_pub_time, find_zbid, post_content_href, find_content, post_payload1,
        post_payload2, post_headers, list_method, detail_method
    ))
    if res:
        logger.info("成功插入一条数据")
    else:
        logger.info("插入数据失败，可能与唯一索引冲突")
    return res


def create_monitor_record(one_level_page_id, webname):
    """创建监控记录"""
    sql = """INSERT IGNORE INTO t_bidcollect_monitor 
            (one_level_page_id, webname, collect_date, start_time) 
            VALUES (%s, %s, CURDATE(), NOW())"""
    return execute_update(sql, (one_level_page_id, webname))


def update_monitor(one_level_page_id, total_count, success_count):
    """更新监控记录成功状态"""
    sql = """
        UPDATE t_bidcollect_monitor 
        SET is_executed = 1,
            is_success = 1,
            total_count = %s,
            success_count = %s,
            end_time = NOW()
        WHERE one_level_page_id = %s 
        AND collect_date = CURDATE()
    """
    return execute_update(sql, (total_count, success_count, one_level_page_id))

def update_monitor_success(id, success_count, fail_count):
    """更新监控记录成功状态"""
    sql = """
        UPDATE t_bidcollect_monitor 
        SET is_executed = 1,
            is_success = 1,
            is_count = 1,
            success_count = %s,
            fail_count = %s,
            end_time = NOW()
        WHERE id = %s 
        AND collect_date = CURDATE()
    """
    return execute_update(sql, (success_count, fail_count, id))


def update_monitor_fail(one_level_page_id, error_message, total_count=0, success_count=0):
    """更新失败状态"""
    sql = """UPDATE t_bidcollect_monitor 
            SET is_executed = 1,
                is_success = 0,
                error_message = %s,
                total_count = %s,
                success_count = %s,
                fail_count = total_count - %s,
                end_time = NOW()
            WHERE one_level_page_id = %s 
            AND collect_date = CURDATE()"""
    return execute_update(sql, (error_message, total_count, success_count, success_count, one_level_page_id))


def get_monitor_stats(start_date=None, end_date=None):
    """获取监控统计数据"""
    where_clause = ""
    params = []

    if start_date and end_date:
        where_clause = "WHERE collect_date BETWEEN %s AND %s"
        params = [start_date, end_date]

    sql = f"""
        SELECT 
            m.webname,
            m.collect_date,
            m.is_executed,
            m.is_success,
            m.error_message,
            m.total_count,
            m.success_count,
            m.fail_count,
            m.start_time,
            m.end_time,
            o.href as website_url
        FROM t_bidcollect_monitor m
        LEFT JOIN t_bidcollect_one_level_page o ON m.one_level_page_id = o.id
        {where_clause}
        ORDER BY m.collect_date DESC, m.webname
    """

    return execute_query(sql, params)


def get_daily_stats():
    """获取每日采集统计数据"""
    sql = """
        SELECT 
            DATE(create_time) as collect_date,
            COUNT(*) as total_count,
            SUM(CASE WHEN isfail = TRUE THEN 1 ELSE 0 END) as fail_count,
            SUM(CASE WHEN isfail = FALSE THEN 1 ELSE 0 END) as success_count
        FROM t_bidcollect_end_level_page
        WHERE create_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        GROUP BY DATE(create_time)
        ORDER BY collect_date DESC
    """
    return execute_query(sql)


def get_website_stats():
    """获取各网站采集统计数据"""
    sql = """
        SELECT 
            o.webname,
            o.href as website_url,
            o.last_collect_time,
            COUNT(e.id) as total_pages,
            SUM(CASE WHEN e.isfail = TRUE THEN 1 ELSE 0 END) as fail_count,
            SUM(CASE WHEN e.isfail = FALSE THEN 1 ELSE 0 END) as success_count
        FROM t_bidcollect_one_level_page o
        LEFT JOIN t_bidcollect_end_level_page e ON e.p_href = o.href
        WHERE e.create_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        GROUP BY o.id
        ORDER BY total_pages DESC
    """
    return execute_query(sql)


def get_error_stats():
    """获取错误统计数据"""
    sql = """
        SELECT 
            m.webname,
            m.error_message,
            COUNT(*) as error_count,
            MAX(m.create_time) as last_error_time
        FROM t_bidcollect_monitor m
        WHERE m.is_success = FALSE
        AND m.create_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        GROUP BY m.webname, m.error_message
        ORDER BY error_count DESC
        LIMIT 100
    """
    return execute_query(sql)


def get_website_detail_stats():
    """获取每个网站的详情页采集统计"""
    sql = """
        SELECT 
            o.webname,
            o.href as website_url,
            o.last_collect_time,
            COUNT(DISTINCT e.id) as total_details,
            SUM(CASE WHEN e.isexec = TRUE AND e.isfail = FALSE THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN e.isexec = TRUE AND e.isfail = TRUE THEN 1 ELSE 0 END) as fail_count,
            SUM(CASE WHEN e.isexec = FALSE THEN 1 ELSE 0 END) as pending_count,
            MAX(e.create_time) as last_detail_time,
            MAX(i.create_time) as last_success_time
        FROM t_bidcollect_one_level_page o
        LEFT JOIN t_bidcollect_end_level_page e ON e.p_href = o.href
        LEFT JOIN t_bidcollect_info i ON i.href = e.href
        WHERE e.create_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        GROUP BY o.id, o.webname, o.href, o.last_collect_time
        ORDER BY total_details DESC
    """
    return execute_query(sql)


def get_website_daily_stats(website_id=None):
    """获取每个网站的每日采集统计"""
    where_clause = "WHERE e.create_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"
    params = []

    if website_id:
        where_clause += " AND o.id = %s"
        params.append(website_id)

    sql = f"""
        SELECT 
            o.webname,
            DATE(e.create_time) as collect_date,
            COUNT(DISTINCT e.id) as total_count,
            SUM(CASE WHEN e.isexec = TRUE AND e.isfail = FALSE THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN e.isexec = TRUE AND e.isfail = TRUE THEN 1 ELSE 0 END) as fail_count,
            SUM(CASE WHEN e.isexec = FALSE THEN 1 ELSE 0 END) as pending_count
        FROM t_bidcollect_one_level_page o
        LEFT JOIN t_bidcollect_end_level_page e ON e.p_href = o.href
        {where_clause}
        GROUP BY o.webname, DATE(e.create_time)
        ORDER BY o.webname, collect_date DESC
    """
    return execute_query(sql, params)


def get_pending_monitor_records():
    """获取需要更新的监控记录"""
    # sql = """
    #     SELECT m.one_level_page_id
    #     FROM t_bidcollect_monitor m
    #     JOIN t_bidcollect_end_level_page e ON e.one_level_page_id = m.one_level_page_id
    #     WHERE m.collect_date = CURDATE()
    #     AND ABS(TIMESTAMPDIFF(MINUTE, e.create_time, m.create_time)) <= 10
    #     AND NOT EXISTS (
    #         SELECT 1
    #         FROM t_bidcollect_end_level_page e2
    #         WHERE e2.one_level_page_id = m.one_level_page_id
    #         AND e2.isexec = 0
    #         AND ABS(TIMESTAMPDIFF(MINUTE, e2.create_time, m.create_time)) <= 1
    #     )
    # """
    sql ="""
            SELECT *
            FROM t_bidcollect_monitor 
            WHERE collect_date = CURDATE()
            AND total_count > 0
            AND is_success = 1
        """
    return execute_query(sql)


def get_page_stats(id):
    """获取特定一级页面对应的详情页统计数据"""
    sql = """
        SELECT 
            COUNT(*) as total_count,
            SUM(CASE WHEN isexec = 1 AND isfail IS NULL THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN isfail IS NOT NULL THEN 1 ELSE 0 END) as fail_count
        FROM t_bidcollect_end_level_page e
        JOIN t_bidcollect_monitor m ON e.one_level_page_id = m.one_level_page_id
        WHERE m.id = %s
        AND m.collect_date = CURDATE()
        AND m.total_count > 0
        AND ABS(TIMESTAMPDIFF(MINUTE, e.create_time, m.create_time)) <= 1
    """
    result = execute_query(sql, (id,))
    return result[0] if result else {'total_count': 0, 'success_count': 0, 'fail_count': 0}