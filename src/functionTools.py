import logging
import os
from logging.handlers import TimedRotatingFileHandler
import sys

'''
工程项目tools
'''

def setup_logger(log_file, level=logging.INFO):
    # 创建日志记录器
    logger = logging.getLogger(log_file)
    logger.setLevel(level)
    # 创建日志格式
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - %(process)d - %(levelname)s - %(message)s')

    # 创建控制台处理器并设置级别和格式
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # 创建文件处理器，并设置为按天分割日志文件
    # 获取当前脚本的路径
    current_directory = os.path.dirname(os.path.abspath(__file__))
    log_file_path = f'{current_directory}/../logs/{log_file}.log'
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)  # 确保日志文件目录存在

    file_handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=7, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    file_handler.suffix = "%Y-%m-%d"  # 按日期分割日志文件I

    # 添加处理器到记录器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

def script_name():
    script_name = os.path.basename(sys.argv[0]).split('.')[0]
    return script_name

# 日志记录器
# logger = setup_logger('1')
# error_logger = setup_logger('2', level=logging.DEBUG)
logger = setup_logger(script_name())
error_logger = setup_logger(script_name() + '-error', level=logging.DEBUG)