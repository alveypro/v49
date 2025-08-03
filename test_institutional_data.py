import pandas as pd
import time
import logging
import sys
import os

# 将项目根目录添加到 sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from 机构级V13系统_v730_改进版 import V13InstitutionalDataManager, UNIFIED_DATA_MODULE_AVAILABLE, get_data_for_short_term_surge

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_dataframe_check(df):
    return df is None or df.empty

def test_institutional_data_module():
    """测试机构数据模块的性能和数据准确性"""
    logger.info("开始测试机构数据模块...")

    try:
        data_manager = V13InstitutionalDataManager()
        logger.info("V13InstitutionalDataManager 实例化成功")
    except Exception as e:
        logger.error(f"实例化 V13InstitutionalDataManager 失败: {e}", exc_info=True)
        return

    # 测试 get_institutional_data 方法
    logger.info("测试 get_institutional_data 方法...")
    start_time = time.time()
    try:
        institutional_data = data_manager.get_institutional_data(count=100)
        end_time = time.time()

        if not safe_dataframe_check(institutional_data):
            logger.info(f"get_institutional_data 成功获取 {len(institutional_data)} 条数据")
            logger.info(f"数据获取耗时: {end_time - start_time:.4f} 秒")
            logger.info("数据预览 (前5行):")
            print(institutional_data.head())
            logger.info(f"数据形状: {institutional_data.shape}")
            logger.info(f"数据列名: {institutional_data.columns.tolist()}")
        else:
            logger.warning("get_institutional_data 未返回有效数据")

    except Exception as e:
        logger.error(f"调用 get_institutional_data 失败: {e}", exc_info=True)

    # 如果统一数据模块可用，测试 get_data_for_short_term_surge
    if UNIFIED_DATA_MODULE_AVAILABLE:
        logger.info("测试 get_data_for_short_term_surge 方法...")
        start_time = time.time()
        try:
            surge_data = get_data_for_short_term_surge(count=100)
            end_time = time.time()

            if not safe_dataframe_check(surge_data):
                logger.info(f"get_data_for_short_term_surge 成功获取 {len(surge_data)} 条数据")
                logger.info(f"数据获取耗时: {end_time - start_time:.4f} 秒")
                logger.info("数据预览 (前5行):")
                print(surge_data.head())
                logger.info(f"数据形状: {surge_data.shape}")
                logger.info(f"数据列名: {surge_data.columns.tolist()}")
            else:
                logger.warning("get_data_for_short_term_surge 未返回有效数据")

        except Exception as e:
            logger.error(f"调用 get_data_for_short_term_surge 失败: {e}", exc_info=True)
    else:
        logger.info("统一数据模块不可用，跳过 get_data_for_short_term_surge 测试")

    logger.info("机构数据模块测试完成")

if __name__ == "__main__":
    test_institutional_data_module()
