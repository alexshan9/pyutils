#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL 列名映射处理器
直接从MySQL数据库中读取表结构，根据 column_dict.csv 进行列名映射处理
"""

import pandas as pd
import os
import logging
import pymysql
import configparser
from pathlib import Path
from datetime import datetime


def setup_logging():
    """设置日志记录"""
    # 确保 logs 目录存在
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    log_filename = log_dir / f"mysql_column_mapper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def load_mysql_config(config_path='config.ini'):
    """
    加载MySQL数据库配置
    
    Args:
        config_path (str): 配置文件路径
        
    Returns:
        dict: MySQL连接配置字典
    """
    logger = logging.getLogger(__name__)
    
    try:
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        
        mysql_config = {
            'host': config.get('mysql', 'host'),
            'port': config.getint('mysql', 'port'),
            'database': config.get('mysql', 'database'),
            'user': config.get('mysql', 'user'),
            'password': config.get('mysql', 'password'),
            'charset': config.get('mysql', 'charset', fallback='utf8mb4')
        }
        
        logger.info(f"成功加载MySQL配置: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
        return mysql_config
        
    except Exception as e:
        logger.error(f"加载MySQL配置时发生错误: {str(e)}")
        raise


def create_mysql_connection(config):
    """
    创建MySQL数据库连接
    
    Args:
        config (dict): MySQL连接配置
        
    Returns:
        pymysql.Connection: 数据库连接对象
    """
    logger = logging.getLogger(__name__)
    
    try:
        connection = pymysql.connect(**config)
        logger.info(f"成功连接到MySQL数据库: {config['host']}:{config['port']}/{config['database']}")
        return connection
        
    except Exception as e:
        logger.error(f"连接MySQL数据库时发生错误: {str(e)}")
        raise


def get_table_columns_from_db(connection, database_name, table_name):
    """
    从数据库中获取指定表的列信息
    
    Args:
        connection: 数据库连接对象
        database_name (str): 数据库名
        table_name (str): 表名
        
    Returns:
        list: 列名列表
    """
    logger = logging.getLogger(__name__)
    
    try:
        with connection.cursor() as cursor:
            # 查询表的所有列信息
            query = """
                SELECT COLUMN_NAME, COLUMN_COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(query, (database_name, table_name))
            columns = cursor.fetchall()
            
            logger.debug(f"表 {table_name} 共有 {len(columns)} 个列")
            return columns
            
    except Exception as e:
        logger.error(f"查询表 {table_name} 列信息时发生错误: {str(e)}")
        return []


def get_all_tables_from_db(connection, database_name):
    """
    从数据库中获取所有表名
    
    Args:
        connection: 数据库连接对象
        database_name (str): 数据库名
        
    Returns:
        list: 表名列表
    """
    logger = logging.getLogger(__name__)
    
    try:
        with connection.cursor() as cursor:
            # 查询数据库中的所有表
            query = """
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """
            cursor.execute(query, (database_name,))
            tables = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"数据库 {database_name} 共有 {len(tables)} 个表")
            return tables
            
    except Exception as e:
        logger.error(f"查询数据库表列表时发生错误: {str(e)}")
        return []


def load_column_mapping(column_dict_path='column_dict.csv'):
    """
    加载列映射字典
    
    Args:
        column_dict_path (str): 列映射字典文件路径
        
    Returns:
        dict: 从 raw_column 到 new_column 的映射字典
    """
    logger = logging.getLogger(__name__)
    
    try:
        # 读取列映射字典
        df_mapping = pd.read_csv(column_dict_path, encoding='utf-8')
        
        # 构建映射字典：raw_column -> new_column
        column_mapping = dict(zip(df_mapping['raw_column'], df_mapping['new_column']))
        
        logger.info(f"成功加载列映射字典，共 {len(column_mapping)} 个映射关系")
        return column_mapping
        
    except FileNotFoundError:
        logger.error(f"列映射字典文件未找到: {column_dict_path}")
        raise
    except Exception as e:
        logger.error(f"加载列映射字典时发生错误: {str(e)}")
        raise


def load_target_tables(target_table_file='target_table.txt'):
    """
    加载目标表列表
    
    Args:
        target_table_file (str): 目标表文件路径
        
    Returns:
        set: 目标表名集合，如果文件不存在返回None
    """
    logger = logging.getLogger(__name__)
    
    if not Path(target_table_file).exists():
        logger.info(f"目标表文件 {target_table_file} 不存在，将处理所有表")
        return None
    
    try:
        with open(target_table_file, 'r', encoding='utf-8') as f:
            target_tables = set()
            for line in f:
                table_name = line.strip()
                if table_name and not table_name.startswith('#'):  # 跳过空行和注释行
                    target_tables.add(table_name)
        
        logger.info(f"成功加载目标表列表，共 {len(target_tables)} 个表")
        logger.debug(f"目标表: {sorted(target_tables)}")
        return target_tables
        
    except Exception as e:
        logger.error(f"加载目标表文件时发生错误: {str(e)}")
        raise


def process_single_table(table_name, connection, database_name, column_mapping, output_dir):
    """
    处理单个表
    
    Args:
        table_name (str): 表名
        connection: 数据库连接对象
        database_name (str): 数据库名
        column_mapping (dict): 列映射字典
        output_dir (str): 输出目录
        
    Returns:
        tuple: (成功处理的行数, 未找到映射的列数)
    """
    logger = logging.getLogger(__name__)
    
    try:
        # 从数据库获取表的列信息
        columns = get_table_columns_from_db(connection, database_name, table_name)
        
        if not columns:
            logger.warning(f"表 {table_name} 没有找到列信息，跳过处理")
            return 0, 0
        
        # 创建结果 DataFrame
        result_data = []
        unmapped_count = 0
        
        for column_name, column_comment in columns:
            # 查找映射
            mapped_column = column_mapping.get(column_name)
            
            if mapped_column is None:
                mapped_column = column_name  # 保持原名
                unmapped_count += 1
                logger.debug(f"列名 '{column_name}' 在映射字典中未找到，保持原名")
            
            result_data.append({
                'mysql': column_name,
                'clickhouse': mapped_column
            })
        
        # 创建输出 DataFrame
        df_result = pd.DataFrame(result_data)
        
        # 生成输出文件名
        output_filename = f"{table_name}.csv"
        output_path = Path(output_dir) / output_filename
        
        # 保存结果
        df_result.to_csv(output_path, index=False, encoding='utf-8')
        
        mapped_count = len(result_data) - unmapped_count
        logger.info(f"处理完成: {table_name} -> {output_path}")
        logger.info(f"  总列数: {len(result_data)}, 成功映射: {mapped_count}, 未找到映射: {unmapped_count}")
        
        return mapped_count, unmapped_count
        
    except Exception as e:
        logger.error(f"处理表 {table_name} 时发生错误: {str(e)}")
        return 0, 0


def process_all_tables(database_name=None, output_dir='mysql_column_mapper', column_dict_path='column_dict.csv', config_path='config.ini', target_table_file='target_table.txt'):
    """
    批量处理所有表
    
    Args:
        database_name (str): 数据库名，如果为None则从配置文件读取
        output_dir (str): 输出目录  
        column_dict_path (str): 列映射字典文件路径
        config_path (str): 数据库配置文件路径
        target_table_file (str): 目标表文件路径
        
    Returns:
        dict: 处理统计信息
    """
    logger = logging.getLogger(__name__)
    
    # 加载数据库配置
    mysql_config = load_mysql_config(config_path)
    if database_name is None:
        database_name = mysql_config['database']
    
    # 加载列映射字典
    column_mapping = load_column_mapping(column_dict_path)
    
    # 加载目标表列表
    target_tables = load_target_tables(target_table_file)
    
    # 创建输出目录
    Path(output_dir).mkdir(exist_ok=True)
    logger.info(f"输出目录已创建: {output_dir}")
    
    # 统计信息
    stats = {
        'total_tables': 0,
        'processed_tables': 0,
        'total_columns': 0,
        'mapped_columns': 0,
        'unmapped_columns': 0,
        'failed_tables': [],
        'skipped_tables': []
    }
    
    connection = None
    try:
        # 创建数据库连接
        connection = create_mysql_connection(mysql_config)
        
        # 获取所有表名
        all_table_names = get_all_tables_from_db(connection, database_name)
        
        # 根据目标表文件过滤表名
        if target_tables is not None:
            table_names = [table for table in all_table_names if table in target_tables]
            skipped_count = len(all_table_names) - len(table_names)
            if skipped_count > 0:
                logger.info(f"根据目标表文件过滤，跳过 {skipped_count} 个表")
                stats['skipped_tables'] = [table for table in all_table_names if table not in target_tables]
        else:
            table_names = all_table_names
        
        stats['total_tables'] = len(table_names)
        
        logger.info(f"找到 {len(table_names)} 个表需要处理")
        
        for table_name in table_names:
            try:
                mapped_count, unmapped_count = process_single_table(
                    table_name, connection, database_name, column_mapping, output_dir
                )
                
                if mapped_count > 0 or unmapped_count > 0:
                    stats['processed_tables'] += 1
                    stats['mapped_columns'] += mapped_count
                    stats['unmapped_columns'] += unmapped_count
                    stats['total_columns'] += (mapped_count + unmapped_count)
                else:
                    stats['failed_tables'].append(table_name)
                    
            except Exception as e:
                logger.error(f"处理表 {table_name} 失败: {str(e)}")
                stats['failed_tables'].append(table_name)
    
    finally:
        # 关闭数据库连接
        if connection:
            connection.close()
            logger.info("数据库连接已关闭")
    
    return stats


def generate_report(stats, output_file='mysql_column_mapping_report.txt'):
    """
    生成处理报告
    
    Args:
        stats (dict): 处理统计信息
        output_file (str): 报告文件路径
    """
    logger = logging.getLogger(__name__)
    
    mapping_rate = 0
    if stats['total_columns'] > 0:
        mapping_rate = stats['mapped_columns']/stats['total_columns']*100
    
    report_content = f"""MySQL 列名映射处理报告
{'='*50}
处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

处理统计:
- 总表数: {stats['total_tables']}
- 成功处理表数: {stats['processed_tables']}
- 失败表数: {len(stats['failed_tables'])}
- 跳过表数: {len(stats.get('skipped_tables', []))}
- 总列数: {stats['total_columns']}
- 成功映射列数: {stats['mapped_columns']}
- 未找到映射列数: {stats['unmapped_columns']}

映射成功率: {mapping_rate:.1f}%

"""
    
    if stats['failed_tables']:
        report_content += "失败表列表:\n"
        for failed_table in stats['failed_tables']:
            report_content += f"- {failed_table}\n"
        report_content += "\n"
    
    if stats.get('skipped_tables'):
        report_content += "跳过表列表 (不在目标表文件中):\n"
        for skipped_table in stats['skipped_tables']:
            report_content += f"- {skipped_table}\n"
    
    # 保存报告
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    logger.info(f"处理报告已生成: {output_file}")
    print(report_content)


def main():
    """主程序入口"""
    logger = setup_logging()
    logger.info("开始 MySQL 列名映射处理（从数据库读取表结构）")
    
    try:
        # 处理所有表
        stats = process_all_tables()
        
        # 生成报告
        generate_report(stats)
        
        logger.info("MySQL 列名映射处理完成")
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        raise


if __name__ == "__main__":
    main() 