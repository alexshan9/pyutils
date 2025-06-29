#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ClickHouse 文件整理器
将 mysql_column_mapper 文件夹中的文件移动到 clickhouse_mapper 文件夹，
根据 table_dict.csv 进行重命名，跳过 ignore_table.txt 中指定的表
"""

import pandas as pd
import os
import shutil
import logging
from pathlib import Path
from datetime import datetime


def setup_logging():
    """设置日志记录"""
    # 确保 logs 目录存在
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    log_filename = log_dir / f"clickhouse_file_organizer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def load_table_mapping(table_dict_path='table_dict.csv'):
    """
    加载表名映射字典
    
    Args:
        table_dict_path (str): 表映射字典文件路径
        
    Returns:
        dict: 从 table 到 new_table_name 的映射字典
    """
    logger = logging.getLogger(__name__)
    
    try:
        # 读取表映射字典
        df_mapping = pd.read_csv(table_dict_path, encoding='utf-8')
        
        # 构建映射字典：table -> new_table_name
        table_mapping = dict(zip(df_mapping['table'], df_mapping['new_table_name']))
        
        logger.info(f"成功加载表映射字典，共 {len(table_mapping)} 个映射关系")
        return table_mapping
        
    except FileNotFoundError:
        logger.error(f"表映射字典文件未找到: {table_dict_path}")
        raise
    except Exception as e:
        logger.error(f"加载表映射字典时发生错误: {str(e)}")
        raise


def load_ignore_tables(ignore_file_path='ignore_table.txt'):
    """
    加载需要忽略的表名列表
    
    Args:
        ignore_file_path (str): 忽略表文件路径
        
    Returns:
        set: 需要忽略的表名集合
    """
    logger = logging.getLogger(__name__)
    
    try:
        with open(ignore_file_path, 'r', encoding='utf-8') as f:
            ignore_tables = set(line.strip() for line in f if line.strip())
        
        logger.info(f"成功加载忽略表列表，共 {len(ignore_tables)} 个表")
        return ignore_tables
        
    except FileNotFoundError:
        logger.error(f"忽略表文件未找到: {ignore_file_path}")
        raise
    except Exception as e:
        logger.error(f"加载忽略表列表时发生错误: {str(e)}")
        raise


def process_single_file(file_path, table_mapping, ignore_tables, output_dir):
    """
    处理单个 CSV 文件
    
    Args:
        file_path (Path): 输入文件路径
        table_mapping (dict): 表映射字典
        ignore_tables (set): 忽略表集合
        output_dir (str): 输出目录
        
    Returns:
        tuple: (处理状态, 处理信息)
    """
    logger = logging.getLogger(__name__)
    
    try:
        # 提取表名（去除 .csv 后缀）
        table_name = file_path.stem
        
        # 检查是否在忽略列表中
        if table_name in ignore_tables:
            logger.info(f"跳过忽略表: {table_name}")
            return 'ignored', f"表 {table_name} 在忽略列表中"
        
        # 查找映射的新表名
        new_table_name = table_mapping.get(table_name)
        
        if new_table_name is None:
            logger.warning(f"表 {table_name} 在映射字典中未找到")
            return 'missing_mapping', f"表 {table_name} 缺少映射"
        
        # 生成新文件名：mysql_table_name-clickhouse_table_name.csv
        new_filename = f"{table_name}-{new_table_name}.csv"
        output_path = Path(output_dir) / new_filename
        
        # 移动并重命名文件
        shutil.move(str(file_path), str(output_path))
        
        logger.info(f"成功移动: {file_path.name} -> {new_filename}")
        return 'success', f"成功移动到 {new_filename}"
        
    except Exception as e:
        logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
        return 'error', f"处理失败: {str(e)}"


def process_all_files(input_dir='mysql_column_mapper', output_dir='clickhouse_mapper', 
                     table_dict_path='table_dict.csv', ignore_file_path='ignore_table.txt'):
    """
    批量处理所有文件
    
    Args:
        input_dir (str): 输入目录
        output_dir (str): 输出目录  
        table_dict_path (str): 表映射字典文件路径
        ignore_file_path (str): 忽略表文件路径
        
    Returns:
        dict: 处理统计信息
    """
    logger = logging.getLogger(__name__)
    
    # 加载映射字典和忽略列表
    table_mapping = load_table_mapping(table_dict_path)
    ignore_tables = load_ignore_tables(ignore_file_path)
    
    # 创建输出目录
    Path(output_dir).mkdir(exist_ok=True)
    logger.info(f"输出目录已创建: {output_dir}")
    
    # 统计信息
    stats = {
        'total_files': 0,
        'success_files': 0,
        'ignored_files': 0,
        'missing_mapping_files': 0,
        'error_files': 0,
        'processed_details': []
    }
    
    # 遍历输入目录中的所有 CSV 文件
    input_path = Path(input_dir)
    if not input_path.exists():
        logger.error(f"输入目录不存在: {input_dir}")
        return stats
    
    csv_files = list(input_path.glob('*.csv'))
    stats['total_files'] = len(csv_files)
    
    logger.info(f"找到 {len(csv_files)} 个 CSV 文件需要处理")
    
    for csv_file in csv_files:
        status, message = process_single_file(csv_file, table_mapping, ignore_tables, output_dir)
        
        stats['processed_details'].append({
            'file': csv_file.name,
            'status': status,
            'message': message
        })
        
        if status == 'success':
            stats['success_files'] += 1
        elif status == 'ignored':
            stats['ignored_files'] += 1
        elif status == 'missing_mapping':
            stats['missing_mapping_files'] += 1
        elif status == 'error':
            stats['error_files'] += 1
    
    return stats


def generate_report(stats, output_file='clickhouse_file_organizer_report.txt'):
    """
    生成处理报告
    
    Args:
        stats (dict): 处理统计信息
        output_file (str): 报告文件路径
    """
    logger = logging.getLogger(__name__)
    
    report_content = f"""ClickHouse 文件整理报告
{'='*50}
处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

处理统计:
- 总文件数: {stats['total_files']}
- 成功移动文件数: {stats['success_files']}
- 忽略文件数: {stats['ignored_files']}
- 缺少映射文件数: {stats['missing_mapping_files']}
- 错误文件数: {stats['error_files']}

"""
    
    # 详细处理结果
    if stats['processed_details']:
        report_content += "详细处理结果:\n"
        for detail in stats['processed_details']:
            report_content += f"- {detail['file']}: {detail['status']} - {detail['message']}\n"
    
    # 保存报告
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    logger.info(f"处理报告已生成: {output_file}")
    print(report_content)


def main():
    """主程序入口"""
    logger = setup_logging()
    logger.info("开始 ClickHouse 文件整理处理")
    
    try:
        # 处理所有文件
        stats = process_all_files()
        
        # 生成报告
        generate_report(stats)
        
        logger.info("ClickHouse 文件整理处理完成")
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        raise


if __name__ == "__main__":
    main() 