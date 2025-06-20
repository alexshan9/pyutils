#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL 列名映射处理器
遍历 mysql_scan 中的所有 CSV 文件，根据 column_dict.csv 进行列名映射处理
"""

import pandas as pd
import os
import logging
from pathlib import Path
from datetime import datetime


def setup_logging():
    """设置日志记录"""
    log_filename = f"mysql_column_mapper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


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


def process_single_file(file_path, column_mapping, output_dir):
    """
    处理单个 CSV 文件
    
    Args:
        file_path (str): 输入文件路径
        column_mapping (dict): 列映射字典
        output_dir (str): 输出目录
        
    Returns:
        tuple: (成功处理的行数, 未找到映射的列数)
    """
    logger = logging.getLogger(__name__)
    
    try:
        # 读取源文件
        df_source = pd.read_csv(file_path, encoding='utf-8')
        
        if 'columnName' not in df_source.columns:
            logger.warning(f"文件 {file_path} 中缺少 columnName 列，跳过处理")
            return 0, 0
        
        # 创建结果 DataFrame
        result_data = []
        unmapped_count = 0
        
        for _, row in df_source.iterrows():
            original_column = row['columnName']
            column_remark = row.get('columnRemark', '')
            
            # 查找映射
            mapped_column = column_mapping.get(original_column)
            
            if mapped_column is None:
                mapped_column = original_column  # 保持原名
                unmapped_count += 1
                logger.debug(f"列名 '{original_column}' 在映射字典中未找到，保持原名")
            
            result_data.append({
                'mysql': original_column,
                'clickhouse': mapped_column
            })
        
        # 创建输出 DataFrame
        df_result = pd.DataFrame(result_data)
        
        # 生成输出文件名（去除 _structure 后缀）
        input_filename = Path(file_path).name
        if input_filename.endswith('_structure.csv'):
            output_filename = input_filename.replace('_structure.csv', '.csv')
        else:
            output_filename = input_filename
        
        output_path = Path(output_dir) / output_filename
        
        # 保存结果
        df_result.to_csv(output_path, index=False, encoding='utf-8')
        
        mapped_count = len(result_data) - unmapped_count
        logger.info(f"处理完成: {file_path} -> {output_path}")
        logger.info(f"  总列数: {len(result_data)}, 成功映射: {mapped_count}, 未找到映射: {unmapped_count}")
        
        return mapped_count, unmapped_count
        
    except Exception as e:
        logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
        return 0, 0


def process_all_files(input_dir='mysql_scan', output_dir='mysql_column_mapper', column_dict_path='column_dict.csv'):
    """
    批量处理所有文件
    
    Args:
        input_dir (str): 输入目录
        output_dir (str): 输出目录  
        column_dict_path (str): 列映射字典文件路径
        
    Returns:
        dict: 处理统计信息
    """
    logger = logging.getLogger(__name__)
    
    # 加载列映射字典
    column_mapping = load_column_mapping(column_dict_path)
    
    # 创建输出目录
    Path(output_dir).mkdir(exist_ok=True)
    logger.info(f"输出目录已创建: {output_dir}")
    
    # 统计信息
    stats = {
        'total_files': 0,
        'processed_files': 0,
        'total_columns': 0,
        'mapped_columns': 0,
        'unmapped_columns': 0,
        'failed_files': []
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
        try:
            mapped_count, unmapped_count = process_single_file(
                str(csv_file), column_mapping, output_dir
            )
            
            if mapped_count > 0 or unmapped_count > 0:
                stats['processed_files'] += 1
                stats['mapped_columns'] += mapped_count
                stats['unmapped_columns'] += unmapped_count
                stats['total_columns'] += (mapped_count + unmapped_count)
            else:
                stats['failed_files'].append(str(csv_file))
                
        except Exception as e:
            logger.error(f"处理文件 {csv_file} 失败: {str(e)}")
            stats['failed_files'].append(str(csv_file))
    
    return stats


def generate_report(stats, output_file='mysql_column_mapping_report.txt'):
    """
    生成处理报告
    
    Args:
        stats (dict): 处理统计信息
        output_file (str): 报告文件路径
    """
    logger = logging.getLogger(__name__)
    
    report_content = f"""MySQL 列名映射处理报告
{'='*50}
处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

处理统计:
- 总文件数: {stats['total_files']}
- 成功处理文件数: {stats['processed_files']}
- 失败文件数: {len(stats['failed_files'])}
- 总列数: {stats['total_columns']}
- 成功映射列数: {stats['mapped_columns']}
- 未找到映射列数: {stats['unmapped_columns']}

映射成功率: {stats['mapped_columns']/stats['total_columns']*100:.1f}% (如果总列数 > 0)

"""
    
    if stats['failed_files']:
        report_content += "失败文件列表:\n"
        for failed_file in stats['failed_files']:
            report_content += f"- {failed_file}\n"
    
    # 保存报告
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    logger.info(f"处理报告已生成: {output_file}")
    print(report_content)


def main():
    """主程序入口"""
    logger = setup_logging()
    logger.info("开始 MySQL 列名映射处理")
    
    try:
        # 处理所有文件
        stats = process_all_files()
        
        # 生成报告
        generate_report(stats)
        
        logger.info("MySQL 列名映射处理完成")
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        raise


if __name__ == "__main__":
    main() 