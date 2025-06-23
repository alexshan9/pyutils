#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL字段描述导出工具
从MySQL数据库读取所有字段名和字段备注，生成CSV格式的字段字典文件
"""

import os
import csv
import configparser
import pymysql
from typing import List, Tuple


class MySQLColumnDescGenerator:
    """MySQL字段描述生成器"""
    
    def __init__(self, config_file: str = 'config.ini'):
        """初始化配置"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file, encoding='utf-8')
        self.connection = None
        
        # MySQL连接配置
        self.connection_config = {
            'host': self.config.get('mysql', 'host'),
            'port': self.config.getint('mysql', 'port'),
            'database': self.config.get('mysql', 'database'),
            'user': self.config.get('mysql', 'user'),
            'password': self.config.get('mysql', 'password'),
            'charset': self.config.get('mysql', 'charset', fallback='utf8mb4'),
            'autocommit': True
        }
    
    def connect(self):
        """连接MySQL数据库"""
        try:
            self.connection = pymysql.connect(**self.connection_config)
            print(f"✓ 成功连接到MySQL数据库: {self.connection_config['host']}:{self.connection_config['port']}/{self.connection_config['database']}")
        except Exception as e:
            raise Exception(f"连接MySQL数据库失败: {e}")
    
    def get_all_columns(self) -> List[Tuple[str, str]]:
        """获取数据库中所有字段信息"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COLUMN_NAME, COLUMN_COMMENT 
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = %s 
                    ORDER BY TABLE_NAME, ORDINAL_POSITION
                """, (self.connection_config['database'],))
                
                columns = cursor.fetchall()
                return [(column[0], column[1] if column[1] else '') for column in columns]
        except Exception as e:
            raise Exception(f"获取字段列表失败: {e}")
    
    def generate_csv(self, output_file: str = 'column_dict_raw.csv'):
        """生成字段字典CSV文件"""
        try:
            # 获取所有字段信息
            all_columns = self.get_all_columns()
            
            print(f"发现 {len(all_columns)} 个字段，正在处理...")
            
            # 去除重复的字段（基于字段名）
            original_count = len(all_columns)
            unique_columns = {}
            
            for column_name, column_comment in all_columns:
                if column_name not in unique_columns:
                    unique_columns[column_name] = {
                        'raw_column': column_name,
                        'remark': column_comment,
                        'new_column': ''  # 根据要求留空
                    }
            
            # 转换回列表并排序
            result_columns = list(unique_columns.values())
            result_columns.sort(key=lambda x: x['raw_column'])
            
            duplicate_count = original_count - len(result_columns)
            
            if duplicate_count > 0:
                print(f"去除了 {duplicate_count} 个重复字段")
            
            # 写入CSV文件
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['raw_column', 'remark', 'new_column']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # 写入表头
                writer.writeheader()
                
                # 写入数据
                writer.writerows(result_columns)
            
            print(f"\n✓ CSV文件已生成: {output_file}")
            print(f"✓ 总计导出 {len(result_columns)} 个唯一字段")
            
            # 显示统计信息
            with_comment = sum(1 for col in result_columns if col['remark'].strip())
            without_comment = len(result_columns) - with_comment
            print(f"✓ 有备注的字段: {with_comment} 个")
            print(f"✓ 无备注的字段: {without_comment} 个")
            
            # 显示前几行作为预览
            print("\n前5行预览：")
            print("raw_column,remark,new_column")
            for i, col in enumerate(result_columns[:5]):
                print(f"{col['raw_column']},{col['remark']},{col['new_column']}")
            
            if len(result_columns) > 5:
                print("...")
                
        except Exception as e:
            raise Exception(f"生成CSV文件失败: {e}")
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            print("✓ 数据库连接已关闭")


def main():
    """主函数"""
    print("MySQL字段描述导出工具")
    print("=" * 40)
    
    generator = MySQLColumnDescGenerator()
    
    try:
        # 连接数据库
        print("正在连接MySQL数据库...")
        generator.connect()
        
        # 生成CSV文件
        print("\n正在生成字段字典CSV文件...")
        generator.generate_csv()
        
        print("\n✓ 字段字典导出完成！")
        
    except Exception as e:
        print(f"\n❌ 导出过程中发生错误: {e}")
    finally:
        generator.close()


if __name__ == "__main__":
    main() 