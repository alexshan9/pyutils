#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL表描述导出工具
从MySQL数据库读取所有表名和表备注，生成CSV格式的描述文件
"""

import os
import csv
import configparser
import pymysql
from typing import List, Tuple


class MySQLTableDescGenerator:
    """MySQL表描述生成器"""
    
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
    
    def get_all_tables(self) -> List[str]:
        """获取数据库中所有表名"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """, (self.connection_config['database'],))
                
                tables = cursor.fetchall()
                return [table[0] for table in tables]
        except Exception as e:
            raise Exception(f"获取表列表失败: {e}")
    
    def get_table_comment(self, table_name: str) -> str:
        """获取表备注"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT TABLE_COMMENT 
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """, (self.connection_config['database'], table_name))
                
                result = cursor.fetchone()
                return result[0] if result and result[0] else ""
        except Exception as e:
            print(f"获取表备注失败 ({table_name}): {e}")
            return ""
    
    def get_table_descriptions(self) -> List[Tuple[str, str]]:
        """获取所有表的名称和描述"""
        tables = self.get_all_tables()
        table_descriptions = []
        
        print(f"发现 {len(tables)} 个表，正在获取表描述...")
        
        for table_name in tables:
            table_comment = self.get_table_comment(table_name)
            table_descriptions.append((table_name, table_comment))
            print(f"  ✓ {table_name}: {table_comment if table_comment else '(无备注)'}")
        
        return table_descriptions
    
    def generate_csv(self, output_file: str = 'table_dict_raw.csv'):
        """生成CSV文件"""
        try:
            # 获取表描述数据
            table_descriptions = self.get_table_descriptions()
            
            # 写入CSV文件
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # 写入表头
                writer.writerow(['table', 'table_desc'])
                
                # 写入数据
                for table_name, table_desc in table_descriptions:
                    writer.writerow([table_name, table_desc])
            
            print(f"\n✓ CSV文件已生成: {output_file}")
            print(f"✓ 总计导出 {len(table_descriptions)} 个表的描述信息")
            
            # 显示统计信息
            with_comment = sum(1 for _, desc in table_descriptions if desc.strip())
            without_comment = len(table_descriptions) - with_comment
            print(f"✓ 有备注的表: {with_comment} 个")
            print(f"✓ 无备注的表: {without_comment} 个")
            
        except Exception as e:
            raise Exception(f"生成CSV文件失败: {e}")
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            print("✓ 数据库连接已关闭")


def main():
    """主函数"""
    print("MySQL表描述导出工具")
    print("=" * 40)
    
    generator = MySQLTableDescGenerator()
    
    try:
        # 连接数据库
        print("正在连接MySQL数据库...")
        generator.connect()
        
        # 生成CSV文件
        print("\n正在生成表描述CSV文件...")
        generator.generate_csv()
        
        print("\n✓ 表描述导出完成！")
        
    except Exception as e:
        print(f"\n❌ 导出过程中发生错误: {e}")
    finally:
        generator.close()


if __name__ == "__main__":
    main() 