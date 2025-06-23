#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL到ClickHouse数据映射工具 V3
支持从MySQL数据库读取数据，映射到ClickHouse库
支持表备注、字段备注、自增ID排序、数据一致性验证
"""

import os
import sys
import csv
import configparser
import pymysql
from typing import List, Dict, Tuple, Optional, Any
from tqdm import tqdm
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
from dataclasses import dataclass


@dataclass
class TableMapping:
    """表映射信息"""
    mysql_table: str
    clickhouse_table: str
    field_mappings: Dict[str, Tuple[str, str]]  # {mysql_field: (clickhouse_field, clickhouse_type)}


@dataclass
class MigrationResult:
    """迁移结果"""
    table_mapping: TableMapping
    success: bool
    mysql_rows: int
    clickhouse_rows: int
    error_message: str = ""
    processing_time: float = 0.0


class MySQLClient:
    """MySQL数据库客户端"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str, charset: str = 'utf8mb4'):
        self.connection_config = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password,
            'charset': charset,
            'autocommit': True
        }
        self.connection = None
    
    def connect(self):
        """连接MySQL数据库"""
        try:
            self.connection = pymysql.connect(**self.connection_config)
            print(f"✓ 成功连接到MySQL数据库: {self.connection_config['host']}:{self.connection_config['port']}/{self.connection_config['database']}")
        except Exception as e:
            raise Exception(f"连接MySQL数据库失败: {e}")
    
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
            print(f"获取表备注失败: {e}")
            return ""
    
    def get_table_structure(self, table_name: str) -> List[Tuple[str, str, str]]:
        """获取表结构: (字段名, 数据类型, 备注)"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT 
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s 
                    ORDER BY ORDINAL_POSITION
                """, (self.connection_config['database'], table_name))
                return cursor.fetchall()
        except Exception as e:
            raise Exception(f"获取表结构失败: {e}")
    
    def get_table_data(self, table_name: str, batch_size: int = 1000):
        """批量获取表数据"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                total_rows = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT * FROM `{table_name}`")
                
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    yield rows, total_rows
        except Exception as e:
            raise Exception(f"读取表数据失败: {e}")
    
    def get_table_row_count(self, table_name: str) -> int:
        """获取表行数"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"获取表行数失败: {e}")
            return 0
    
    def close(self):
        """关闭连接"""
        if self.connection:
            self.connection.close()


class TypeMapper:
    """MySQL到ClickHouse类型映射器"""
    
    TYPE_MAPPING = {
        'TINYINT': 'Int8',
        'TINYINT UNSIGNED': 'UInt8',
        'SMALLINT': 'Int16',
        'SMALLINT UNSIGNED': 'UInt16',
        'MEDIUMINT': 'Int32',
        'MEDIUMINT UNSIGNED': 'UInt32',
        'INT': 'Int32',
        'INTEGER': 'Int32',
        'INT UNSIGNED': 'UInt32',
        'INTEGER UNSIGNED': 'UInt32',
        'BIGINT': 'Int64',
        'BIGINT UNSIGNED': 'UInt64',
        'YEAR': 'Int16',
        'FLOAT': 'Float32',
        'DOUBLE': 'Float64',
        'DECIMAL': 'Decimal',
        'CHAR': 'String',
        'VARCHAR': 'String',
        'TINYTEXT': 'String',
        'TEXT': 'String',
        'MEDIUMTEXT': 'String',
        'LONGTEXT': 'String',
        'BINARY': 'String',
        'VARBINARY': 'String',
        'TINYBLOB': 'String',
        'BLOB': 'String',
        'MEDIUMBLOB': 'String',
        'LONGBLOB': 'String',
        'DATE': 'Date32',
        'TIME': 'DateTime64(6)',
        'DATETIME': 'DateTime64(6)',
        'TIMESTAMP': 'DateTime64(6)',
        'ENUM': 'LowCardinality(String)',
        'SET': 'String',
        'JSON': 'String',
        'GEOMETRY': 'String',
        'POINT': 'String',
        'LINESTRING': 'String',
        'POLYGON': 'String',
        'VECTOR': 'Array(Float32)'
    }
    
    @classmethod
    def map_mysql_type_to_clickhouse(cls, mysql_type: str) -> str:
        """将MySQL类型映射为ClickHouse类型"""
        # 处理带参数的类型，如 VARCHAR(255), DECIMAL(10,2)
        base_type = mysql_type.upper().split('(')[0].strip()
        
        # 处理TINYINT(1)作为Bool的特殊情况
        if mysql_type.upper().startswith('TINYINT(1)'):
            return 'Bool'
        
        # 处理DECIMAL类型保持精度
        if base_type == 'DECIMAL' and '(' in mysql_type:
            precision_part = mysql_type[mysql_type.find('(')+1:mysql_type.find(')')]
            return f'Decimal({precision_part})'
        
        return cls.TYPE_MAPPING.get(base_type, 'String')


class CSVMappingLoader:
    """CSV映射文件加载器"""
    
    def __init__(self):
        self.mappings = {}
    
    def load_csv_mapping(self, csv_file: str) -> TableMapping:
        """从CSV文件加载字段映射关系"""
        try:
            # 从文件名提取表名信息
            filename = os.path.basename(csv_file)
            if '-' in filename:
                parts = filename.replace('.csv', '').split('-')
                mysql_table = parts[0]
                clickhouse_table = parts[1]
            else:
                raise ValueError(f"CSV文件名格式不正确: {filename}")
            
            field_mappings = {}
            
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    mysql_field = row['mysql'].strip()
                    clickhouse_field = row['clickhouse'].strip()
                    # 类型映射将在后续根据MySQL实际类型确定
                    field_mappings[mysql_field] = (clickhouse_field, 'String')
            
            return TableMapping(
                mysql_table=mysql_table,
                clickhouse_table=clickhouse_table,
                field_mappings=field_mappings
            )
        except Exception as e:
            raise Exception(f"加载CSV映射文件失败 {csv_file}: {e}")


class ClickHouseClientV3:
    """ClickHouse客户端V3"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.client = Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print(f"✓ 成功连接到ClickHouse数据库: {host}:{port}/{database}")
    
    def drop_table_if_exists(self, table_name: str):
        """删除表（如果存在）"""
        try:
            self.client.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            print(f"✓ 已删除表: {table_name}")
        except Exception as e:
            print(f"删除表失败: {e}")
    
    def create_table(self, table_name: str, fields: List[Tuple[str, str, str]], table_comment: str = ""):
        """创建ClickHouse表，使用第一个字段作为主键"""
        try:
            # 构建字段定义
            field_definitions = []
            primary_key_field = None
            
            for field_name, field_type, comment in fields:
                if comment:
                    field_definitions.append(f"`{field_name}` {field_type} COMMENT '{comment}'")
                else:
                    field_definitions.append(f"`{field_name}` {field_type}")
                
                # 使用第一个字段作为主键
                if primary_key_field is None:
                    primary_key_field = field_name
            
            # 如果没有字段，使用默认主键
            if primary_key_field is None:
                raise Exception("表必须至少包含一个字段")
            
            fields_sql = ',\n    '.join(field_definitions)
            
            # 构建表注释
            table_comment_sql = f"COMMENT '{table_comment}'" if table_comment else ""
            
            create_sql = f"""
            CREATE TABLE `{table_name}` (
                {fields_sql}
            ) ENGINE = MergeTree()
            ORDER BY `{primary_key_field}`
            {table_comment_sql}
            """
            
            self.client.execute(create_sql)
            print(f"✓ 成功创建表: {table_name}，主键字段: {primary_key_field}")
            
        except Exception as e:
            raise Exception(f"创建表失败: {e}")
    
    def insert_batch(self, table_name: str, field_names: List[str], data_batch: List[List[Any]], field_types: List[str] = None):
        """批量插入数据，使用原始MySQL数据"""
        try:
            # print(f"  🔍 开始处理批次数据，行数: {len(data_batch)}, 字段数: {len(field_names)}")
            # if field_types:
            #     print(f"  🔍 字段类型: {dict(zip(field_names, field_types))}")
            
            # 处理NULL值和类型转换，不添加自增ID
            processed_data = []
            for i, row in enumerate(data_batch):
                # 处理每个字段的值
                processed_row = []
                for j, value in enumerate(row):
                    try:
                        field_name = field_names[j] if j < len(field_names) else f"field_{j}"
                        field_type = field_types[j] if field_types and j < len(field_types) else "String"
                        
                        if value is None:
                            # 根据字段类型提供合适的默认值
                            default_value = self._get_default_value_for_type(field_type)
                            processed_row.append(default_value)
                            # if i < 3:  # 只记录前3行的详细信息
                            #     print(f"    🔍 字段 {field_name}: NULL -> {default_value} (类型: {field_type})")
                        else:
                            # 根据ClickHouse字段类型转换值
                            converted_value = self._convert_value_for_clickhouse(value, field_type, field_name)
                            processed_row.append(converted_value)
                            if i < 1:  # 只记录前3行的详细信息
                                print(f"    🔍 字段 {field_name}: {repr(value)} -> {repr(converted_value)} (类型: {field_type}, Python类型: {type(converted_value).__name__})")
                    
                    except Exception as field_error:
                        print(f"    ⚠ 字段 {field_name} 处理失败: {field_error}, 使用默认值")
                        default_value = self._get_default_value_for_type(field_type if field_types and j < len(field_types) else "String")
                        processed_row.append(default_value)
                
                processed_data.append(processed_row)
            
            # 插入数据
            insert_fields = field_names
            # print(f"  🔍 准备插入数据: 表={table_name}, 字段={insert_fields}")
            
            # 增加详细的插入前检查
            # if processed_data:
                # print(f"  🔍 即将插入的数据样本（前2行）:")
                # for i, row in enumerate(processed_data[:2]):
                #     print(f"    行{i+1}: {[type(val).__name__ + ':' + repr(val)[:50] + ('...' if len(repr(val)) > 50 else '') for val in row]}")
            
            try:
                self.client.execute(
                    f"INSERT INTO `{table_name}` ({', '.join(f'`{field}`' for field in insert_fields)}) VALUES",
                    processed_data
                )
                # print(f"  ✓ 成功插入 {len(processed_data)} 行数据")
            except Exception as insert_error:
                print(f"  ❌ ClickHouse插入详细错误: {insert_error}")
                print(f"  🔍 错误类型: {type(insert_error).__name__}")
                # print(f"  ✓ 成功插入 {len(processed_data)} 行数据")
                
                # 尝试逐行插入以定位问题行
                print(f"  🔍 尝试逐行插入以定位问题...")
                for i, row in enumerate(processed_data[:5]):  # 只检查前5行
                    try:
                        self.client.execute(
                            f"INSERT INTO `{table_name}` ({', '.join(f'`{field}`' for field in insert_fields)}) VALUES",
                            [row]
                        )
                        print(f"    ✓ 第{i+1}行插入成功")
                    except Exception as row_error:
                        print(f"    ❌ 第{i+1}行插入失败: {row_error}")
                        print(f"    🔍 问题行数据: {[f'{type(val).__name__}:{repr(val)}' for val in row]}")
                        
                        # 检查每个字段值
                        for j, (field, val) in enumerate(zip(insert_fields, row)):
                            if isinstance(val, str) and len(val) > 0:
                                print(f"      字段{field}: 长度={len(val)}, 内容={repr(val[:100])}")
                                # 检查特殊字符
                                special_chars = [c for c in val if ord(c) < 32 and c not in '\t\n\r']
                                if special_chars:
                                    print(f"      ⚠ 发现特殊字符: {[hex(ord(c)) for c in special_chars[:10]]}")
                        break
                
                raise Exception(f"批量插入数据失败: {insert_error}")
            
        except Exception as e:
            print(f"  ✗ 批量插入失败: {e}")
            # 打印第一行数据用于调试
            if data_batch:
                print(f"  🔍 第一行原始数据: {data_batch[0]}")
                if 'processed_data' in locals() and processed_data:
                    print(f"  🔍 第一行处理后数据: {processed_data[0]}")
            raise Exception(f"批量插入数据失败: {e}")
    
    def _get_default_value_for_type(self, field_type: str):
        """根据字段类型获取默认值"""
        field_type_lower = field_type.lower()
        if any(num_type in field_type_lower for num_type in ['int', 'uint']):
            return 0
        elif any(float_type in field_type_lower for float_type in ['float', 'double']):
            return 0.0
        elif 'decimal' in field_type_lower:
            return 0
        elif field_type_lower == 'date32':
            from datetime import date
            return date(2000, 1, 1)
        elif 'datetime' in field_type_lower:
            from datetime import datetime
            return datetime(2000, 1, 1, 0, 0, 0)
        elif field_type_lower == 'bool':
            return False
        else:
            return ''
    
    def _convert_value_for_clickhouse(self, value, field_type: str, field_name: str = ""):
        """将MySQL值转换为ClickHouse兼容的格式"""
        if value is None:
            return self._get_default_value_for_type(field_type)
        
        field_type_lower = field_type.lower()
        
        try:
            # 处理日期时间类型
            if 'datetime' in field_type_lower or 'timestamp' in field_type_lower:
                if hasattr(value, 'strftime'):
                    # datetime对象，直接返回，ClickHouse驱动会处理
                    return value
                elif isinstance(value, str):
                    # 字符串格式的日期时间，尝试解析为datetime对象
                    try:
                        from datetime import datetime
                        # 尝试常见的日期时间格式
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                            try:
                                return datetime.strptime(value, fmt)
                            except ValueError:
                                continue
                        # 如果都不匹配，返回默认值
                        return datetime(2000, 1, 1)
                    except Exception:
                        from datetime import datetime
                        return datetime(2000, 1, 1)
                else:
                    from datetime import datetime
                    return datetime(2000, 1, 1)
            
            # 处理日期类型
            elif field_type_lower == 'date32':
                if hasattr(value, 'date'):
                    # date或datetime对象，返回date部分
                    return value.date() if hasattr(value, 'date') else value
                elif isinstance(value, str):
                    # 字符串格式的日期，尝试解析为date对象
                    try:
                        from datetime import datetime
                        for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']:
                            try:
                                return datetime.strptime(value, fmt).date()
                            except ValueError:
                                continue
                        # 如果都不匹配，返回默认值
                        from datetime import date
                        return date(2000, 1, 1)
                    except Exception:
                        from datetime import date
                        return date(2000, 1, 1)
                else:
                    from datetime import date
                    return date(2000, 1, 1)
            
            # 处理数值类型
            elif any(num_type in field_type_lower for num_type in ['int', 'uint']):
                if isinstance(value, (int, float)):
                    return int(value)
                elif isinstance(value, str) and value.strip():
                    try:
                        return int(float(value))  # 先转float再转int，处理小数字符串
                    except (ValueError, TypeError):
                        return 0
                else:
                    return 0
            
            # 处理浮点类型
            elif any(float_type in field_type_lower for float_type in ['float', 'double', 'decimal']):
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str) and value.strip():
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return 0.0
                else:
                    return 0.0
            
            # 处理布尔类型
            elif field_type_lower == 'bool':
                if isinstance(value, bool):
                    return value
                elif isinstance(value, (int, float)):
                    return bool(value)
                elif isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                else:
                    return False
            
            # 处理字符串类型
            else:
                if isinstance(value, str):
                    try:
                        # 清理和规范化字符串
                        cleaned_value = self._clean_string_value(value)
                        return cleaned_value
                    except Exception:
                        return ''
                else:
                    return str(value)
        
        except Exception as e:
            print(f"    ⚠ 值转换失败 {field_name}: {repr(value)} -> {field_type}, 错误: {e}")
            return self._get_default_value_for_type(field_type)
    
    def _clean_string_value(self, value: str) -> str:
        """清理字符串值，移除可能导致问题的特殊字符"""
        if not value:
            return ''
        
        try:
            # 1. 移除NULL字符和控制字符
            cleaned = ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
            
            # 2. 替换可能有问题的引号
            cleaned = cleaned.replace('\x00', '').replace('\x01', '').replace('\x02', '')
            cleaned = cleaned.replace('\x03', '').replace('\x04', '').replace('\x05', '')
            cleaned = cleaned.replace('\x06', '').replace('\x07', '').replace('\x08', '')
            cleaned = cleaned.replace('\x0b', '').replace('\x0c', '').replace('\x0e', '')
            cleaned = cleaned.replace('\x0f', '').replace('\x10', '').replace('\x11', '')
            cleaned = cleaned.replace('\x12', '').replace('\x13', '').replace('\x14', '')
            cleaned = cleaned.replace('\x15', '').replace('\x16', '').replace('\x17', '')
            cleaned = cleaned.replace('\x18', '').replace('\x19', '').replace('\x1a', '')
            cleaned = cleaned.replace('\x1b', '').replace('\x1c', '').replace('\x1d', '')
            cleaned = cleaned.replace('\x1e', '').replace('\x1f', '')
            
            # 3. 确保可以UTF-8编码
            cleaned.encode('utf-8')
            
            # 4. 限制长度（防止过长字符串）
            if len(cleaned) > 1000:
                cleaned = cleaned[:1000]
            
            return cleaned
        
        except Exception as e:
            print(f"    ⚠ 字符串清理失败: {repr(value)}, 错误: {e}")
            return ''
    
    def get_table_row_count(self, table_name: str) -> int:
        """获取表行数"""
        try:
            result = self.client.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            return result[0][0]
        except Exception as e:
            print(f"获取ClickHouse表行数失败: {e}")
            return 0
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            result = self.client.execute(f"EXISTS TABLE `{table_name}`")
            return result[0][0] == 1
        except Exception as e:
            print(f"检查表存在性失败: {e}")
            return False
    
    def get_table_structure(self, table_name: str) -> List[Tuple[str, str, str]]:
        """获取表结构信息"""
        try:
            result = self.client.execute(f"DESCRIBE TABLE `{table_name}`")
            return [(row[0], row[1], row[4] if len(row) > 4 else "") for row in result]
        except Exception as e:
            print(f"获取表结构失败: {e}")
            return []
    
    def close(self):
        """关闭连接"""
        try:
            self.client.disconnect()
        except:
            pass


class DataMigrator:
    """数据迁移协调器"""
    
    def __init__(self, config_file: str = 'config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file, encoding='utf-8')
        
        # 初始化客户端
        self.mysql_client = None
        self.clickhouse_client = None
        self.csv_loader = CSVMappingLoader()
        
        # 配置参数
        self.batch_size = self.config.getint('settings', 'batch_size', fallback=1000)
        self.auto_recreate = self.config.getboolean('settings', 'auto_recreate_table', fallback=True)
        self.skip_existing = self.config.getboolean('settings', 'skip_existing_tables', fallback=False)
    
    def connect_databases(self):
        """连接数据库"""
        # 连接MySQL
        self.mysql_client = MySQLClient(
            host=self.config.get('mysql', 'host'),
            port=self.config.getint('mysql', 'port'),
            database=self.config.get('mysql', 'database'),
            user=self.config.get('mysql', 'user'),
            password=self.config.get('mysql', 'password'),
            charset=self.config.get('mysql', 'charset', fallback='utf8mb4')
        )
        self.mysql_client.connect()
        
        # 连接ClickHouse
        self.clickhouse_client = ClickHouseClientV3(
            host=self.config.get('clickhouse', 'host'),
            port=self.config.getint('clickhouse', 'port'),
            database=self.config.get('clickhouse', 'database'),
            user=self.config.get('clickhouse', 'user'),
            password=self.config.get('clickhouse', 'password')
        )
    
    def migrate_table(self, csv_file: str) -> MigrationResult:
        """迁移单个表的数据"""
        start_time = datetime.now()
        
        try:
            # 加载映射配置
            table_mapping = self.csv_loader.load_csv_mapping(csv_file)
            print(f"\n开始迁移表: {table_mapping.mysql_table} -> {table_mapping.clickhouse_table}")
            
            # 检查MySQL表是否存在
            mysql_row_count = self.mysql_client.get_table_row_count(table_mapping.mysql_table)
            if mysql_row_count == 0:
                print(f"⚠ MySQL表 {table_mapping.mysql_table} 不存在或为空")
                return MigrationResult(
                    table_mapping=table_mapping,
                    success=False,
                    mysql_rows=0,
                    clickhouse_rows=0,
                    error_message="MySQL表不存在或为空"
                )
            
            # 检查ClickHouse表是否存在
            if self.clickhouse_client.table_exists(table_mapping.clickhouse_table):
                if self.skip_existing:
                    print(f"⚠ ClickHouse表 {table_mapping.clickhouse_table} 已存在，跳过处理")
                    return MigrationResult(
                        table_mapping=table_mapping,
                        success=True,
                        mysql_rows=mysql_row_count,
                        clickhouse_rows=self.clickhouse_client.get_table_row_count(table_mapping.clickhouse_table),
                        processing_time=(datetime.now() - start_time).total_seconds()
                    )
                elif self.auto_recreate:
                    print(f"⚠ ClickHouse表 {table_mapping.clickhouse_table} 已存在，将删除重建")
                    self.clickhouse_client.drop_table_if_exists(table_mapping.clickhouse_table)
            
            # 获取MySQL表结构
            mysql_structure = self.mysql_client.get_table_structure(table_mapping.mysql_table)
            table_comment = self.mysql_client.get_table_comment(table_mapping.mysql_table)
            
            # 构建ClickHouse字段定义
            clickhouse_fields = []
            final_field_mappings = {}
            
            for mysql_field, mysql_type, mysql_comment in mysql_structure:
                if mysql_field in table_mapping.field_mappings:
                    clickhouse_field, _ = table_mapping.field_mappings[mysql_field]
                    
                    clickhouse_type = TypeMapper.map_mysql_type_to_clickhouse(mysql_type)
                    clickhouse_fields.append((clickhouse_field, clickhouse_type, mysql_comment))
                    final_field_mappings[mysql_field] = (clickhouse_field, clickhouse_type)
            
            # 创建ClickHouse表
            print(f"✓ 最终映射字段数量: {len(clickhouse_fields)} (原MySQL表字段数: {len(mysql_structure)})")
            self.clickhouse_client.create_table(
                table_mapping.clickhouse_table,
                clickhouse_fields,
                table_comment
            )
            
            # 迁移数据
            total_migrated = 0
            progress_bar = tqdm(total=mysql_row_count, desc=f"迁移数据")
            print(f"开始迁移数据，总计 {mysql_row_count} 行，批次大小: {self.batch_size}")
            
            batch_count = 0
            for batch_data, _ in self.mysql_client.get_table_data(table_mapping.mysql_table, self.batch_size):
                batch_count += 1
                # print(f"\n📦 处理第 {batch_count} 批次，行数: {len(batch_data)}")
                # 重新排序数据以匹配字段映射，同时忽略被过滤的id字段
                mapped_data = []
                if batch_count == 1:  # 只在第一批次打印详细映射信息
                    print(f"  🔍 字段映射详情:")
                    for i, (mysql_field, mysql_type, _) in enumerate(mysql_structure):
                        if mysql_field in final_field_mappings:
                            ch_field, ch_type = final_field_mappings[mysql_field]
                            print(f"    {i}: {mysql_field}({mysql_type}) -> {ch_field}({ch_type})")
                        else:
                            print(f"    {i}: {mysql_field}({mysql_type}) -> [跳过]")
                
                for row_idx, row in enumerate(batch_data):
                    mapped_row = []
                    for i, (mysql_field, mysql_type, _) in enumerate(mysql_structure):
                        if mysql_field in final_field_mappings:
                            mapped_row.append(row[i])
                    mapped_data.append(mapped_row)
                    
                    # 打印第一行数据的详细信息
                    if batch_count == 1 and row_idx == 0:
                        print(f"  🔍 第一行原始数据长度: {len(row)}")
                        print(f"  🔍 第一行映射数据长度: {len(mapped_row)}")
                        for i, (mysql_field, mysql_type, _) in enumerate(mysql_structure):
                            if mysql_field in final_field_mappings:
                                ch_field, ch_type = final_field_mappings[mysql_field]
                                value = row[i] if i < len(row) else "越界"
                                print(f"    {mysql_field}={repr(value)} -> {ch_field}({ch_type})")
                
                # print(f"  📊 映射完成: {len(batch_data)} 行 -> {len(mapped_data)} 行，每行字段数: {len(mapped_data[0]) if mapped_data else 0}")
                
                # 插入到ClickHouse，只包含最终映射的字段
                clickhouse_field_names = [final_field_mappings[field][0] for field, _, _ in mysql_structure if field in final_field_mappings]
                clickhouse_field_types = [final_field_mappings[field][1] for field, _, _ in mysql_structure if field in final_field_mappings]
                self.clickhouse_client.insert_batch(
                    table_mapping.clickhouse_table,
                    clickhouse_field_names,
                    mapped_data,
                    clickhouse_field_types
                )
                
                total_migrated += len(mapped_data)
                progress_bar.update(len(mapped_data))
            
            progress_bar.close()
            
            # 验证迁移结果
            clickhouse_row_count = self.clickhouse_client.get_table_row_count(table_mapping.clickhouse_table)
            
            result = MigrationResult(
                table_mapping=table_mapping,
                success=True,
                mysql_rows=mysql_row_count,
                clickhouse_rows=clickhouse_row_count,
                processing_time=(datetime.now() - start_time).total_seconds()
            )
            
            print(f"✓ 迁移完成: MySQL({mysql_row_count}行) -> ClickHouse({clickhouse_row_count}行)")
            return result
            
        except Exception as e:
            return MigrationResult(
                table_mapping=table_mapping if 'table_mapping' in locals() else None,
                success=False,
                mysql_rows=0,
                clickhouse_rows=0,
                error_message=str(e),
                processing_time=(datetime.now() - start_time).total_seconds()
            )
    
    def migrate_all_tables(self, csv_directory: str = "./clickhouse_mapper") -> List[MigrationResult]:
        """迁移所有CSV配置的表"""
        csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
        
        if not csv_files:
            print("未找到CSV映射文件")
            return []
        
        print(f"发现 {len(csv_files)} 个CSV映射文件")
        results = []
        
        for csv_file in csv_files:
            csv_path = os.path.join(csv_directory, csv_file)
            result = self.migrate_table(csv_path)
            results.append(result)
        
        return results
    
    def print_migration_summary(self, results: List[MigrationResult]):
        """打印迁移汇总"""
        print("\n" + "="*80)
        print("迁移汇总报告")
        print("="*80)
        
        successful_migrations = [r for r in results if r.success]
        failed_migrations = [r for r in results if not r.success]
        
        print(f"总计表数: {len(results)}")
        print(f"成功迁移: {len(successful_migrations)}")
        print(f"失败数量: {len(failed_migrations)}")
        
        if successful_migrations:
            print(f"\n成功迁移的表:")
            for result in successful_migrations:
                print(f"  ✓ {result.table_mapping.mysql_table} -> {result.table_mapping.clickhouse_table}")
                print(f"    MySQL行数: {result.mysql_rows}, ClickHouse行数: {result.clickhouse_rows}")
                print(f"    处理时间: {result.processing_time:.2f}秒")
        
        if failed_migrations:
            print(f"\n失败的表:")
            for result in failed_migrations:
                table_name = result.table_mapping.mysql_table if result.table_mapping else "未知"
                print(f"  ✗ {table_name}: {result.error_message}")
    
    def close(self):
        """关闭所有连接"""
        if self.mysql_client:
            self.mysql_client.close()
        if self.clickhouse_client:
            self.clickhouse_client.close()


def main():
    """主函数"""
    print("MySQL到ClickHouse数据映射工具 V3")
    print("="*50)
    
    migrator = DataMigrator()
    
    try:
        # 连接数据库
        print("正在连接数据库...")
        migrator.connect_databases()
        
        # 执行迁移
        results = migrator.migrate_all_tables()
        
        # 打印汇总
        migrator.print_migration_summary(results)
        
    except Exception as e:
        print(f"迁移过程中发生错误: {e}")
    finally:
        migrator.close()


if __name__ == "__main__":
    main() 