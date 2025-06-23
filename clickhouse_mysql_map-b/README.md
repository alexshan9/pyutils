# MySQL to ClickHouse 数据迁移工具

一个功能完整的MySQL到ClickHouse数据库迁移解决方案，支持自动化字段映射、表结构转换和数据迁移。

## ✨ 主要特性

- 🔄 **自动化迁移流程**：6步完整迁移工作流
- 🎯 **智能字段映射**：自动生成并支持自定义字段映射关系
- 📊 **数据类型转换**：完整的MySQL到ClickHouse数据类型映射
- 🚀 **批量处理**：支持大数据量的批量迁移
- 📝 **详细日志**：完整的操作日志和进度跟踪
- ⚡ **性能优化**：可配置的批处理大小和并发控制
- 🛡️ **数据验证**：迁移后数据一致性验证
- 🔧 **灵活配置**：丰富的配置选项满足不同需求

## 📁 项目结构

```
clickhouse_mysql_map-b/
├── README.md                           # 项目说明文档
├── config.ini.example                  # 配置文件模板
├── guide.md                            # 详细使用指南
├── todo-list.md                        # 任务清单
├── main.py                             # 主程序入口
├── 01generate_column_dict_csv.py       # 步骤1：生成字段字典
├── 02generate_table_dict_csv.py        # 步骤2：生成表字典
├── 03column_rename.py                  # 步骤3：字段映射处理
├── 04table_mapper.py                   # 步骤4：表映射处理
├── column_dict_raw.csv                 # 原始字段字典
├── column_dict.csv                     # 处理后字段字典（需手工编辑）
├── table_dict_raw.csv                  # 原始表字典
├── table_dict.csv                      # 处理后表字典（需手工编辑）
├── mysql_column_mapper/                # 步骤3输出目录
└── logs/                               # 日志文件目录
```

## 🚀 快速开始

### 环境要求

- Python 3.7+
- MySQL 5.7+ 或 8.0+
- ClickHouse 20.3+

### 安装依赖

```bash
pip install pymysql clickhouse-driver pandas tqdm configparser
```

### 配置设置

1. 复制配置文件模板：
```bash
cp config.ini.example config.ini
```

2. 编辑 `config.ini` 文件，配置数据库连接信息：

```ini
[clickhouse]
host = 192.168.10.129
port = 9000
database = abc
user = root
password = 123456

[mysql]
host = 192.168.10.129
port = 9527
database = abc
user = root
password = 123456
charset = utf8mb4

[settings]
# 批量插入的批次大小
batch_size = 100
# 是否显示详细日志
verbose = True
# 如果表结构不匹配是否自动删除重建
auto_recreate_table = True
# 是否启用数据一致性验证
enable_validation = True
# 验证时检查的样本数据行数
validation_sample_size = 5
# 如果表已存在是否跳过处理
skip_existing_tables = True
```

## 📋 完整迁移流程

### 步骤1：生成字段字典

从MySQL数据库提取所有字段信息，生成原始字段字典。

```bash
python 01generate_column_dict_csv.py
```

**输出文件**：`column_dict_raw.csv`
**格式**：
```csv
raw_column,remark,new_column
id,主键ID,
name,用户名,
create_time,创建时间,
```

### 步骤2：生成表字典

从MySQL数据库提取所有表信息，生成原始表字典。

```bash
python 02generate_table_dict_csv.py
```

**输出文件**：`table_dict_raw.csv`
**格式**：
```csv
table,table_desc
users,用户表
orders,订单表
products,产品表
```

### 步骤3：手工编辑映射关系

这是**唯一需要手工干预**的步骤。

1. **编辑字段映射** - 修改 `column_dict.csv`：
```csv
raw_column,remark,new_column
id,主键ID,user_id
name,用户名,user_name
create_time,创建时间,created_at
```

2. **编辑表映射** - 修改 `table_dict.csv`：
```csv
table,table_desc,new_table_name
users,用户表,dim_users
orders,订单表,fact_orders
products,产品表,dim_products
```

### 步骤4：生成字段映射文件

根据编辑好的字段字典，为每个表生成具体的字段映射文件。

```bash
python 03column_rename.py
```

**输出目录**：`mysql_column_mapper/`
**输出格式**：每个表一个CSV文件，包含字段映射关系

### 步骤5：生成最终映射文件

将字段映射文件重命名为最终格式，准备数据迁移。

```bash
python 04table_mapper.py
```

**输出格式**：`mysql_table_name-clickhouse_table_name.csv`

### 步骤6：执行数据迁移

运行主程序，开始数据迁移过程。

```bash
python main.py
```

**功能**：
- 自动创建ClickHouse表结构
- 批量读取MySQL数据
- 数据类型转换和映射
- 实时进度显示
- 数据一致性验证
- 详细的迁移报告

## ⚙️ 配置参数详解

### ClickHouse 配置
- `host`: ClickHouse服务器地址
- `port`: ClickHouse服务端口（默认9000）
- `database`: 目标数据库名
- `user`: 用户名
- `password`: 密码

### MySQL 配置
- `host`: MySQL服务器地址
- `port`: MySQL服务端口（默认3306）
- `database`: 源数据库名
- `user`: 用户名
- `password`: 密码
- `charset`: 字符编码（建议utf8mb4）

### 迁移设置
- `batch_size`: 批处理大小，影响内存使用和性能
- `verbose`: 详细日志输出
- `auto_recreate_table`: 遇到表结构冲突时是否自动重建
- `enable_validation`: 是否启用数据验证
- `validation_sample_size`: 验证采样数量
- `skip_existing_tables`: 是否跳过已存在的表

## 🔄 数据类型映射

| MySQL类型 | ClickHouse类型 | 说明 |
|-----------|----------------|------|
| TINYINT | Int8 | 8位整数 |
| SMALLINT | Int16 | 16位整数 |
| INT/INTEGER | Int32 | 32位整数 |
| BIGINT | Int64 | 64位整数 |
| FLOAT | Float32 | 单精度浮点 |
| DOUBLE | Float64 | 双精度浮点 |
| DECIMAL | Decimal | 精确小数 |
| VARCHAR/TEXT | String | 字符串 |
| DATE | Date32 | 日期 |
| DATETIME/TIMESTAMP | DateTime64(6) | 日期时间 |
| TINYINT(1) | Bool | 布尔值 |
| JSON | String | JSON字符串 |

## 📊 日志和监控

### 日志文件位置
```
logs/
├── mysql_column_mapper_YYYYMMDD_HHMMSS.log    # 字段映射日志
├── clickhouse_file_organizer_YYYYMMDD_HHMMSS.log  # 文件整理日志
└── migration_YYYYMMDD_HHMMSS.log               # 迁移过程日志
```

### 监控指标
- 处理表数量和进度
- 数据行数统计
- 处理时间和性能
- 错误和警告信息
- 数据验证结果

## 🛠️ 故障排除

### 常见问题

**Q: 连接数据库失败**
```
A: 检查网络连接和防火墙设置
   验证数据库服务是否启动
   确认用户名密码正确
   检查数据库权限设置
```

**Q: 字段类型转换失败**
```
A: 检查数据类型映射配置
   验证源数据是否包含NULL值
   调整ClickHouse表结构定义
   查看详细错误日志
```

**Q: 迁移性能慢**
```
A: 调整batch_size参数
   优化网络连接
   检查磁盘I/O性能
   考虑并行处理
```

**Q: 数据验证失败**
```
A: 检查字段映射是否正确
   验证数据类型转换
   排查特殊字符问题
   增加validation_sample_size
```

### 调试模式

启用详细日志进行调试：
```ini
[settings]
verbose = True
```

## 📈 性能优化建议

1. **批处理大小**：根据可用内存调整batch_size
2. **网络优化**：使用高速网络连接
3. **并行处理**：对于大型数据库，考虑分表并行迁移
4. **索引策略**：迁移完成后添加适当的索引
5. **压缩设置**：配置ClickHouse压缩算法

## 📚 示例

### 完整迁移示例

```bash
# 1. 配置数据库连接
cp config.ini.example config.ini
vim config.ini

# 2. 生成字段和表字典
python 01generate_column_dict_csv.py
python 02generate_table_dict_csv.py

# 3. 手工编辑映射关系
vim column_dict.csv
vim table_dict.csv

# 4. 生成映射文件
python 03column_rename.py
python 04table_mapper.py

# 5. 执行迁移
python main.py
```

### 批处理脚本示例

```bash
#!/bin/bash
# auto_migrate.sh

echo "开始MySQL到ClickHouse迁移..."

# 检查配置文件
if [ ! -f "config.ini" ]; then
    echo "错误：config.ini文件不存在"
    exit 1
fi

# 执行迁移步骤
python 01generate_column_dict_csv.py && \
python 02generate_table_dict_csv.py && \
echo "请编辑column_dict.csv和table_dict.csv文件，然后按Enter继续..." && \
read && \
python 03column_rename.py && \
python 04table_mapper.py && \
python main.py

echo "迁移完成！"
```

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## �� 许可证

本项目采用MIT许可证。