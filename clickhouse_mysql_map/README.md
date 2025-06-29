## 项目概述
实现从mysql 数据库 -> click house 表的迁移。可自定义映射的表名 和 字段名,批量处理。维护了 table_dcit,column_dict 作为映射的依据

table_dcit: 映射表
column_dict：映射字段

## 运行方式：
1. 正确配置config.ini，看文件末尾

2. docker方式运行：
   + 构建镜像: `docker build -t clickhouse_mapper:latest .` OR `docker load -i clickhouse_mapper.tar`
   + 选择config.ini 下 `run_mode` ，选择运行的模式
      + 0: 收集模式，将mysql 的数据库中 表名，字段名 及 对应的备注 收集到 `table_dcit_raw.csv` 和 `column_dict_raw.csv`中，
      + 1：写入模式，运行此模式，最低需要满足如下条件
         + `table_dcit_raw.csv` 和 `column_dict_raw.csv` 正确配置，并重命名为 `table_dcit.csv` 和 `column_dict.csv`
   + docker compose up


## 部分重要运行文件说明

### table_dict_raw.csv：
> 收集模式生成的文件，需要补充内容，规范如下
```csv
table,table_desc,new_table_name
d_event_czzt,事件处置状态,road_event_disposal_status
d_event_lx,事件类型,road_event_type
d_event_ly,事件来源,road_event_source
```

### column_dict_raw.csv
> 收集模式生成的文件，需要补充内容，规范如下
```csv
raw_column,remark,new_column
ATMOS,逐公里气压,atmospheric_pressure
DT,日期 格式 yyyy-MM-dd,date
EXTEND,拓展字段,extended_field
```

### target_table.txt
> 可选，默认为空，只同步设置的表，多表换行显示。如果不设置，默认同步所有。规范如下
```txt
t_car_qzc_gps
t_car_qzc
```

### ignore_table.txt
> 可选，默认为空，和 target_table.txt 只能2选1 设置，将文件中的表，不进行同步。多表换行显示。
```txt
t_car_qzc_gps
t_car_qzc
```

## 配置项 config.ini
只允许进行如下配置修改，其他配置项没有测试过。
```ini
[clickhouse]
host = 192.168.10.129
port = 9000
database = traffic_flow
user = default
password = data!dhee2341

[mysql]
host = 192.168.10.129
port = 9527
database = jg_hb_allinmap
user = root
password = 123456
charset = utf8mb4

[settings]
# 如果表已存在是否跳过处理 (默认false，即不跳过)
skip_existing_tables = False 
# 0：收集阶段， 运行01 02 两个文件 生成 talbe_dict_csv  column_dict_raw.csv，收集mysql数据库 表信息和字段信息。
# 1：整理阶段，运行 03 04 两个py文件，读取 table_dcit.csv，column_dict.csv，target_table.txt 将表数据映射到clickhouse中
run_mode = 1
```