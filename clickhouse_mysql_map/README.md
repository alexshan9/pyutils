流程：
1. generate_column_dict.py
2. generate_table_dict_csv.py
3. 人工补充对应的字段和表名 column_dict.csv table_dict.csv
4. 运行 column_rename.py -> mysql_column_mapper/xxx.csv
5. 运行 table_mapper.py -> clickhouse_mapper/xxx.csv
6. 复制所有clickhouse_mapper 下文件，运行 main.py 进行数据映射插入