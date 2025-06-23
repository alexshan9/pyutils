实现一个python程序，实现对已有sql的读取，并根据对应的txt 映射关系，写入到 clickhouse db中，附带进度展示。
比如 sql为 t_glz.sql 那么需要查找它对应的 t_glz_xxx.txt 映射关系，并进行映射写入。

你应该提供一个配置文件，用于设置clickhouse的数据库连接方式

映射关系txt说明：
1. 它是一个csv文件，第一列为mysql的属性名，第二列为clickhouse 中的属性名称，第三列为clickhouse中的备注。
2. 映射关系txt的文件名，明明规则为 mysql表名称-clickhouse表名称.txt

