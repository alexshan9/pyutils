import csv
import os
from pathlib import Path

def scan_mysql_structure_files():
    """
    扫描mysql_scan文件夹下的所有CSV文件，提取字段信息
    生成包含raw_column, remark, new_column三列的dict.csv文件
    """
    
    # 获取mysql_scan文件夹路径
    mysql_scan_dir = Path("mysql_scan")
    
    if not mysql_scan_dir.exists():
        print(f"错误：找不到mysql_scan文件夹")
        return
    
    # 获取所有CSV文件
    csv_files = list(mysql_scan_dir.glob("*.csv"))
    
    if not csv_files:
        print("错误：mysql_scan文件夹中没有找到CSV文件")
        return
    
    print(f"找到 {len(csv_files)} 个CSV文件")
    
    # 存储所有字段信息
    all_columns = []
    processed_files = 0
    
    # 遍历所有CSV文件
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                # 检查是否包含必要的列
                if 'columnName' in reader.fieldnames and 'columnRemark' in reader.fieldnames:
                    # 提取字段名和备注
                    for row in reader:
                        column_name = row['columnName'].strip()
                        column_remark = row['columnRemark'].strip() if row['columnRemark'] else ''
                        
                        if column_name:  # 只处理非空的字段名
                            all_columns.append({
                                'raw_column': column_name,
                                'remark': column_remark,
                                'new_column': ''  # 根据要求留空
                            })
                    
                    processed_files += 1
                    print(f"已处理：{csv_file.name}")
                else:
                    print(f"警告：{csv_file.name} 不包含预期的列结构")
                    
        except Exception as e:
            print(f"错误：处理文件 {csv_file.name} 时出现异常：{e}")
    
    if not all_columns:
        print("错误：没有提取到任何字段信息")
        return
    
    print(f"成功处理了 {processed_files} 个文件")
    
    # 去除重复的字段（基于raw_column）
    original_count = len(all_columns)
    unique_columns = {}
    
    for col in all_columns:
        column_name = col['raw_column']
        if column_name not in unique_columns:
            unique_columns[column_name] = col
    
    # 转换回列表并排序
    result_columns = list(unique_columns.values())
    result_columns.sort(key=lambda x: x['raw_column'])
    
    duplicate_count = original_count - len(result_columns)
    
    if duplicate_count > 0:
        print(f"去除了 {duplicate_count} 个重复字段")
    
    # 输出到dict.csv
    output_file = "dict.csv"
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            fieldnames = ['raw_column', 'remark', 'new_column']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            
            # 写入头部
            writer.writeheader()
            
            # 写入数据
            writer.writerows(result_columns)
        
        print(f"成功生成 {output_file}")
        print(f"总共包含 {len(result_columns)} 个唯一字段")
        
        # 显示前几行作为预览
        print("\n前5行预览：")
        print("raw_column,remark,new_column")
        for i, col in enumerate(result_columns[:5]):
            print(f"{col['raw_column']},{col['remark']},{col['new_column']}")
        
        if len(result_columns) > 5:
            print("...")
            
    except Exception as e:
        print(f"错误：写入输出文件时出现异常：{e}")

if __name__ == "__main__":
    print("开始扫描mysql_scan文件夹...")
    scan_mysql_structure_files()
    print("扫描完成！") 