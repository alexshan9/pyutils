import requests
import os
from datetime import datetime
from urllib.parse import urlparse
import ftplib
from urllib.parse import urlparse

def download_wuhan_brdc(date_str, save_dir='downloads'):
    """
    下载武汉大学IGS中心的GNSS广播星历数据
    
    参数:
    date_str - 日期字符串（格式：YYYY-MM-DD）
    save_dir - 本地保存目录（默认：downloads）
    
    返回:
    tuple: (是否成功, 结果信息, 本地文件路径)
    """
    try:
        # 解析日期
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        year = date_obj.year
        doy = date_obj.timetuple().tm_yday  # 年积日
        year_short = str(year)[-2:]  # 年份后两位
        
        # 构建下载URL - 基于提供的模式
        # ftp://igs.gnsswhu.cn/pub/gps/data/daily/2025/brdc/BRDM00DLR_S_20251660000_01D_MN.rnx.gz
        file_name = f"BRDM00DLR_S_{year}{doy:03d}0000_01D_MN.rnx.gz"
        url = f"ftp://igs.gnsswhu.cn/pub/gps/data/daily/{year}/brdc/{file_name}"
        
        # 重命名模式：brdm + doy + year_short + .p.gz
        renamed_file = f"brdm{doy:03d}{year_short}.p.gz"
        
        # 创建保存目录
        os.makedirs(save_dir, exist_ok=True)
        
        # 下载文件路径
        download_path = os.path.join(save_dir, file_name)
        final_path = os.path.join(save_dir, renamed_file)
        
        print(f"正在下载: {url}")
        print(f"目标文件: {renamed_file}")
        
        # 下载文件 (支持FTP协议)
        try:
            if url.startswith('ftp://'):
                # 使用FTP下载
                parsed_url = urlparse(url)
                ftp_host = parsed_url.hostname
                ftp_path = parsed_url.path
                
                print("正在连接FTP服务器...")
                ftp = ftplib.FTP()
                ftp.connect(ftp_host, timeout=120)
                ftp.login()  # 匿名登录
                
                print("开始下载文件...")
                with open(download_path, 'wb') as f:
                    def callback(data):
                        f.write(data)
                        # 简单的进度指示
                        print(".", end='', flush=True)
                    
                    ftp.retrbinary(f'RETR {ftp_path}', callback)
                
                ftp.quit()
                print()  # 换行
                
            else:
                # 使用HTTP/HTTPS下载
                response = requests.get(url, timeout=60, stream=True)
                response.raise_for_status()
                
                # 保存下载的文件
                with open(download_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            # 重命名文件
            if os.path.exists(final_path):
                os.remove(final_path)  # 删除已存在的文件
            os.rename(download_path, final_path)
            
            return True, f"下载成功: {final_path}", final_path
            
        except (requests.exceptions.RequestException, ftplib.Error, Exception) as e:
            return False, f"下载失败: {str(e)}", None
        except Exception as e:
            return False, f"处理文件时出错: {str(e)}", None
            
    except ValueError as e:
        return False, f"日期格式错误，应为YYYY-MM-DD: {str(e)}", None
    except Exception as e:
        return False, f"系统错误: {str(e)}", None

def download_wuhan_brdc_range(start_date, end_date, save_dir='downloads'):
    """
    批量下载指定日期范围的星历数据
    
    参数:
    start_date - 开始日期（格式：YYYY-MM-DD）
    end_date - 结束日期（格式：YYYY-MM-DD）
    save_dir - 保存目录
    
    返回:
    list: 下载结果列表
    """
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        results = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y-%m-%d')
            success, message, file_path = download_wuhan_brdc(date_str, save_dir)
            
            results.append({
                'date': date_str,
                'success': success,
                'message': message,
                'file_path': file_path
            })
            
            # 移动到下一天
            from datetime import timedelta
            current_dt += timedelta(days=1)
        
        return results
        
    except ValueError as e:
        return [{'error': f"日期格式错误: {str(e)}"}]


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='武汉大学IGS中心星历数据下载工具')
    parser.add_argument('date', nargs='?', default=datetime.now().strftime('%Y-%m-%d'),
                       help='下载日期（格式：YYYY-MM-DD，默认：当天）')
    parser.add_argument('-o', '--output', default='downloads',
                       help='保存目录（默认：downloads）')
    parser.add_argument('-r', '--range', nargs=2, metavar=('START', 'END'),
                       help='下载日期范围（格式：YYYY-MM-DD YYYY-MM-DD）')
    
    args = parser.parse_args()
    
    if args.range:
        # 批量下载
        print(f"批量下载 {args.range[0]} 到 {args.range[1]} 的星历数据...")
        results = download_wuhan_brdc_range(args.range[0], args.range[1], args.output)
        
        for result in results:
            if 'error' in result:
                print(f"错误: {result['error']}")
            else:
                status = "✓" if result['success'] else "✗"
                print(f"{status} {result['date']}: {result['message']}")
    else:
        # 单日下载
        success, message, file_path = download_wuhan_brdc(args.date, args.output)
        print(message)
        if success:
            print(f"文件已保存到: {file_path}")