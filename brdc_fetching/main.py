#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BRDC数据自动下载主程序
基于wuhan_brdc.py实现的增强版，支持配置文件、定时任务和历史数据检查
"""

import os
import sys
import time
import signal
import logging
import threading
import configparser
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional

import schedule
import pytz

# 导入现有的下载函数
from wuhan_brdc import download_wuhan_brdc


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: str = 'config.ini'):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.load_config()
    
    def load_config(self) -> Dict:
        """加载配置文件"""
        try:
            if not os.path.exists(self.config_path):
                self._create_default_config()
            
            self.config.read(self.config_path, encoding='utf-8')
            
            # 验证必要的配置项
            self._validate_config()
            
            return self._get_config_dict()
            
        except Exception as e:
            logging.error(f"加载配置文件失败: {e}")
            return self._get_default_config()
    
    def _create_default_config(self):
        """创建默认配置文件"""
        default_config = """[download]
download_dir = brdc

[schedule]
daily_minute = 5

[history]
enable_history_check = true
history_check_months = 3

[logging]
log_level = INFO
log_file = brdc_downloader.log
"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            f.write(default_config)
    
    def _validate_config(self):
        """验证配置文件完整性"""
        required_sections = ['download', 'schedule', 'history', 'logging']
        for section in required_sections:
            if not self.config.has_section(section):
                raise ValueError(f"配置文件缺少必要的section: {section}")
    
    def _get_config_dict(self) -> Dict:
        """获取配置字典"""
        return {
            'download_dir': self.config.get('download', 'download_dir', fallback='brdc'),
            'daily_minute': self.config.getint('schedule', 'daily_minute', fallback=5),
            'enable_history_check': self.config.getboolean('history', 'enable_history_check', fallback=True),
            'history_check_months': self.config.getint('history', 'history_check_months', fallback=3),
            'log_level': self.config.get('logging', 'log_level', fallback='INFO'),
            'log_file': self.config.get('logging', 'log_file', fallback='brdc_downloader.log')
        }
    
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'download_dir': 'brdc',
            'daily_minute': 5,
            'enable_history_check': True,
            'history_check_months': 3,
            'log_level': 'INFO',
            'log_file': 'brdc_downloader.log'
        }


class DownloadManager:
    """下载管理器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.download_dir = config['download_dir']
        
        # 确保下载目录存在
        os.makedirs(self.download_dir, exist_ok=True)
    
    def download_daily_data(self, date_str: Optional[str] = None) -> Tuple[bool, str, Optional[str]]:
        """下载指定日期的数据，默认为UTC前一天"""
        if date_str is None:
            # 获取UTC前一天的日期
            utc_yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            date_str = utc_yesterday.strftime('%Y-%m-%d')
        
        logging.info(f"开始下载 {date_str} 的BRDC数据")
        
        try:
            success, message, file_path = download_wuhan_brdc(date_str, self.download_dir)
            
            if success:
                logging.info(f"数据下载成功: {message}")
            else:
                logging.error(f"数据下载失败: {message}")
            
            return success, message, file_path
            
        except Exception as e:
            error_msg = f"下载过程中发生异常: {str(e)}"
            logging.error(error_msg)
            return False, error_msg, None
    
    def is_data_exists(self, date_str: str) -> bool:
        """检查指定日期的数据是否已存在"""
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            year = date_obj.year
            doy = date_obj.timetuple().tm_yday
            year_short = str(year)[-2:]
            
            # 检查重命名后的文件格式（与wuhan_brdc.py保持一致）
            filename = f"brdm{doy:03d}0.{year_short}p.gz"
            file_path = os.path.join(self.download_dir, filename)
            
            return os.path.exists(file_path)
            
        except Exception as e:
            logging.error(f"检查文件存在性时出错: {e}")
            return False


class ScheduleManager:
    """调度管理器"""
    
    def __init__(self, config: Dict, download_manager: DownloadManager):
        self.config = config
        self.download_manager = download_manager
        self.running = False
    
    def setup_scheduled_download(self):
        """设置定时下载任务"""
        daily_minute = self.config['daily_minute']
        
        # 清除现有的任务
        schedule.clear()
        
        # 设置每天UTC 0点指定分钟执行
        utc_timezone = pytz.UTC
        schedule.every().day.at(f"00:{daily_minute:02d}", utc_timezone).do(self._scheduled_download_job)
        
        logging.info(f"定时任务已设置：每天UTC时间 00:{daily_minute:02d} 执行下载（下载前一天数据）")
    
    def _scheduled_download_job(self):
        """定时下载任务 - 下载前一天的数据"""
        utc_now = datetime.now(timezone.utc)
        logging.info(f"执行定时下载任务 (UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')})")
        
        try:
            # 下载前一天的数据（默认行为）
            success, message, file_path = self.download_manager.download_daily_data()
            
            if success:
                logging.info(f"定时下载成功: {file_path}")
            else:
                logging.warning(f"定时下载失败: {message}")
                
        except Exception as e:
            logging.error(f"定时下载任务执行异常: {e}")
    
    def start(self):
        """启动调度器"""
        self.running = True
        logging.info("调度器已启动")
        
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    
    def stop(self):
        """停止调度器"""
        self.running = False
        logging.info("调度器已停止")


class HistoryManager:
    """历史数据管理器"""
    
    def __init__(self, config: Dict, download_manager: DownloadManager):
        self.config = config
        self.download_manager = download_manager
        self.running = False
    
    def start_history_check(self):
        """启动历史数据检查线程"""
        if not self.config['enable_history_check']:
            logging.info("历史数据检查功能已禁用")
            return
        
        self.running = True
        history_thread = threading.Thread(target=self._history_check_worker, daemon=True)
        history_thread.start()
        logging.info("历史数据检查线程已启动")
    
    def _history_check_worker(self):
        """历史数据检查工作线程"""
        # 等待一段时间再开始，避免启动时的冲突
        time.sleep(300)  # 等待5分钟
        
        while self.running:
            try:
                self._check_and_download_history()
                
                # 每天检查一次历史数据
                time.sleep(24 * 60 * 60)
                
            except Exception as e:
                logging.error(f"历史数据检查异常: {e}")
                time.sleep(60 * 60)  # 出错后1小时后重试
    
    def _check_and_download_history(self):
        """检查并下载缺失的历史数据"""
        utc_now = datetime.now(timezone.utc)
        logging.info(f"开始检查历史数据 (UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')})")
        
        months = self.config['history_check_months']
        current_date = datetime.now(timezone.utc)
        
        # 检查过去几个月的数据
        for i in range(months * 30):  # 简化为按天计算
            check_date = current_date - timedelta(days=i+1)
            date_str = check_date.strftime('%Y-%m-%d')
            
            if not self.download_manager.is_data_exists(date_str):
                logging.info(f"发现缺失数据: {date_str}，开始下载")
                
                success, message, file_path = self.download_manager.download_daily_data(date_str)
                
                if success:
                    logging.info(f"历史数据下载成功: {date_str}")
                else:
                    logging.warning(f"历史数据下载失败: {date_str} - {message}")
                
                # 避免频繁下载，每次下载后暂停
                time.sleep(10)
    
    def stop(self):
        """停止历史数据检查"""
        self.running = False
        logging.info("历史数据检查已停止")


class BRDCDownloader:
    """BRDC数据下载主控制器"""
    
    def __init__(self, config_path: str = 'config.ini'):
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.load_config()
        
        # 配置日志
        self._setup_logging()
        
        # 初始化各管理器
        self.download_manager = DownloadManager(self.config)
        self.schedule_manager = ScheduleManager(self.config, self.download_manager)
        self.history_manager = HistoryManager(self.config, self.download_manager)
        
        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logging(self):
        """设置日志配置"""
        log_level = getattr(logging, self.config['log_level'].upper(), logging.INFO)
        log_file = self.config['log_file']
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def _signal_handler(self, signum, frame):
        """信号处理器，优雅退出"""
        logging.info(f"接收到信号 {signum}，正在优雅退出...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """启动BRDC下载服务"""
        utc_now = datetime.now(timezone.utc)
        logging.info(f"BRDC数据下载服务启动 (UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')})")
        logging.info(f"配置信息: {self.config}")
        
        # 启动历史数据检查
        self.history_manager.start_history_check()
        
        # 设置并启动定时任务
        self.schedule_manager.setup_scheduled_download()
        
        # 立即执行一次下载（前一天数据）
        logging.info("执行初始下载（前一天数据）...")
        self.download_manager.download_daily_data()
        
        # 启动调度器主循环
        try:
            self.schedule_manager.start()
        except KeyboardInterrupt:
            logging.info("接收到中断信号")
            self.stop()
    
    def stop(self):
        """停止服务"""
        logging.info("正在停止BRDC数据下载服务...")
        self.schedule_manager.stop()
        self.history_manager.stop()
        logging.info("服务已停止")


def main():
    """主函数"""
    print("BRDC数据自动下载器")
    print("="*50)
    
    try:
        # 检查配置文件
        config_path = 'config.ini'
        if not os.path.exists(config_path):
            print(f"配置文件 {config_path} 不存在，将创建默认配置...")
        
        # 创建并启动下载器
        downloader = BRDCDownloader(config_path)
        downloader.start()
        
    except Exception as e:
        print(f"程序启动失败: {e}")
        logging.error(f"程序启动失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 