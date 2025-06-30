import configparser
import os
import logging
from typing import Dict, Any


class ConfigManager:
    """配置文件管理器"""
    
    def __init__(self, config_path: str = 'config.ini'):
        """
        初始化配置管理器
        
        Args:
            config_path (str): 配置文件路径
        """
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self._default_config = self._get_default_config()
    
    def load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            Dict[str, Any]: 配置字典
        """
        if not os.path.exists(self.config_path):
            logging.warning(f"配置文件 {self.config_path} 不存在，使用默认配置")
            return self._default_config
        
        try:
            self.config.read(self.config_path, encoding='utf-8')
            self._validate_config()
            return self._get_config_dict()
        except Exception as e:
            logging.error(f"读取配置文件失败: {e}，使用默认配置")
            return self._default_config
    
    def _validate_config(self):
        """验证配置文件完整性"""
        required_sections = ['video', 'output', 'processing']
        for section in required_sections:
            if not self.config.has_section(section):
                raise ValueError(f"配置文件缺少必要的section: {section}")
    
    def _get_config_dict(self) -> Dict[str, Any]:
        """获取配置字典"""
        return {
            # 视频配置
            'video_path': self.config.get('video', 'video_path', fallback='input_video.mp4'),
            'interval': self.config.getint('video', 'interval', fallback=10),
            'img_size': self.config.getint('video', 'img_size', fallback=640),
            
            # 输出配置
            'output_dir': self.config.get('output', 'output_dir', fallback='imgs'),
            'image_format': self.config.get('output', 'image_format', fallback='jpg'),
            'image_quality': self.config.getint('output', 'image_quality', fallback=95),
            
            # 处理配置
            'show_progress': self.config.getboolean('processing', 'show_progress', fallback=True),
            'clear_output_dir': self.config.getboolean('processing', 'clear_output_dir', fallback=False),
            'log_level': self.config.get('processing', 'log_level', fallback='INFO')
        }
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'video_path': 'input_video.mp4',
            'interval': 10,
            'img_size': 640,
            'output_dir': 'imgs',
            'image_format': 'jpg',
            'image_quality': 95,
            'show_progress': True,
            'clear_output_dir': False,
            'log_level': 'INFO'
        } 