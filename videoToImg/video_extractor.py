import cv2
import os
import shutil
import logging
from typing import Dict, Any, Tuple, Optional
from pathlib import Path


class VideoFrameExtractor:
    """视频帧提取器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化视频帧提取器
        
        Args:
            config (Dict[str, Any]): 配置字典
        """
        self.config = config
        self.video_path = config['video_path']
        self.interval = config['interval']
        self.img_size = config['img_size']
        self.output_dir = config['output_dir']
        self.image_format = config['image_format'].lower()
        self.image_quality = config['image_quality']
        self.show_progress = config['show_progress']
        self.clear_output_dir = config['clear_output_dir']
        
        # 视频信息
        self.total_frames = 0
        self.fps = 0
        self.duration = 0
        self.frame_width = 0
        self.frame_height = 0
        
        # 设置日志
        self._setup_logging()
    
    def _setup_logging(self):
        """设置日志配置"""
        log_level = getattr(logging, self.config['log_level'].upper(), logging.INFO)
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
    
    def validate_video(self) -> bool:
        """
        验证视频文件并获取基本信息
        
        Returns:
            bool: 验证是否成功
        """
        if not os.path.exists(self.video_path):
            logging.error(f"视频文件不存在: {self.video_path}")
            return False
        
        if not os.path.isfile(self.video_path):
            logging.error(f"指定路径不是文件: {self.video_path}")
            return False
        
        # 尝试打开视频文件
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            logging.error(f"无法打开视频文件: {self.video_path}")
            return False
        
        try:
            # 获取视频基本信息
            self.total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            self.fps = cap.get(cv2.CAP_PROP_FPS)
            self.frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            self.frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            self.duration = self.total_frames / self.fps if self.fps > 0 else 0
            
            # 验证视频基本信息
            if self.total_frames <= 0:
                logging.error("视频文件损坏：无法获取帧数")
                return False
            
            if self.fps <= 0:
                logging.error("视频文件损坏：无法获取帧率")
                return False
            
            if self.frame_width <= 0 or self.frame_height <= 0:
                logging.error("视频文件损坏：无法获取分辨率")
                return False
            
            # 验证裁剪尺寸合理性
            min_dimension = min(self.frame_width, self.frame_height)
            if self.img_size > min_dimension:
                logging.warning(f"裁剪尺寸 ({self.img_size}) 大于视频最小维度 ({min_dimension})，将自动调整")
                self.img_size = min_dimension
            
            logging.info(f"视频信息: {self.frame_width}x{self.frame_height}, {self.total_frames}帧, {self.fps:.2f}fps, {self.duration:.2f}秒")
            
            # 计算预期提取的帧数
            expected_frames = (self.total_frames - 1) // self.interval + 1
            logging.info(f"预计提取 {expected_frames} 帧图像 (间隔: {self.interval}帧)")
            
            return True
            
        except Exception as e:
            logging.error(f"验证视频信息时出错: {e}")
            return False
        finally:
            cap.release()
    
    def _prepare_output_directory(self) -> bool:
        """
        准备输出目录
        
        Returns:
            bool: 准备是否成功
        """
        try:
            output_path = Path(self.output_dir)
            
            # 如果目录存在且需要清空
            if output_path.exists() and self.clear_output_dir:
                logging.info(f"清空输出目录: {self.output_dir}")
                shutil.rmtree(output_path)
            
            # 创建输出目录
            output_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"输出目录已准备: {self.output_dir}")
            
            return True
            
        except Exception as e:
            logging.error(f"准备输出目录失败: {e}")
            return False
    
    def _crop_center_region(self, frame) -> Optional[Any]:
        """
        从帧中裁剪中央区域
        
        Args:
            frame: 输入帧
            
        Returns:
            Optional[any]: 裁剪后的图像，失败时返回None
        """
        try:
            height, width = frame.shape[:2]
            
            # 计算中央裁剪区域
            center_x, center_y = width // 2, height // 2
            half_size = self.img_size // 2
            
            # 计算裁剪边界
            x1 = max(0, center_x - half_size)
            y1 = max(0, center_y - half_size)
            x2 = min(width, center_x + half_size)
            y2 = min(height, center_y + half_size)
            
            # 确保裁剪区域是正方形
            actual_width = x2 - x1
            actual_height = y2 - y1
            crop_size = min(actual_width, actual_height)
            
            # 重新计算居中的正方形区域
            center_x_new = x1 + actual_width // 2
            center_y_new = y1 + actual_height // 2
            half_crop = crop_size // 2
            
            x1 = center_x_new - half_crop
            y1 = center_y_new - half_crop
            x2 = x1 + crop_size
            y2 = y1 + crop_size
            
            # 裁剪图像
            cropped = frame[y1:y2, x1:x2]
            
            # 如果裁剪尺寸不等于目标尺寸，进行缩放
            if crop_size != self.img_size:
                cropped = cv2.resize(cropped, (self.img_size, self.img_size), interpolation=cv2.INTER_AREA)
            
            return cropped
            
        except Exception as e:
            logging.error(f"裁剪中央区域失败: {e}")
            return None
    
    def _save_frame(self, frame, frame_number: int) -> bool:
        """
        保存帧到文件
        
        Args:
            frame: 要保存的帧
            frame_number (int): 帧号
            
        Returns:
            bool: 保存是否成功
        """
        try:
            # 生成文件名
            filename = f"frame_{frame_number:06d}.{self.image_format}"
            filepath = os.path.join(self.output_dir, filename)
            
            # 设置保存参数
            save_params = []
            if self.image_format == 'jpg':
                save_params = [cv2.IMWRITE_JPEG_QUALITY, self.image_quality]
            elif self.image_format == 'png':
                save_params = [cv2.IMWRITE_PNG_COMPRESSION, 9]
            
            # 保存图像
            success = cv2.imwrite(filepath, frame, save_params)
            
            if not success:
                logging.error(f"保存图像失败: {filepath}")
                return False
            
            logging.debug(f"成功保存: {filename}")
            return True
            
        except Exception as e:
            logging.error(f"保存帧时出错: {e}")
            return False
    
    def extract_frames(self) -> bool:
        """
        执行帧提取主流程
        
        Returns:
            bool: 提取是否成功
        """
        logging.info("开始提取视频帧...")
        
        # 准备输出目录
        if not self._prepare_output_directory():
            return False
        
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            logging.error("无法打开视频文件")
            return False
        
        try:
            extracted_count = 0
            failed_count = 0
            
            # 计算需要提取的帧位置
            frame_positions = list(range(0, self.total_frames, self.interval))
            total_to_extract = len(frame_positions)
            
            logging.info(f"开始提取 {total_to_extract} 帧...")
            
            for i, frame_pos in enumerate(frame_positions):
                # 设置视频位置到指定帧
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                
                # 读取帧
                ret, frame = cap.read()
                if not ret:
                    logging.warning(f"无法读取第 {frame_pos} 帧")
                    failed_count += 1
                    continue
                
                # 裁剪中央区域
                cropped_frame = self._crop_center_region(frame)
                if cropped_frame is None:
                    logging.warning(f"裁剪第 {frame_pos} 帧失败")
                    failed_count += 1
                    continue
                
                # 保存帧
                if self._save_frame(cropped_frame, frame_pos):
                    extracted_count += 1
                else:
                    failed_count += 1
                
                # 显示进度
                if self.show_progress and (i + 1) % max(1, total_to_extract // 20) == 0:
                    progress = (i + 1) / total_to_extract * 100
                    logging.info(f"进度: {progress:.1f}% ({i + 1}/{total_to_extract})")
            
            # 输出结果统计
            logging.info(f"提取完成: 成功 {extracted_count} 帧, 失败 {failed_count} 帧")
            
            if extracted_count == 0:
                logging.error("没有成功提取任何帧")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"提取过程中发生错误: {e}")
            return False
        finally:
            cap.release() 