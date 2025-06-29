#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
视频帧提取工具主程序
从视频中按指定间隔提取帧并保存为图像文件
"""

import sys
import logging
from config_manager import ConfigManager
from video_extractor import VideoFrameExtractor


def main():
    """主函数"""
    print("视频帧提取工具")
    print("=" * 50)
    
    try:
        # 加载配置
        config_manager = ConfigManager('config.ini')
        config = config_manager.load_config()
        
        # 显示配置信息
        print("配置信息:")
        print(f"  视频文件: {config['video_path']}")
        print(f"  提取间隔: 每 {config['interval']} 帧")
        print(f"  图像尺寸: {config['img_size']}x{config['img_size']}")
        print(f"  输出目录: {config['output_dir']}")
        print(f"  图像格式: {config['image_format']}")
        print("-" * 50)
        
        # 创建视频帧提取器
        extractor = VideoFrameExtractor(config)
        
        # 验证视频文件
        print("验证视频文件...")
        if not extractor.validate_video():
            print("❌ 视频文件验证失败")
            return 1
        print("✅ 视频文件验证成功")
        
        # 执行帧提取
        print("\n开始提取帧...")
        if extractor.extract_frames():
            print("✅ 帧提取完成!")
            print(f"图像文件已保存到: {config['output_dir']}")
            return 0
        else:
            print("❌ 帧提取失败")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断操作")
        return 1
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {e}")
        print(f"❌ 执行失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 