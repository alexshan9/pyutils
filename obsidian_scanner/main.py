import os
import re
import yaml
import requests
from typing import List, Optional, Dict
from dataclasses import dataclass
import hashlib

def generate_file_hash(content: str) -> str:
    """计算内容的唯一hash值"""
    hash_obj = hashlib.sha256(content.encode('utf-8'))
    return hash_obj.hexdigest()


@dataclass
class Note:
    """笔记数据结构类"""
    title: str
    publish: bool
    tags: List[str]
    desc: str
    outlinkNotes: List[str]
    files: List[str]
    path: str
    file_hash: str = ""  # 新增：文件内容hash
    content: str = ""
    
    def to_dict(self):
        """转换为字典格式便于输出"""
        return {
            'title': self.title,
            'publish': self.publish,
            'tags': self.tags,
            'desc': self.desc,
            'outlinkNotes': self.outlinkNotes,
            'files': self.files,
            'path': self.path,
            'file_hash': self.file_hash,
            'content': self.content
        }


class NoteScanner:
    """笔记扫描器类"""
    
    def __init__(self, root_path: str, debug: bool = False):
        """初始化扫描器
        
        Args:
            root_path: 笔记根目录路径
            debug: 是否开启调试模式
        """
        self.root_path = root_path
        self.debug = debug
        
    def log_debug(self, message: str):
        """调试日志输出
        
        Args:
            message: 日志消息
        """
        if self.debug:
            print(f"[DEBUG] {message}")
            
    def parse_metadata(self, content: str) -> Optional[dict]:
        """解析Markdown文件的YAML front matter metadata
        
        Args:
            content: 文件内容
            
        Returns:
            解析后的metadata字典，如果没有metadata返回None
        """
        # 匹配YAML front matter格式：---开头和结尾
        yaml_pattern = r'^---\s*\n(.*?)\n---\s*\n'
        match = re.match(yaml_pattern, content, re.DOTALL)
        
        if not match:
            return None
            
        yaml_content = match.group(1)
        
        try:
            metadata = yaml.safe_load(yaml_content)
            return metadata
        except yaml.YAMLError as e:
            print(f"YAML解析错误: {e}")
            return None
            
    def scan_outlink_notes(self, content: str) -> List[str]:
        """扫描文档中的笔记链接 [[note_name]]
        
        Args:
            content: 文档内容
            
        Returns:
            链接的笔记文件名称列表
        """
        # 匹配 [[...]] 格式的链接，但排除以!开头的附件引用
        # 使用负向后顾断言确保[[前面不是!
        pattern = r'(?<!\!)\[\[([^\]]+)\]\]'
        matches = re.findall(pattern, content)
        
        # 清理链接名称，处理可能的别名语法 [[file|alias]]
        outlink_notes = []
        for match in matches:
            # 如果包含|符号，取|前面的部分作为文件名
            note_name = match.split('|')[0].strip()
            if note_name and note_name not in outlink_notes:
                outlink_notes.append(note_name)
                
        return outlink_notes
         
    def scan_files(self, content: str) -> List[str]:
        """扫描文档中的附件引用 ![[file_name]]
        
        Args:
            content: 文档内容
            
        Returns:
            引用的附件文件名称列表
        """
        # 匹配 ![[...]] 格式的附件引用
        pattern = r'!\[\[([^\]]+)\]\]'
        matches = re.findall(pattern, content)
        
        # 清理附件名称
        files = []
        for match in matches:
            file_name = match.strip()
            if file_name and file_name not in files:
                files.append(file_name)
                
        return files
        
    def scan_directory(self) -> List[str]:
        """递归扫描目录，获取所有.md文件的路径
        
        Returns:
            所有markdown文件的路径列表
        """
        md_files = []
        
        for root, dirs, files in os.walk(self.root_path):
            for file in files:
                if file.endswith('.md'):
                    file_path = os.path.join(root, file)
                    md_files.append(file_path)
                    
        return md_files
        
    def get_relative_path(self, file_path: str) -> str:
        """获取文件相对于根目录的路径
        
        Args:
            file_path: 文件绝对路径
            
        Returns:
            相对路径
        """
        return os.path.relpath(file_path, self.root_path)
        
    def extract_description(self, content: str, metadata: dict) -> str:
        """从内容中提取文章描述
        
        Args:
            content: 文档内容
            metadata: 元数据
            
        Returns:
            文章描述
        """
        # 移除YAML front matter
        yaml_pattern = r'^---\s*\n.*?\n---\s*\n'
        content_without_yaml = re.sub(yaml_pattern, '', content, flags=re.DOTALL)
        
        # 提取第一段作为描述，移除markdown格式
        lines = content_without_yaml.strip().split('\n')
        description_lines = []
        
        for line in lines:
            line = line.strip()
            # 跳过空行、标题、标签等
            if line and not line.startswith('#') and not line.startswith('![[') and not line.startswith('[['):
                # 移除markdown格式符号
                clean_line = re.sub(r'[*_`]', '', line)
                description_lines.append(clean_line)
                # 取前3行或遇到代码块时停止
                if len(description_lines) >= 3 or line.startswith('```'):
                    break
                    
        return ' '.join(description_lines)[:200] + ('...' if len(' '.join(description_lines)) > 200 else '')
        
    def parse_file(self, file_path: str) -> Optional[Note]:
        """解析单个markdown文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            Note对象，如果文件没有metadata则返回None
        """
        self.log_debug(f"正在解析文件: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"读取文件 {file_path} 时出错: {e}")
            return None
            
        # 解析metadata
        metadata = self.parse_metadata(content)
        if not metadata:
            self.log_debug(f"文件 {file_path} 没有metadata，跳过")
            return None
            
        self.log_debug(f"找到metadata: {metadata}")
        
        # 提取各项信息
        title = metadata.get('title', os.path.splitext(os.path.basename(file_path))[0])
        publish = metadata.get('dg-publish', False)
        tags = metadata.get('tags', [])
        
        # 确保tags是列表格式
        if isinstance(tags, str):
            tags = [tags]
        elif not isinstance(tags, list):
            tags = []
            
        desc = self.extract_description(content, metadata)
        outlink_notes = self.scan_outlink_notes(content)
        files = self.scan_files(content)
        relative_path = self.get_relative_path(file_path)
        
        self.log_debug(f"提取的链接笔记: {outlink_notes}")
        self.log_debug(f"提取的附件文件: {files}")
        
        # 生成文件hash和存储完整内容
        file_hash = generate_file_hash(content)
        
        self.log_debug(f"生成文件hash: {file_hash}")
        
        return Note(
            title=title,
            publish=publish,
            tags=tags,
            desc=desc,
            outlinkNotes=outlink_notes,
            files=files,
            path=relative_path,
            content=content,
            file_hash=file_hash
        )
        
    def scan_all_notes(self) -> List[Note]:
        """扫描所有笔记文件
        
        Returns:
            所有包含metadata的Note对象列表
        """
        notes = []
        md_files = self.scan_directory()
        
        print(f"找到 {len(md_files)} 个markdown文件")
        
        for file_path in md_files:
            note = self.parse_file(file_path)
            if note:
                if note.publish:
                    notes.append(note)
                    print(f"已解析(已发布): {note.path}")
                else:
                    print(f"跳过 (未发布): {self.get_relative_path(file_path)}")
            else:
                print(f"跳过 (无metadata): {self.get_relative_path(file_path)}")
                
        print(f"成功解析 {len(notes)} 个已发布的笔记")
        return notes
        
    def validate_path(self) -> bool:
        """验证根目录路径是否有效
        
        Returns:
            路径是否有效
        """
        if not os.path.exists(self.root_path):
            print(f"错误: 路径 {self.root_path} 不存在")
            return False
            
        if not os.path.isdir(self.root_path):
            print(f"错误: {self.root_path} 不是目录")
            return False
            
        return True 


class DiffManager:
    """差分管理器类，负责与服务器通信和差分逻辑处理"""
    
    def __init__(self, server_url: str, debug: bool = False):
        """初始化差分管理器
        
        Args:
            server_url: 服务器API地址
            debug: 是否开启调试模式
        """
        self.server_url = server_url
        self.debug = debug
        
    def log_debug(self, message: str):
        """调试日志输出
        
        Args:
            message: 日志消息
        """
        if self.debug:
            print(f"[DIFF DEBUG] {message}")
    
    def send_hashes_to_server(self, notes: List[Note]) -> Optional[List[str]]:
        """发送所有文件的hash到服务器，获取需要更新的hash列表
        
        Args:
            notes: 所有笔记对象列表
            
        Returns:
            服务器返回的需要更新的hash列表，网络异常时返回None
        """
        if not self.server_url:
            self.log_debug("服务器URL未配置，跳过服务器通信")
            return None
            
        # # 构建hash列表
        # hash_list = [note.file_hash for note in notes]
        # self.log_debug(f"准备发送 {len(hash_list)} 个hash到服务器")
        
        try:
            # 构建请求数据
            payload = {
                "notes": notes,
                "timestamp": hash(str(notes))  # 简单的请求标识
            }
            
            self.log_debug(f"发送请求到: {self.server_url}")
            self.log_debug(f"请求数据: {len(payload['notes'])} 个笔记")
            
            # 发送POST请求
            response = requests.post(
                self.server_url,
                json=payload,
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )
            
            response.raise_for_status()
            result = response.json()
            
            # 解析服务器响应
            if isinstance(result, dict) and 'hashes' in result:
                updated_hashes = result['hashes']
            elif isinstance(result, list):
                updated_hashes = result
            else:
                print(f"服务器响应格式异常: {result}")
                return None
                
            self.log_debug(f"服务器返回 {len(updated_hashes)} 个需要更新的hash")
            return updated_hashes
            
        except requests.exceptions.RequestException as e:
            print(f"服务器通信异常: {e}")
            self.log_debug(f"网络错误详情: {e}")
            return None
        except Exception as e:
            print(f"处理服务器响应时出错: {e}")
            self.log_debug(f"响应处理错误详情: {e}")
            return None
    
    def filter_notes_by_server_response(self, notes: List[Note], updated_hashes: Optional[List[str]]) -> List[Note]:
        """根据服务器响应过滤需要更新的笔记
        
        Args:
            notes: 所有笔记对象列表
            updated_hashes: 服务器返回的需要更新的hash列表
            
        Returns:
            过滤后的笔记列表
        """
        if updated_hashes is None:
            # 服务器通信失败，返回所有笔记（降级处理）
            self.log_debug("服务器通信失败，返回所有笔记作为降级处理")
            return notes
            
        if not updated_hashes:
            # 服务器返回空列表，表示没有需要更新的文件
            self.log_debug("服务器返回空列表，没有需要更新的文件")
            return []
            
        # 过滤出需要更新的笔记
        updated_hashes_set = set(updated_hashes)
        filtered_notes = [note for note in notes if note.file_hash in updated_hashes_set]
        
        self.log_debug(f"过滤结果: {len(filtered_notes)}/{len(notes)} 个笔记需要更新")
        
        return filtered_notes


def main():
    """主程序入口"""
    import json
    import sys
    
    # 检查命令行参数
    debug_mode = '--debug' in sys.argv or '-d' in sys.argv
    
    # 服务器URL配置 - 可以通过命令行参数或环境变量配置
    server_url = ""
    for i, arg in enumerate(sys.argv):
        if arg == '--server-url' and i + 1 < len(sys.argv):
            server_url = sys.argv[i + 1]
            break
    
    # 如果没有通过命令行指定，尝试从环境变量获取
    if not server_url:
        server_url = os.environ.get('NOTES_DIFF_SERVER_URL', '')
    
    # 使用当前目录作为默认扫描路径
    # 用户可以修改这个路径为自己的笔记目录
    notes_path = r"C:\Users\shancw\Desktop\project\notes"
    
    # 创建扫描器实例
    scanner = NoteScanner(notes_path, debug=debug_mode)
    
    # 创建差分管理器实例
    diff_manager = DiffManager(server_url, debug=debug_mode)
    
    if debug_mode:
        print("调试模式已开启")
        print(f"服务器URL: {server_url if server_url else '未配置'}")
    
    # 验证路径
    if not scanner.validate_path():
        return
    
    print(f"开始扫描笔记目录: {notes_path}")
    print("-" * 50)
    
    # 扫描所有笔记
    notes = scanner.scan_all_notes()
    
    print("-" * 50)
    print(f"扫描完成，共找到 {len(notes)} 个有效笔记")
    
    # 差分处理：与服务器通信获取需要更新的文件列表
    if server_url:
        print("\n=== 开始差分处理 ===")
        updated_hashes = diff_manager.send_hashes_to_server(notes)
        filtered_notes = diff_manager.filter_notes_by_server_response(notes, updated_hashes)
        
        print(f"差分结果: {len(filtered_notes)}/{len(notes)} 个笔记需要更新")
        notes_to_save = filtered_notes
    else:
        print("\n=== 跳过差分处理 ===")
        print("未配置服务器URL，将保存所有笔记")
        notes_to_save = notes
    
    # 输出结果示例
    if notes_to_save:
        print("\n=== 将要保存的笔记示例 ===")
        for i, note in enumerate(notes_to_save[:3]):  # 只显示前3个作为示例
            print(f"\n第{i+1}个笔记:")
            print(f"  标题: {note.title}")
            print(f"  发布: {note.publish}")
            print(f"  标签: {note.tags}")
            print(f"  描述: {note.desc}")
            print(f"  链接笔记: {note.outlinkNotes}")
            print(f"  附件文件: {note.files}")
            print(f"  路径: {note.path}")
            print(f"  文件Hash: {note.file_hash[:16]}...")
            
        # 保存为JSON文件
        output_file = "notes_scan_result.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([note.to_dict() for note in notes_to_save], f, ensure_ascii=False, indent=2)
        
        print(f"\n完整结果已保存到: {output_file}")
        print(f"保存了 {len(notes_to_save)} 个笔记")
    else:
        print("\n=== 没有需要更新的笔记 ===")
        print("所有文件都是最新的，无需生成JSON文件")
        
    print(f"\n使用方法:")
    print(f"  普通模式: python main.py")
    print(f"  调试模式: python main.py --debug")
    print(f"  指定服务器: python main.py --server-url http://your-server.com/api/diff")
    print(f"  环境变量: set NOTES_DIFF_SERVER_URL=http://your-server.com/api/diff")


if __name__ == "__main__":
    main() 