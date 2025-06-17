# 使用Python 3.11 slim镜像作为基础镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
# 关闭输出缓冲，实时输出日志
ENV PYTHONUNBUFFERED=1
# 不生成__pycache__，docker内无用
ENV PYTHONDONTWRITEBYTECODE=1

# 复制requirements.txt并安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用程序文件
COPY main.py .
COPY wuhan_brdc.py .
COPY config.ini .

# 创建必要的目录
RUN mkdir -p brdc data downloads

# 创建非root用户
RUN groupadd -r appuser && useradd -r -g appuser appuser

# 设置目录权限
RUN chown -R appuser:appuser /app

# 切换到非root用户
USER appuser

# 暴露端口（如果应用需要，当前应用主要是后台任务，可能不需要）
# EXPOSE 8000

# 设置默认执行命令
CMD ["python", "main.py"]
