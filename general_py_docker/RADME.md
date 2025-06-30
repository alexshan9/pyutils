通用py 容器构建，保留扩展性

```bash
# 构建镜像
docker build -t general_py_docker .
# 运行容器
docker compose up -d
# 打包
docker save -o general_py_docker.tar general_py_docker
# 加载
docker load -i general_py_docker.tar
```