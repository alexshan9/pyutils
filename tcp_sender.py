import socket
import threading
import time
import random
import string

def generate_random_string():
    """生成100个随机字母数字组合的字符串"""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=1000))

def handle_client(client_socket):
    """处理客户端连接"""
    try:
        while True:
            # 生成并发送数据
            data = generate_random_string()
            try:
                client_socket.send(data.encode('utf-8'))
                print(f"Sent {len(data)} bytes to client")
            except Exception as e:
                print(f"Send error: {e}")
                break
            
            # 等待5秒
            time.sleep(5)
    except Exception as e:
        print(f"Client error: {e}")
    finally:
        client_socket.close()
        print("Client connection closed")

def start_server(host='0.0.0.0', port=12345):
    """启动TCP服务器"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    print(f"Server started on {host}:{port}")

    try:
        while True:
            client_sock, addr = server.accept()
            print(f"New connection from {addr[0]}:{addr[1]}")
            
            # 为每个客户端创建新线程
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_sock,)
            )
            client_thread.start()
    except KeyboardInterrupt:
        print("\nServer is shutting down...")
    finally:
        server.close()

if __name__ == "__main__":
    start_server()