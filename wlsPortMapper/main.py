import os
import re
import sys
import ctypes
import argparse
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class MapperConfig:
    ports: List[int]
    start_with_system: bool = False
    listen_address: str = "0.0.0.0"
    wsl_distro: Optional[str] = None
    startup_method: str = "task"  # task | registry


def read_config(config_path: str) -> MapperConfig:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"未找到配置文件: {config_path}")

    ports: List[int] = []
    start_with_system = False
    listen_address = "0.0.0.0"
    wsl_distro: Optional[str] = None
    startup_method = "task"

    with open(config_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            if line.startswith("[") and line.endswith("]"):
                # 兼容 ini section 行，忽略
                continue
            if "=" not in line:
                continue
            key, val = [p.strip() for p in line.split("=", 1)]
            key_upper = key.upper()

            if key_upper == "PORTLIST":
                for p in re.split(r"[,\s]+", val):
                    if not p:
                        continue
                    try:
                        ports.append(int(p))
                    except ValueError:
                        raise ValueError(f"无效端口: {p}")
            elif key_upper == "START_WITH_SYSTEM":
                start_with_system = str(val).strip().lower() in {"1", "true", "yes", "y"}
            elif key_upper == "LISTEN_ADDRESS":
                listen_address = val
            elif key_upper == "WSL_DISTRO" or key_upper == "WSL_DISTRO_NAME":
                wsl_distro = val if val else None
            elif key_upper == "STARTUP_METHOD":
                v = val.strip().lower()
                if v in {"task", "registry"}:
                    startup_method = v
                else:
                    raise ValueError("STARTUP_METHOD 仅支持 task 或 registry")

    if not ports:
        raise ValueError("配置 PORTLIST 为空或无效")

    return MapperConfig(
        ports=ports,
        start_with_system=start_with_system,
        listen_address=listen_address,
        wsl_distro=wsl_distro,
        startup_method=startup_method,
    )


def is_user_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() == 1
    except Exception:
        return False


def run_cmd(command: List[str]) -> Tuple[int, str, str]:
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
    )
    out_b, err_b = process.communicate()
    out = out_b.decode("utf-8", errors="ignore")
    err = err_b.decode("utf-8", errors="ignore")
    return process.returncode, out, err


def get_wsl_ipv4(distro: Optional[str]) -> Optional[str]:
    # 优先使用 hostname -I
    base = ["wsl.exe"]
    if distro:
        base += ["-d", distro]
    cmd1 = base + ["hostname", "-I"]
    code, out, _ = run_cmd(cmd1)
    if code == 0:
        ips = re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", out)
        ipv4s = [ip for ip in ips if not ip.startswith("127.")]
        if ipv4s:
            return ipv4s[0]

    # 尝试通过 ip 命令获取 eth0 地址
    cmd2 = base + ["-e", "sh", "-lc", "ip -4 addr show eth0 | awk '/inet /{print $2}' | cut -d'/' -f1 | head -n1"]
    code, out, _ = run_cmd(cmd2)
    ip = out.strip()
    if code == 0 and re.match(r"^(?:\d{1,3}\.){3}\d{1,3}$", ip):
        if not ip.startswith("127."):
            return ip

    return None


def delete_portproxy_rule(port: int, listen_address: str) -> None:
    # 删除 v4 -> v4 规则
    run_cmd([
        "netsh", "interface", "portproxy", "delete", "v4tov4",
        f"listenport={port}", f"listenaddress={listen_address}",
    ])


def add_portproxy_rule(port: int, listen_address: str, connect_ip: str, connect_port: Optional[int] = None) -> Tuple[int, str, str]:
    if connect_port is None:
        connect_port = port
    return run_cmd([
        "netsh", "interface", "portproxy", "add", "v4tov4",
        f"listenport={port}", f"listenaddress={listen_address}",
        f"connectport={connect_port}", f"connectaddress={connect_ip}",
    ])


def ensure_firewall_rule(port: int) -> None:
    rule_name = f"WSL Port {port}"
    # 尝试先删除同名规则，避免重复
    run_cmd(["netsh", "advfirewall", "firewall", "delete", "rule", f"name={rule_name}"])
    # 添加允许入站规则
    run_cmd([
        "netsh", "advfirewall", "firewall", "add", "rule",
        f"name={rule_name}", "dir=in", "action=allow", "protocol=TCP", f"localport={port}",
    ])


def remove_firewall_rule(port: int) -> None:
    rule_name = f"WSL Port {port}"
    run_cmd(["netsh", "advfirewall", "firewall", "delete", "rule", f"name={rule_name}"])


def register_startup_task(script_path: str, config_path: str, method: str = "task") -> None:
    python_exe = sys.executable
    if method == "registry":
        # 当前用户 Run 注册表项
        name = "WSLPortMapper"
        value = f'"{python_exe}" "{script_path}" --config "{config_path}"'
        run_cmd([
            "reg", "add",
            r"HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Run",
            "/v", name, "/t", "REG_SZ", "/d", value, "/f",
        ])
        print("已注册登录自启动（当前用户，Registry 模式）")
        return

    # 默认使用计划任务，提升权限运行
    task_name = "WSL Port Mapper"
    # 延迟 15 秒，确保 WSL 网络初始化
    tr = f'\"{python_exe}\" \"{script_path}\" --config \"{config_path}\"'
    run_cmd([
        "schtasks", "/Create", "/TN", task_name,
        "/TR", tr,
        "/SC", "ONLOGON",
        "/RL", "HIGHEST",
        "/DELAY", "0000:15",
        "/F",
    ])
    print("已注册计划任务为开机自启动（最高权限）")


def apply_mappings(cfg: MapperConfig, wsl_ip: str) -> None:
    print(f"WSL IPv4: {wsl_ip}")
    for port in cfg.ports:
        print(f"配置端口 {port}: {cfg.listen_address} -> {wsl_ip}:{port}")
        delete_portproxy_rule(port, cfg.listen_address)
        code, _, err = add_portproxy_rule(port, cfg.listen_address, wsl_ip, port)
        if code != 0:
            print(f"添加端口映射失败: 端口 {port}, 错误: {err.strip()}")
            continue
        ensure_firewall_rule(port)
        print(f"端口 {port} 映射完成")


def remove_mappings(cfg: MapperConfig) -> None:
    for port in cfg.ports:
        print(f"移除端口 {port} 的映射和防火墙规则")
        delete_portproxy_rule(port, cfg.listen_address)
        remove_firewall_rule(port)


def main() -> None:
    parser = argparse.ArgumentParser(description="Windows <-> WSL 端口映射工具")
    parser.add_argument("--config", default=os.path.join(os.path.dirname(__file__), "config.ini"), help="配置文件路径")
    parser.add_argument("--remove", action="store_true", help="移除配置中的端口映射")
    parser.add_argument("--register-startup", action="store_true", help="注册为开机自启动（读取配置后按配置的 startup_method 执行）")
    parser.add_argument("--distro", default=None, help="临时覆盖配置中的 WSL 发行版名称")
    args = parser.parse_args()

    try:
        cfg = read_config(args.config)
    except Exception as e:
        print(f"读取配置失败: {e}")
        sys.exit(1)

    if args.distro:
        cfg.wsl_distro = args.distro

    if args.remove:
        if not is_user_admin():
            print("当前非管理员权限，可能无法删除端口映射/防火墙规则。请以管理员身份运行。")
        remove_mappings(cfg)
        return

    if not is_user_admin():
        print("警告: 当前非管理员权限。添加端口映射与防火墙规则通常需要管理员权限。")

    # 确保 WSL 启动一次（提升 IP 获取成功率）
    run_cmd(["wsl.exe", "-e", "sh", "-lc", "echo start"])

    wsl_ip = get_wsl_ipv4(cfg.wsl_distro)
    if not wsl_ip:
        print("无法获取 WSL IPv4 地址，请确认 WSL 已安装并可正常运行。")
        sys.exit(2)

    apply_mappings(cfg, wsl_ip)

    if args.register_startup or cfg.start_with_system:
        script_abs = os.path.abspath(__file__)
        cfg_abs = os.path.abspath(args.config)
        register_startup_task(script_abs, cfg_abs, cfg.startup_method)

    print("全部处理完成。")


if __name__ == "__main__":
    main()


