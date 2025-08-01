在三次贝塞尔曲线 cubic_bezier（如 CSS cubic-bezier）中，x 轴和 y 轴的值都是参数 t 的函数。如果你想通过任意 x 值反查该曲线上的 y（也就是给定 x，找出贝塞尔曲线上的 y），核心思路如下：

先以高精度采样，生成曲线的所有 (x, y) 对。

用插值法，对采样得到的曲线点，找到最接近输入 x 的两点，并做线性插值。

这种方法即使 x 对 t 不是严格单调，也可应对大多数实际斜率不会过于极端的动画曲线。

下面是 Python 实现的关键片段：

python
import numpy as np

def cubic_bezier(t, p0, p1, p2, p3):
    return (1-t)**3*p0 + 3*(1-t)**2*t*p1 + 3*(1-t)*t**2*p2 + t**3*p3

# 获取给定x对应的y值（数值近似）
def get_y_for_x(x_target, p0, p1, p2, p3, num_points=1000):
    ts = np.linspace(0, 1, num_points)
    points = np.array([cubic_bezier(t, p0, p1, p2, p3) for t in ts])
    xs = points[:, 0]
    ys = points[:, 1]

    sorted_indices = np.argsort(xs)
    sorted_xs = xs[sorted_indices]
    sorted_ys = ys[sorted_indices]

    if x_target < sorted_xs[0] or x_target > sorted_xs[-1]:
        return None  # 超出曲线x范围
    idx = np.searchsorted(sorted_xs, x_target)
    if idx == 0:
        return sorted_ys[0]
    if idx == len(sorted_xs):
        return sorted_ys[-1]
    x0, x1 = sorted_xs[idx-1], sorted_xs[idx]
    y0, y1 = sorted_ys[idx-1], sorted_ys[idx]
    y = y0 + (y1 - y0) * (x_target - x0) / (x1 - x0)
    return y

# 示例
p0 = np.array([0, 0])
p1 = np.array([0.16, 1.17])
p2 = np.array([1.13, 1.25])
p3 = np.array([1, 1])

x = 0.5
y = get_y_for_x(x, p0, p1, p2, p3)
print(f"当 x={x} 时，曲线上的 y ≈ {y:.4f}")
上例 x=0.5 时，计算得 y≈0.9622。

可以循环调用该函数获得任意 x 范围内的 y 值，用于动画映射或离线生成采样表。

此方法无需解析反解三次方程，适合所有贝塞尔曲线。你可以根据实际需求将部分代码进一步包装，支持区间批量查询等。