import configparser
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Button

config_path = "config.ini"

def load_points():
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    def to_np(s):
        return np.array(list(map(float, s.split(','))))
    p0 = to_np(config['BezierPoints']['p0'])
    p1 = to_np(config['BezierPoints']['p1'])
    p2 = to_np(config['BezierPoints']['p2'])
    p3 = to_np(config['BezierPoints']['p3'])
    return p0, p1, p2, p3

def save_points(p0, p1, p2, p3):
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    def arr2str(a):
        return f"{a[0]:.4f},{a[1]:.4f}"
    config['BezierPoints']['p0'] = arr2str(p0)
    config['BezierPoints']['p1'] = arr2str(p1)
    config['BezierPoints']['p2'] = arr2str(p2)
    config['BezierPoints']['p3'] = arr2str(p3)
    with open(config_path, 'w', encoding='utf-8') as f:
        config.write(f)

class DraggablePoint:
    def __init__(self, point, update_func, ax, label):
        self.point = point
        self.update_func = update_func
        self.ax = ax
        self.press = None
        self.label = label

    def connect(self):
        self.cidpress = self.point.figure.canvas.mpl_connect('button_press_event', self.on_press)
        self.cidrelease = self.point.figure.canvas.mpl_connect('button_release_event', self.on_release)
        self.cidmotion = self.point.figure.canvas.mpl_connect('motion_notify_event', self.on_motion)

    def on_press(self, event):
        if event.inaxes != self.point.axes: return
        contains, _ = self.point.contains(event)
        if not contains: return
        x0, y0 = self.point.get_data()
        self.press = (x0[0], y0[0], event.xdata, event.ydata)

    def on_motion(self, event):
        if self.press is None: return
        if event.inaxes != self.point.axes: return
        x0, y0, xpress, ypress = self.press
        dx = event.xdata - xpress
        dy = event.ydata - ypress
        new_x = x0 + dx
        new_y = y0 + dy
        new_x = min(max(new_x, -1.5), 1.5)
        new_y = min(max(new_y, -1.5), 1.5)
        self.point.set_data([new_x], [new_y])
        self.update_func(new_x, new_y)
        self.point.figure.canvas.draw()
        self.label.set_position((new_x, new_y))
        self.label.set_text(f'({new_x:.4f}, {new_y:.4f})')

    def on_release(self, event):
        self.press = None
        self.point.figure.canvas.draw()

def cubic_bezier(t, p0, p1, p2, p3):
    return (1-t)**3*p0 + 3*(1-t)**2*t*p1 + 3*(1-t)*t**2*p2 + t**3*p3

p0, p1, p2, p3 = load_points()
num_points = 100
fig, ax = plt.subplots(figsize=(8,6))
curve_pts = np.array([cubic_bezier(t, p0, p1, p2, p3) for t in np.linspace(0, 1, num_points)])
line, = ax.plot(curve_pts[:, 0], curve_pts[:, 1], label='Cubic Bezier Curve')

points_data = [p0, p1, p2, p3]
points_plot = [ax.plot(point[0], point[1], 'ro')[0] for point in points_data]
labels = []
for i, point in enumerate(points_data):
    label = ax.text(point[0], point[1], f'P{i}: ({point[0]:.4f}, {point[1]:.4f})', fontsize=10, ha='left', va='bottom')
    labels.append(label)

def update_curve():
    curve_pts = np.array([cubic_bezier(t, p0, p1, p2, p3) for t in np.linspace(0, 1, num_points)])
    line.set_data(curve_pts[:, 0], curve_pts[:, 1])

# 要实现 p1、p2、p3 拖拽，定义回调
def update_p1(x, y): p1[0], p1[1] = x, y; update_curve()
def update_p2(x, y): p2[0], p2[1] = x, y; update_curve()
def update_p3(x, y): p3[0], p3[1] = x, y; update_curve()
# 如需 p0 也拖拽，添加 update_p0(x, y): p0[0], p0[1] = x, y; update_curve()

# 可拖拽 p1、p2、p3
p1_drag = DraggablePoint(points_plot[1], update_p1, ax, labels[1])
p2_drag = DraggablePoint(points_plot[2], update_p2, ax, labels[2])
p3_drag = DraggablePoint(points_plot[3], update_p3, ax, labels[3])
p1_drag.connect()
p2_drag.connect()
p3_drag.connect()

config = configparser.ConfigParser()
config.read(config_path, encoding='utf-8')
x_min = float(config['axis']['x_min'])
x_max = float(config['axis']['x_max'])
y_min = float(config['axis']['y_min'])
y_max = float(config['axis']['y_max'])
ax.set_xlim(x_min, x_max)
ax.set_ylim(y_min, y_max)
ax.set_aspect('equal', adjustable='box')
ax.grid(True)
ax.legend()
ax.set_title('Interactive Bezier with INI Config')
ax.set_xlabel('X')
ax.set_ylabel('Y')

def on_save(event):
    save_points(p0, p1, p2, p3)
    print('已保存当前控制点参数到 config.ini')

saveax = plt.axes([0.81, 0.01, 0.1, 0.075])
button = Button(saveax, 'SAVE')
button.on_clicked(on_save)

plt.show()
