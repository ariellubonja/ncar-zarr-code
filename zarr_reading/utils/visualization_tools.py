import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np


def animate_cube(data, dimension='z', save_filename='3d_data_animation.gif'):
    """
    Animate a 3D data cube along a given dimension.
    :param data: 3D data cube
    :param dimension: dimension to animate along
    """
    def init(): 
        # creating an empty plot/frame 
        im.set_data(np.zeros((512, 512)))
        return [im]
    
    vmin = np.min(data)
    vmax = np.max(data)

    fig = plt.figure(figsize=(12, 12))
    im = plt.imshow(data[0, :, :], vmin=vmin, vmax=vmax, animated=True, cmap='gray')

    if dimension == 'z':
        ani = animation.FuncAnimation(fig, animate_z, init_func=init, frames=100, interval=200, blit=True)
    elif dimension == 'y':
        ani = animation.FuncAnimation(fig, animate_y, init_func=init, frames=100, interval=200, blit=True)
    elif dimension == 'x':
        ani = animation.FuncAnimation(fig, animate_x, init_func=init, frames=100, interval=200, blit=True)
    
    ani.save(save_filename, writer='pillow', fps=5)

def animate_z(i):
    im.set_data(data[i, :, :])
    return [im]

def animate_y(i):
    im.set_data(data[:, i, :])
    return [im]

def animate_x(i):
    im.set_data(data[:, :, i])
    return [im]