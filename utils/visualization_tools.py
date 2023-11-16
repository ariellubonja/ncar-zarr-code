import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np


def animate_cube(data, dimension='z', save_filename='visualizations/3d_data_animation.gif', n_frames=512):
    """
    Animate a 3D data cube along a given dimension.
    :param data: 3D data cube
    :param dimension: dimension to animate along
    """
    def init(): 
        # creating an empty plot/frame 
        im.set_data(np.zeros((512, 512)))
        return [im]
    
    def animate_z(i):
        im.set_data(data[i, :, :])
        return [im]

    def animate_y(i):
        im.set_data(data[:, i, :])
        return [im]

    def animate_x(i):
        im.set_data(data[:, :, i])
        return [im]
    
    vmin = np.min(data)
    vmax = np.max(data)

    fig = plt.figure(figsize=(12, 12))
    im = plt.imshow(data[0, :, :], vmin=vmin, vmax=vmax, animated=True, cmap='gray')

    if dimension == 'z':
        anim_fn = animate_z
    elif dimension == 'y':
        anim_fn = animate_y
    elif dimension == 'x':
        anim_fn = animate_x
    
    ani = animation.FuncAnimation(fig, anim_fn, init_func=init, frames=n_frames, interval=200, blit=True)
    
    ani.save(save_filename, writer='pillow', fps=5)

