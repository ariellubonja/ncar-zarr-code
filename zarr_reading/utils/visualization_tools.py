import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np


def animate_cube(data_cube, dimension='z'):
    def init(): 
        # creating an empty plot/frame 
        im.set_data(np.zeros((512, 512)))
        return [im]
    
    vmin = np.min(data_cube)
    vmax = np.max(data_cube)
    

def animate_z(i):
    im.set_data(data[i, :, :])
    return [im]