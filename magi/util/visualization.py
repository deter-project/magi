#!/usr/bin/env python
import matplotlib.pyplot as plt
import numpy as np


def line_Graph(xLabel,yLabel,title,x,y,output):
    """
       Program to plot the line graph using matplotlib 
    """
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)	
    plt.title(title)
    ax = plt.subplot(111)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    lines = plt.plot(x, y)
    plt.setp(lines, 'color', 'r', 'linewidth', 2.0)
    plt.savefig(output)

def scatter_Graph(xLabel,yLabel,title,x,y,output):
    """
        Program to plot the scatter graph using matplotlib
    """
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)	
    plt.title(title)
    ax = plt.subplot(111)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    x = np.float64(x)
    y = np.float64(y)
    plt.scatter(x, y)
    plt.savefig(output)
