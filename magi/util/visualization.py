#!/usr/bin/env python
import matplotlib.pyplot as plt
import numpy as np


def line_Graph(xLabel, yLabel, title, xValues, yValues, labels, output):
    """
        Function to plot the line graph using matplotlib. Accepts the following parameters
        xLabel: Label of the x-axis
        yLabel: Label of the y-axis
        title: Title of the graph
        xValues: List of list of X Values
        yValues: List of list of Y Values
        labels: List of series labels
        output: Name of the output file to be created
    """
    setupPlot(xLabel, yLabel, title)
    
    for itr in range(len(xValues)):
        plt.plot(xValues[itr], yValues[itr], label=labels[itr])
    
    plt.legend()
    plt.savefig(output)

def scatter_Graph(xLabel, yLabel, title, xValues, yValues, labels, output):
    """
        Function to plot the scatter graph using matplotlib. Accepts the following parameters
        xLabel: Label of the x-axis
        yLabel: Label of the y-axis
        title: Title of the graph
        xValues: List of list of X Values
        yValues: List of list of Y Values
        labels: List of series labels
        output: Name of the output file to be created
    """
    setupPlot(xLabel, yLabel, title)
    
    for itr in range(len(xValues)):
        x = np.float64(xValues[itr])
        y = np.float64(yValues[itr])
        plt.scatter(x, y, label=labels[itr])
    
    plt.legend()
    plt.savefig(output)

def setupPlot(xLabel, yLabel, title):
    
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)    
    plt.title(title)

    """ Graph layout setup parameters """
    ax = plt.subplot(111)
    
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    