import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')
import pandas as pd




def ScatterPlotReturnsExcessReturn(dataProvider, returnDim, excessReturnDim):
    dataProvider.LoadCachedClose()
    ccyList = list(dataProvider.ToRefCcy.keys())
    ccyTimeSeries = dataProvider.CloseDataStorage[ccyList]
    #ccyTimeSeries.to_csv("C:\\temp\\timeSeries.csv")
    returns = (ccyTimeSeries - ccyTimeSeries.shift(returnDim)) / ccyTimeSeries.shift(returnDim)
    returns = returns.shift(-returnDim)
    returnsColNames = [t+"_"+"R" for t in returns.columns]
    returns.columns = returnsColNames
    excessReturns = (ccyTimeSeries - ccyTimeSeries.shift(excessReturnDim)) / ccyTimeSeries.shift(excessReturnDim)
    excessReturns = excessReturns.shift(-(excessReturnDim+returnDim))
    excessReturnsColNames = [t+"_"+"E" for t in ccyTimeSeries.columns]
    excessReturns.columns = excessReturnsColNames
    totalMatrix = pd.concat([ccyTimeSeries, returns, excessReturns], axis=1)
    plt.figure()
    x=list()
    y=list()
    j=1
    focus = totalMatrix[["XVG", "XVG_R", "XVG_E"]]
    focus.to_csv("C:\\temp\\focus.csv")
    x1 = totalMatrix["XVG_R"].values
    y1 = totalMatrix["XVG_E"].values
    plt.scatter(x=x1, y=y1, marker = '.', s=1)
    plt.show()
    for ccy in ccyList:
        x.append(totalMatrix[ccy+"_R"].values)
        y.append(totalMatrix[ccy+"_E"].values)
    plt.scatter(x=x, y=y,marker = '.', s=1)
    plt.show()
    b=2



def get_cmap(n, name='hsv'):
    '''Returns a function that maps each index in 0, 1, ..., n-1 to a distinct
    RGB color; the keyword argument name must be a standard mpl colormap name.'''
    return plt.cm.get_cmap(name, n)