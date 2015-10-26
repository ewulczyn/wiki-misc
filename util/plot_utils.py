import matplotlib
import matplotlib.pyplot as plt
import prettyplotlib as ppl
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
py.sign_in("ewulczyn", "y2a7n75kl8")


def plot_df(d, ylabel = '', xlabel = '', title = '', interactive = False, rotate = False):
    fig = plt.figure(figsize=(10, 4), dpi=80)
    for c in d.columns:
        plt.plot(d.index.to_pydatetime(), d[c], label = c)
        if rotate:
            plt.xticks(d.index.to_pydatetime(), d.index.to_pydatetime(), rotation='vertical')
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.title(title)

    if interactive:
        plt.legend()
        return py.iplot_mpl(fig)

    else:
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    ax=plt.gca()
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%d%b\n%y'))


