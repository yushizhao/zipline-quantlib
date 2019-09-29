from bokeh.plotting import figure, output_notebook, show
    
def Return_Benchmark(result):	
    '''Plot algorithm_period_return and benchmark_period_return.
    Change output_notebook() to get a file output.

    Parameters
    ----------
    result is the result passed into analyze in a zipline algorithm.   
    '''
    rtn = list(result.algorithm_period_return)
    rtn_bm = list(result.benchmark_period_return)
    dates = list(result.index)
    
    output_notebook()
    
    p = figure(width=800, height=350, x_axis_type="datetime")
    p.line(dates, rtn_bm, color='darkgrey', line_dash='dashed', legend='Benchmark')
    p.line(dates, rtn, color='navy', legend='Algorithm')
    p.legend.location = "top_left"
    p.grid.grid_line_alpha=0
    p.xaxis.axis_label = 'Date'
    p.yaxis.axis_label = 'Return'
    p.ygrid.band_fill_color="olive"
    p.ygrid.band_fill_alpha = 0.1
    
    show(p)