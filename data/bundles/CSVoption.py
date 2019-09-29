import os

import numpy as np
import pandas as pd
from pandas_datareader.data import DataReader
import requests

from zipline.utils.cli import maybe_show_progress
from .core import register

def load_csv(filepath):
    data = pd.read_csv(filepath, index_col="option symbol")
    return data

def LoadOneSymbol(df_multiSymbol, symbol):
    """If there is only one row related to that symbol in df_multiSymbol,
    then we have a problem because .loc will return a Series instead of DataFrame.
    It is easy to fix. Leave it for now.
    """
    data = df_multiSymbol.loc[symbol]
    data = data.set_index('date')
    data.index = pd.DatetimeIndex(data.index)
    data.sort_index(inplace=True)
    data.index.names = ["Date"]
    data.rename(
        columns={
            'adjusted stock close price': 'adjusted_close',
            'unadjusted stock price': 'close',
            'open interest': 'open_interest',
            'call/put': 'callput',
            'symbol': 'underlying'
        },
        inplace=True,
    )
    return data

def _cachpath(symbol, type_):
    return '-'.join((symbol.replace(os.path.sep, '_'), type_))  

def CSV_option(filepath, symbols, start=None, end=None):
    """Create a data bundle ingest function from a set of symbols loaded from
    yahoo.

    Parameters
    ----------
    symbols : iterable[str]
        The ticker symbols to load data for.
    start : datetime, optional
        The start date to query for. By default this pulls the full history
        for the calendar.
    end : datetime, optional
        The end date to query for. By default this pulls the full history
        for the calendar.

    Returns
    -------
    ingest : callable
        The bundle ingest function for the given set of symbols.

    Examples
    --------
    This code should be added to ~/.zipline/extension.py

    .. code-block:: python

       from zipline.data.bundles import yahoo_equities, register

       symbols = (
           'AAPL',
           'IBM',
           'MSFT',
       )
       register('my_bundle', yahoo_equities(symbols))

    Notes
    -----
    The sids for each symbol will be the index into the symbols sequence.
    """
    # strict this in memory so that we can reiterate over it
    symbols = tuple(symbols)

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):
        if start is None:
            start = start_session
        if end is None:
            end = None

        metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('expiration', 'datetime64[ns]'),
            ('strike', 'int32'),
            ('callput', 'object'),
            ('style', 'object'),
            ('underlying', 'object'),
            ('exchange', 'object'),
            ('symbol', 'object')
        ]))

        df_multiSymbol = load_csv(filepath)

        def _pricing_iter():
            sid = 0
            with maybe_show_progress(
                    symbols,
                    show_progress,
                    label='Processing CSV: ') as it, \
                    requests.Session() as session:
                for symbol in it:
                    # path = _cachpath(symbol, 'yuusha')
                    # try:
                    #     df = cache[path]
                    # except KeyError:
                    #     df = cache[path] = LoadOneSymbol(df_multiSymbol, symbol)
                    
                    df = LoadOneSymbol(df_multiSymbol, symbol)

                    # the start date is the date of the first trade and
                    # the end date is the date of the last trade
                    start_date = df.index[0]
                    end_date = df.index[-1]
                    # The auto_close date is the day after the last trade.
                    ac_date = end_date + pd.Timedelta(days=1)
                    ex_date = pd.to_datetime(df["expiration"][0])
                    strike = df["strike"][0]
                    callput = df["callput"][0]
                    style = df["style"][0]
                    underlying = df["underlying"][0]
                    exchange = df["exchange"][0]
                    metadata.iloc[sid] = start_date, end_date, ac_date, ex_date, strike, \
                        callput, style, underlying, exchange, symbol

                    yield sid, df
                    sid += 1
        
        daily_bar_writer.write(_pricing_iter(), show_progress=show_progress)

        symbol_map = pd.Series(metadata.symbol.index, metadata.symbol)

        asset_db_writer.write(equities=metadata)
        adjustment_writer.write(splits=None, dividends=None)
    return ingest