import datetime
import peewee as pw
import pandas as pd

db = pw.SqliteDatabase('ohlcv_data_1d.db')

#db = pw.SqliteDatabase('../util_get_data/ohlcv_data_1d.db')


class OHLCVData(pw.Model):
    ''' ORM object '''
    exchange = pw.CharField()
    symbol = pw.CharField()
    base = pw.CharField()
    quote = pw.CharField()
    timestamp = pw.BigIntegerField()
    open = pw.DoubleField()
    high = pw.DoubleField()
    low = pw.DoubleField()
    close = pw.DoubleField()
    volume = pw.DoubleField()

    class Meta:
        ''' pewee parent '''
        database = db


def get_data(symbol: str = 'ETH/BTC'):
    ''' Returns dataframe for exactly one smybol from db '''
    result_series_timstamps = []
    ohlcv_series = OHLCVData.select().where(OHLCVData.symbol == symbol)
    for candle in ohlcv_series:
        timestamp_converted = datetime.datetime.utcfromtimestamp(
            candle.timestamp / 1000)
        result_series_timstamps.append(timestamp_converted)
    dfr = pd.DataFrame(
        list(ohlcv_series.dicts()), index=result_series_timstamps)
    return dfr


def extract_close_value(dfr: pd.DataFrame()):
    ''' Removes all fields except CLOSE from OHCL dataframe '''
    base_currency = dfr.iloc[0]['base']
    dfr = dfr.drop(
        [
            'base', 'exchange', 'high', 'id', 'low', 'open', 'quote', 'symbol',
            'timestamp', 'volume'
        ],
        axis=1)
    dfr.columns = [base_currency]
    return dfr


def get_available_symbols(quote_currency: str = 'BTC'):
    ''' Returns list with all available symbols with the given quote currency '''
    symbols = []
    distinct_list = (OHLCVData.select(OHLCVData.symbol, OHLCVData.base,
                                      OHLCVData.quote).distinct()
                     .where(OHLCVData.quote == quote_currency))
    for result in distinct_list:
        symbols.append(result.symbol)
    return symbols


def get_all_data_combined():
    ''' Returns a dataframe with all closing values combined '''

    # Get available coins
    symbols = get_available_symbols()
    available_symbols = set(symbols)

    # Initialize empty coin lists for candles
    available_symbols_candles = {}
    for symbol in available_symbols:
        available_symbols_candles[symbol] = []

    # Sort candles into coin lists
    all_candles = OHLCVData.select()
    for candle in all_candles:
        if candle.symbol in available_symbols:
            available_symbols_candles[candle.symbol].append(candle)

    # Convert coin lists into timeseries dataframes
    result_dfs = []
    for symbol in available_symbols:
        result_series_timstamps = []
        result_series_dicts = []
        for candle in available_symbols_candles[symbol]:
            timestamp_converted = datetime.datetime.utcfromtimestamp(
                candle.timestamp / 1000)
            result_series_timstamps.append(timestamp_converted)
            result_series_dicts.append(vars(candle)['_data'])
        dfr = pd.DataFrame(result_series_dicts, index=result_series_timstamps)
        dfr = extract_close_value(dfr)
        result_dfs.append(dfr)

    # Merge and return combined dataframe
    return pd.concat(result_dfs, axis=1)


def get_most_expensive_symbols(combined_data_df: pd.DataFrame(),
                               date: str = '2017-01-01'):
    ''' Return the most expensive coins at the given date from the combined dataframe '''
    starting_point = combined_data_df.loc[date].squeeze()
    starting_point = starting_point.fillna(0).sort_values(ascending=False)
    return starting_point.keys()


def convert_combined_df_to_usdt(combined_data_df: pd.DataFrame()):
    ''' Return dataframe with USD/T prices (instead of BTC) '''

    # get and extract usdtc close series
    btc_usd_df = get_data('BTC/USDT')
    btc_usdt_series = btc_usd_df.close

    # truncate period where USDT was not available
    combined_dfr = combined_data_df[btc_usd_df.index[0]:]

    # convert to usdt
    combined_dfr_usdt = combined_dfr.multiply(btc_usdt_series, axis='index')

    # add btc/usdt
    combined_dfr_usdt['BTC'] = btc_usdt_series

    return combined_dfr_usdt