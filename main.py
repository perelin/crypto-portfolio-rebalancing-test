#!/usr/bin/env python3

'''
pipenv shell
'''

import datetime
from pprint import pprint
import sys

#import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import peewee as pw

from logbook import Logger, StreamHandler
StreamHandler(sys.stdout).push_application()
log = Logger(__name__)

db = pw.SqliteDatabase('ohlcv_data_1d.db')
#db = pw.SqliteDatabase('../util_get_data/ohlcv_data_1d.db')

'''

(-) use central sql model / sql file

(/) have all values in USD:
    get BTC/USDT values as well -> own series/df
    convert all values from BTC to USDT based on BTC/USDT series
    concat BTC/USDT values

(-) calculate sharpe/sortino value, based on https://www.theice.com/iba/libor risk free asset (ca 2% anual return)

(-) for rebalancing use 0.25% fee p trade

(-) test 1: hold the x most valuable symbols from date x
(-) test 2: rebalance every x to update to current most valuable symbols
(-) test 3: rebalance like 2 + rebalance weights
(-) test 4: rebalance like 2 + rebalance weight - distribution accrding to time (/position) in most valuable

continues:
instead of most valuable, use highest market cap (get historic from coinmarketcap web scraper)
instead of most valuable, use highest gainer (1d, 7d, 30d)

'''

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


class Trade(pw.Model):
    ''' ORM object '''
    trade_type = pw.CharField()
    exchange = pw.CharField()
    symbol = pw.CharField()
    base = pw.CharField()
    quote = pw.CharField()

    opened_date = pw.BigIntegerField()
    closed_date = pw.BigIntegerField()

    quote_amount = pw.DoubleField() # how much did I spend
    base_amount = pw.DoubleField() # how much did I buy
    opened_price = pw.DoubleField()
    closed_price = pw.DoubleField()
    fees = pw.DoubleField()

    class Meta:
        ''' pewee parent '''
        database = db

POSITIONS = {}
QUOTE_AMOUNT = 1000.0

def get_data(symbol: str = 'ETH/BTC'):
    ''' Returns dataframe for exactly one smybol from db '''
    result_series_timstamps = []
    ohlcv_series = OHLCVData.select().where(OHLCVData.symbol == symbol)
    for candle in ohlcv_series:
        timestamp_converted = datetime.datetime.utcfromtimestamp(candle.timestamp/1000)
        result_series_timstamps.append(timestamp_converted)
    dfr = pd.DataFrame(list(ohlcv_series.dicts()), index=result_series_timstamps)
    return dfr


def extract_close_value(dfr: pd.DataFrame()):
    ''' Removes all fields except CLOSE from OHCL dataframe '''
    base_currency = dfr.iloc[0]['base']
    dfr = dfr.drop(['base', 'exchange', 'high', 'id', 'low', 'open',
                    'quote', 'symbol', 'timestamp', 'volume'], axis=1)
    dfr.columns = [base_currency]
    return dfr

def get_available_symbols(quote_currency: str = 'BTC'):
    ''' Returns list with all available symbols with the given quote currency '''
    symbols = []
    distinct_list = (OHLCVData
                     .select(OHLCVData.symbol, OHLCVData.base, OHLCVData.quote)
                     .distinct()
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
        dfr = pd.DataFrame(result_series_dicts,
                           index=result_series_timstamps)
        dfr = extract_close_value(dfr)
        result_dfs.append(dfr)

    # Merge and return combined dataframe
    return pd.concat(result_dfs, axis=1)

def get_most_expensive_symbols(combined_data_df: pd.DataFrame(), date: str = '2017-01-01'):
    ''' Return the most expensive coins at the given date from the combined dataframe '''
    starting_point = combined_data_df.loc[date].squeeze()
    starting_point = starting_point.fillna(0).sort_values(ascending=False)
    return starting_point.keys()


def most_expensive_symbols_returns(combined_data_df: pd.DataFrame(),
                                   date: str = '2017-05-01'):
    ''' Calculate the return of the most expensive coins at the given date '''

    # Calculate
    portfolio_size = 20
    most_expensive_symbols = get_most_expensive_symbols(combined_data_df, date)
    print('Most expensive coins for: ' + date, most_expensive_symbols[0:portfolio_size])
    last_date = combined_data_df.index[-1]
    mes_start_values = combined_data_df.loc[date,
                                            most_expensive_symbols[0:portfolio_size]]
    mes_end_values = combined_data_df.loc[last_date,
                                          most_expensive_symbols[0:portfolio_size]]
    mes_delta_df = pd.concat([mes_start_values, mes_end_values], axis=1)
    mes_final_returns = mes_delta_df.transpose().pct_change().loc[last_date]
    print('Total return: ', mes_final_returns.sum() / len(mes_final_returns))
    print(mes_final_returns)

    # Plot
    plt.rcParams["patch.force_edgecolor"] = True
    plt.hist(mes_final_returns * 100, bins=80)
    # plt.show(block=True)

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


def backtest(data: pd.DataFrame()):

    '''
        // set initial money
        // get symbols to start with
        // create position for every symbol, equal distribution
        // step over data
        check if data ended
        halt every x period
        check distribution, rebalance
        continue stepping
    '''

    # Setup dates
    startdate = '2017-01-01'
    endate = '2017-12-01'

    # Setup inital trading universe
    portfolio_size = 10
    portfolio_symbols = get_most_expensive_symbols(data, startdate)[
        0:portfolio_size]

    # Setup initial $$$
    initial_quote_amount = QUOTE_AMOUNT
    initial_invest_per_symbol = QUOTE_AMOUNT / len(portfolio_symbols)

    # Buy initial portfolio
    for symbol in portfolio_symbols:
        buy(data=data, date=startdate,
            base=symbol,
            quote_buy_amount=initial_invest_per_symbol)

    # Step over Data
    candle_date = data[startdate:endate].index.values
    date_totals = {}
    for date in candle_date:
        # Equity curve calculation
        final_total_value, portfolio_values = get_value_at(data, date)
        date_totals[date] = final_total_value

    # Final portfolio balance
    #log.debug("result: \n {}".format(portfolio_values))
    #log.debug("total: {}".format(round(final_total_value, 2)))

    rebalance_portfolio_weights(data, date)

    # Equity curve to Series
    candle_date_series = pd.Series(date_totals)
    #log.debug(candle_date_series)

    # Ploting
    BTC_series = data.loc[startdate:endate, 'BTC']
    plt.rcParams["patch.force_edgecolor"] = True
    plt.plot(candle_date_series)
    plt.plot(BTC_series)
    #plt.show(block=True)


def rebalance_portfolio_weights(data: pd.DataFrame(), date: str):

    # get portfolio value
    final_total_value, portfolio_values = get_value_at(data, date)
    log.debug("Portfolio values at {}: \n {}".format(date, portfolio_values))
    log.debug("Total portfolio value: {}".format(round(final_total_value, 2)))

    # get target USD distribution
    even_usd_value_distribution = final_total_value / len(POSITIONS)
    even_perc_value_distribution = 1 / len(POSITIONS)

    # extend portfolio data frame (tmp)
    portfolio_values[
        'distribution_change_USDT'] = even_usd_value_distribution - portfolio_values['USDT_amount']
    portfolio_values['distribution_perc'] = portfolio_values[
        'USDT_amount'] / final_total_value

    # split portfolio data frame in buy and sell parts
    symbols_to_decrease = portfolio_values.loc[
        portfolio_values['distribution_change_USDT'] < 0]

    symbols_to_increase = portfolio_values.loc[
        portfolio_values['distribution_change_USDT'] > 0]

    #log.debug("even distribution usd: {}".format(even_usd_value_distribution))
    #log.debug(
    #    "even distribution perc: {}".format(even_perc_value_distribution))
    #log.debug("portfolio_values: \n {}".format(portfolio_values))
    #log.debug("symbols_to_decrease: \n {}".format(symbols_to_decrease))
    #log.debug("symbols_to_increase: \n {}".format(symbols_to_increase))

    for index, row in symbols_to_decrease.iterrows():
        print(index, row['distribution_change_USDT'])

    # ... 2do sell negative distribution_change to BTC and redistribute among positive distribution_change


def sell(data: pd.DataFrame(), date: str, base: str, base_sell_amount: float):
    ''' Simulate selling '''
    global QUOTE_AMOUNT, POSITIONS

    if base in POSITIONS:
        base_position = POSITIONS[base]
    else:
        raise Exception("Trying to sell from not existing position")

    # Check if enough base money is available
    if base_position['base_amount'] < base_sell_amount:
        raise Exception("Not enough BASE_AMOUNT to sell")

    # Calculate quote amount received
    price = data.loc[date, [base]]
    quote_amount = base_sell_amount * price

    # Fees
    fees = quote_amount * 0.0025
    quote_amount_after_fees = quote_amount - fees

    # Trade object
    trade = {
        'trade_type': 'sell',
        'base': base,
        'price': price[0],
        'date': date,
        'base_amount': base_sell_amount,
        'quote_amount': quote_amount.get(base),
        'fees_in_quote': fees.get(base)
    }
    base_position['trades'].append(trade)

    # Subtract from position
    base_position['base_amount'] -= base_sell_amount

    # Add money
    QUOTE_AMOUNT += quote_amount_after_fees.get(base)


def positions_base_amount_to_df():
    position_base_amount_dict = {}
    for symbol, position in POSITIONS.items():
        #position_base_amount_dict[symbol] = [position.base_amount]
        position_base_amount_dict[symbol] = [position['base_amount']]
    dfr = pd.DataFrame(position_base_amount_dict)
    return dfr


def get_value_at(data: pd.DataFrame(), date: str):

    '''
        2do: extend with BASE/BTC price and BTC_amount columns
    '''

    #log.debug("looking up portfolio value at %s" % date)

    # prepare position data
    positions_base_amount_dfr = positions_base_amount_to_df()
    positions_base_amount_dfr_tp = positions_base_amount_dfr.transpose()
    positions_base_amount_dfr_tp.columns = ['base_amount']

    # extract position symbols
    position_symbols = positions_base_amount_dfr_tp.index.values

    # extend position data frame with usdt value at date
    current_usd_prices = []
    for symbol in position_symbols:
        current_usd_prices.append(data.loc[date, [symbol]].get(symbol))
    positions_base_amount_dfr_tp['BASE/USDT'] = pd.Series(
        current_usd_prices, index=positions_base_amount_dfr_tp.index)

    # extend position data frame with total usdt per curreny
    positions_base_amount_dfr_tp['USDT_amount'] = positions_base_amount_dfr_tp['base_amount'] * \
        positions_base_amount_dfr_tp['BASE/USDT']

    # return
    total_portfolio_value = positions_base_amount_dfr_tp['USDT_amount'].sum()
    #log.debug("result: \n {}".format(positions_base_amount_dfr_tp))
    #log.debug("total: {}".format(round(total_portfolio_value,2)))
    return total_portfolio_value, positions_base_amount_dfr_tp


def buy(data: pd.DataFrame(), date: str, base: str, quote_buy_amount: float):
    ''' Simulate buying '''
    global QUOTE_AMOUNT, POSITIONS

    # Check if enough quote money is available
    if QUOTE_AMOUNT < quote_buy_amount:
        raise Exception("Not enough QUOTE_AMOUNT to buy")

    # Fees
    fees = quote_buy_amount * 0.0025
    quote_buy_amount_after_fees = quote_buy_amount - fees

    # Calculate base amount bought
    price = data.loc[date, [base]]
    base_amount = quote_buy_amount_after_fees / price

    # Trade object
    trade = {
        "trade_type": 'buy',
        "base": base,
        "price": price[0],
        "date": date,
        "base_amount": base_amount.get(base),
        "quote_amount": quote_buy_amount,
        'fees_in_quote': fees
    }

    # Check if we already have position and if so add to that position
    if base in POSITIONS:
        position = POSITIONS[base]
        position['base_amount'] += base_amount.get(base)
        position['trades'].append(trade)
    else:
        position = {
            'base': base,
            'base_amount': base_amount.get(base),
            'trades': [trade]
        }
    POSITIONS[base] = position

    # Deduct money
    QUOTE_AMOUNT -= quote_buy_amount


if __name__ == '__main__':

    db.connect()

    combined_df = get_all_data_combined()
    combined_df_usdt = convert_combined_df_to_usdt(combined_df)

    log.debug("Data USD values data frame: \n {}".format(
        combined_df_usdt.tail(5)))

    #print(combined_df_usdt.head(5))
    #most_expensive_symbols_returns(combined_df_usdt)

    backtest(combined_df_usdt)

    # pprint([POSITIONS['ETH'], QUOTE_AMOUNT])

    # sell(combined_df_usdt, '2017-12-01', 'ETH', 12.0)

    # pprint([POSITIONS['ETH'], QUOTE_AMOUNT])

    # ------- Old stuff

    # print(combined_df.loc['2017-01-01':].head(5))

    # returns = combined_df.pct_change()
    # returns_this_year = returns.loc['2017-01-01':]
    # mean_daily_returns = returns_this_year.mean()

    #print(returns.loc['2017-01-01':].head(5))

    # for index, row in returns_this_year.iterrows():
    #     print(index)
    #     for ind, value in row.iteritems():
    #         print(ind, value)

    # print(mean_daily_returns)

    # print(len(returns_this_year))

    # print(returns_this_year['XRP'].tail(5))

    # plt.rcParams["patch.force_edgecolor"] = True
    # plt.hist(returns_this_year['ETH'], bins=40)
    # plt.show(block=True)
