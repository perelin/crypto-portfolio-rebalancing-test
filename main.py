#!/usr/bin/env python3
'''
pipenv shell
'''

from pprint import pprint

import pandas as pd
import matplotlib.pyplot as plt

import broker
from logger import log
from data import *

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

def backtest(data: pd.DataFrame(), startdate: str, enddate: str):

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

    # Setup inital trading universe
    portfolio_size = 10
    portfolio_symbols = get_most_expensive_symbols(data, startdate)[
        0:portfolio_size]

    # Setup initial $$$
    initial_quote_amount = broker.QUOTE_AMOUNT
    initial_invest_per_symbol = broker.QUOTE_AMOUNT / len(portfolio_symbols)

    # Buy initial portfolio
    for symbol in portfolio_symbols:
        broker.buy(
            data=data,
            date=startdate,
            base=symbol,
            quote_buy_amount=initial_invest_per_symbol)

    # Step over Data
    candle_date = data[startdate:enddate].index.values
    date_totals = {}
    for date in candle_date:
        # Portfolio Logic
        #rebalance_portfolio_weights(data, date)
        # Equity curve calculation
        final_total_value, portfolio_values = broker.get_value_at(data, date)
        date_totals[date] = final_total_value

    # Final portfolio balance
    log.info("result: \n {}".format(portfolio_values))
    log.info("total fees: {}".format(round(broker.get_total_fees(), 2)))
    log.info("total: {}".format(round(final_total_value, 2)))

    # Equity curve to Series
    candle_date_series = pd.Series(date_totals)
    #log.debug(candle_date_series)

    # Ploting
    BTC_series = data.loc[startdate:enddate, 'BTC']
    plt.rcParams["patch.force_edgecolor"] = True
    plt.plot(candle_date_series, label='Portfolio')
    plt.plot(BTC_series, label='BTC')
    plt.legend()
    plt.show(block=True)

def rebalance_portfolio_weights(data: pd.DataFrame(), date: str):

    log.debug("Rebalancing portfolio at {}".format(date))

    # get portfolio value
    final_total_value, portfolio_values = broker.get_value_at(data, date)
    log.debug("Portfolio values at {}: \n {}".format(date, portfolio_values))
    log.debug("Total portfolio value: {}".format(round(final_total_value, 2)))

    # get target USD distribution
    even_usd_value_distribution = final_total_value / len(broker.POSITIONS)
    even_perc_value_distribution = 1 / len(broker.POSITIONS)

    # extend portfolio data frame with distribution values
    portfolio_values[
        'distribution_change_USDT'] = even_usd_value_distribution - portfolio_values['USDT_amount']
    portfolio_values['distribution_perc'] = portfolio_values[
        'USDT_amount'] / final_total_value

    # split portfolio data frame in buy and sell parts
    symbols_to_decrease = portfolio_values.loc[
        portfolio_values['distribution_change_USDT'] < 0]

    symbols_to_increase = portfolio_values.loc[
        portfolio_values['distribution_change_USDT'] > 0]

    log.debug("even distribution usd: {}".format(even_usd_value_distribution))
    log.debug(
        "even distribution perc: {}".format(even_perc_value_distribution))
    log.debug("portfolio_values: \n {}".format(portfolio_values))
    log.debug("symbols_to_decrease: \n {}".format(symbols_to_decrease))
    #log.debug("symbols_to_increase: \n {}".format(symbols_to_increase))

    for base, row in symbols_to_decrease.iterrows():
        # Find out how much base to sell
        price = row['BASE/USDT']
        sell_amount_base = abs(row['distribution_change_USDT']) / price
        #print(base, row['distribution_change_USDT'], price, sell_amount_base)
        # Sell
        broker.sell(
            data=data,
            date=date,
            base=base,
            base_sell_amount=sell_amount_base)

    # Turn off chained assignment warning for pandas
    # https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
    pd.options.mode.chained_assignment = None

    # how much should be redistributed
    total_amount_to_distribute = broker.QUOTE_AMOUNT
    log.debug("QUOTE_AMOUNT after selling: {}".format(broker.QUOTE_AMOUNT))

    # extend symbols to increase with increase percentage distribution
    total_increase_value = symbols_to_increase[
        'distribution_change_USDT'].sum()

    symbols_to_increase['increase_distr_perc'] = symbols_to_increase[
        'distribution_change_USDT'] / total_increase_value

    symbols_to_increase['buy_amount'] = symbols_to_increase[
        'increase_distr_perc'] * total_amount_to_distribute

    log.debug("symbols_to_increase: \n {}".format(symbols_to_increase))

    for base, row in symbols_to_increase.iterrows():
        # Buy
        broker.buy(
            data=data,
            date=date,
            base=base,
            quote_buy_amount=row['buy_amount'])

    # get portfolio value
    final_total_value, portfolio_values = broker.get_value_at(data, date)
    portfolio_values[
        'distribution_change_USDT'] = even_usd_value_distribution - portfolio_values['USDT_amount']
    portfolio_values['distribution_perc'] = portfolio_values[
        'USDT_amount'] / final_total_value
    log.debug("Portfolio values at {}: \n {}".format(date, portfolio_values))
    log.debug("Total portfolio value: {}".format(round(final_total_value, 2)))





if __name__ == '__main__':

    log.info("Backtest starting...")
    db.connect()

    log.info("Collecting and preparing data...")
    combined_df = get_all_data_combined()
    data = convert_combined_df_to_usdt(combined_df)
    log.debug("Data USD values data frame: \n {}".format(data.tail(5)))

    #print(data.head(5))
    #most_expensive_symbols_returns(data)

    log.info("Testing...")
    backtest(data=data, startdate='2017-01-01', enddate='2017-12-01')

    #rebalance_portfolio_weights(data, '2017-12-01')
