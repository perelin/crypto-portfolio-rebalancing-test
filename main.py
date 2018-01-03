#!/usr/bin/env python3
'''
pipenv shell
'''
import datetime
from pprint import pprint

import pandas as pd
import matplotlib.pyplot as plt

import broker
import prices
import rebalancer
from logger import log
'''

(/) use central sql model / sql file

(/) have all values in USD:
    get BTC/USDT values as well -> own series/df
    convert all values from BTC to USDT based on BTC/USDT series
    concat BTC/USDT values

(-) calculate sharpe/sortino value, based on https://www.theice.com/iba/libor risk free asset (ca 2% anual return)

(/) for rebalancing use 0.25% fee p trade

(/) test 1: hold the x most valuable symbols from date x
(-) test 2: rebalance every x to update to current most valuable symbols
(-) test 3: rebalance like 2 + rebalance weights
(-) test 4: rebalance like 2 + rebalance weight - distribution accrding to time (/position) in most valuable

continues:
instead of most valuable, use highest market cap (get historic from coinmarketcap web scraper)
instead of most valuable, use highest gainer (1d, 7d, 30d)

'''


def get_portfolio_composition_01(data: pd.DataFrame(),
                                 date: str,
                                 portfolio_size: int = 10):
    '''
        Equal distribution
    '''

    # Get symbols
    portfolio_symbols = prices.get_most_expensive_symbols(
        data, date)[0:portfolio_size]

    # Get weight distribution
    portfolio_target_weight = 1 / portfolio_size
    portfolio_weights = pd.np.array([portfolio_target_weight])
    portfolio_weights = pd.np.repeat(portfolio_weights, portfolio_size)

    # Build data frame
    portfolio_composition_dfr = pd.DataFrame(
        portfolio_weights, index=portfolio_symbols, columns=['basic_weight'])

    return portfolio_composition_dfr


def backtest(data: pd.DataFrame(),
             startdate: str,
             enddate: str,
             rebalancing: bool = True):
    '''
        Basic portfolio strategy
    '''

    rebalancing_cadence = pd.np.timedelta64(1, 'D')

    # Setup inital trading universe
    portfolio_size = 10
    portfolio_symbols = prices.get_most_expensive_symbols(
        data, startdate)[0:portfolio_size]

    initial_portfolio_composition = get_portfolio_composition_01(
        data=data, date=startdate, portfolio_size=portfolio_size)
    rebalancer.rebalance_positions(
        data=data, date=startdate, composition=initial_portfolio_composition)

    # Step over Data
    candle_date = data[startdate:enddate].index.values
    next_rebalancing_date = candle_date[0] + rebalancing_cadence
    date_totals = {}
    last_mes = portfolio_symbols
    portfolio_composition = initial_portfolio_composition

    for date in candle_date:

        # Portfolio Logic

        if date == next_rebalancing_date and rebalancing:
            rebalancer.rebalance_positions(
                data=data,
                date=date,
                composition=portfolio_composition)

            next_rebalancing_date = date + rebalancing_cadence

        # Equity curve calculation
        final_total_value, portfolio_values = broker.get_value_at(data, date)
        date_totals[date] = final_total_value

    # Final portfolio balance
    log.info("result: \n {}".format(portfolio_values))
    log.info("total fees: {}".format(round(broker.get_total_fees(), 2)))
    log.info("total: {}".format(round(final_total_value, 2)))

    # Equity curve to Series
    candle_date_series = pd.Series(date_totals)
    candle_date_dfr = pd.DataFrame(candle_date_series, columns=['total_value'])
    #calculate_sharpe(candle_date_dfr)

    # Ploting
    BTC_series = data.loc[startdate:enddate, 'BTC']
    plt.rcParams["patch.force_edgecolor"] = True
    plt.plot(candle_date_series, label='Portfolio')
    plt.plot(BTC_series, label='BTC')
    plt.legend()
    #plt.show(block=True)


def calculate_sharpe(candle_date_dfr: pd.DataFrame()):
    candle_date_dfr['pct_change'] = candle_date_dfr[
        'total_value'].pct_change().fillna(0)
    print(candle_date_dfr)
    print(candle_date_dfr['pct_change'].std())
    print(candle_date_dfr['pct_change'].mean())
    # WIP....

    # Plot
    plt.rcParams["patch.force_edgecolor"] = True
    plt.hist(candle_date_dfr['pct_change'], bins=80)
    plt.show(block=True)


if __name__ == '__main__':

    log.info("Backtest starting...")
    prices.db.connect()

    log.info("Collecting and preparing data...")
    combined_df = prices.get_all_data_combined()
    data = prices.convert_combined_df_to_usdt(combined_df)
    log.debug("Data USD values data frame: \n {}".format(data.tail(5)))

    log.info("Testing...")
    backtest(
        data=data,
        startdate='2017-01-01',
        enddate='2017-12-01',
        rebalancing=True)

    #portfolio_composition = get_portfolio_composition_01(
    #    data=data, date='2017-12-01', portfolio_size=10)

    #pprint(portfolio_composition)

    #rebalancer.rebalance_positions(data, '2017-12-01', portfolio_composition)