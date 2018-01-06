#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt

import broker
import prices
import rebalancer
from logger import log

def get_portfolio_composition_01(data: pd.DataFrame(),
                                 date: str,
                                 portfolio_size: int = 10):

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
             rebalancing: bool = True,
             plot: bool = True):

    rebalancing_cadence = pd.np.timedelta64(1, 'D')

    # Setup inital portfolio universe
    portfolio_size = 10
    portfolio_symbols = prices.get_most_expensive_symbols(
        data, startdate)[0:portfolio_size]
    initial_portfolio_composition = get_portfolio_composition_01(
        data=data, date=startdate, portfolio_size=portfolio_size)
    
    # Buy initial portfolio
    rebalancer.rebalance_positions(
        data=data, date=startdate, composition=initial_portfolio_composition)

    # Init backtest dates and data
    candle_date = data[startdate:enddate].index.values
    next_rebalancing_date = candle_date[0] + rebalancing_cadence
    date_totals = {}
    portfolio_composition = initial_portfolio_composition

    # Backtest
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
    if plot:
        plt.show(block=True)

if __name__ == '__main__':

    log.info("Backtest starting...")
    prices.db.connect()

    log.info("Collecting and preparing data...")
    combined_df = prices.get_all_data_combined()
    data = prices.convert_combined_df_to_usdt(combined_df)
    log.debug("Data USD values data frame: \n {}".format(data.tail(5)))

    log.info("Testing...")
    broker.QUOTE_AMOUNT = 3500
    backtest(
        data=data,
        startdate='2017-01-01',
        enddate='2017-12-01',
        rebalancing=True, 
        plot=True)