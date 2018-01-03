import pandas as pd
from logger import log
import broker


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
            data=data, date=date, base=base, base_sell_amount=sell_amount_base)

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


def rebalance_positions(data: pd.DataFrame(), date: str,
                        composition: pd.DataFrame()):

    # positions to dataframe
    # extend with composition weights, add new coins from composition
    # calculate new distribution targets
    # rebalance

    # get old portfolio value
    final_total_value, portfolio_values = broker.get_value_at(data, date)
    # print("Portfolio values at {}: \n {}".format(date, portfolio_values))
    # print("Total portfolio value: {}".format(round(final_total_value, 2)))

    # sell everything not in composition
    old_symbols_with_amount = portfolio_values.loc[
        portfolio_values['base_amount'] > 0]
    old_symbols = set(old_symbols_with_amount.index.values)
    new_symbols = set(composition.index.values)
    symbols_to_sell_set = old_symbols - new_symbols
    symbols_to_sell_dfr = portfolio_values.loc[symbols_to_sell_set]
    for symbol, row in symbols_to_sell_dfr.iterrows():
        #print(symbol, row['base_amount'])
        broker.sell(
            data=data,
            date=date,
            base=symbol,
            base_sell_amount=row['base_amount'])
    final_total_value, portfolio_values = broker.get_value_at(data, date)
    # print("Portfolio values at {}: \n {}".format(date, portfolio_values))
    # print("Total portfolio value: {}".format(round(final_total_value, 2)))
    # print("Total USD: {}".format(round(broker.QUOTE_AMOUNT, 2)))

    # Merge old and new composition
    new_portfolio = pd.concat(
        [portfolio_values, composition], axis=1).fillna(0)

    # calculate full usd portfolio value
    total_quote_amount = broker.QUOTE_AMOUNT + new_portfolio['USDT_amount'].sum(
    )
    # print(total_quote_amount)

    # calculate target amount for symbols
    new_portfolio['target_quote_amount'] = new_portfolio[
        'basic_weight'] * total_quote_amount
    new_portfolio[
        'target_quote_change'] = new_portfolio['target_quote_amount'] - new_portfolio['USDT_amount']

    # split portfolio data frame in buy and sell parts
    symbols_to_decrease = new_portfolio.loc[
        new_portfolio['target_quote_change'] < 0]

    symbols_to_increase = new_portfolio.loc[
        new_portfolio['target_quote_change'] > 0]

    # Sell surplus
    for base, row in symbols_to_decrease.iterrows():
        # Find out how much base to sell
        price = row['BASE/USDT']
        sell_amount_base = abs(row['target_quote_change']) / price
        #print(base, row['distribution_change_USDT'], price, sell_amount_base)
        # Sell
        broker.sell(
            data=data, date=date, base=base, base_sell_amount=sell_amount_base)

    # Turn off chained assignment warning for pandas
    # https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
    pd.options.mode.chained_assignment = None

    # how much should be redistributed
    total_amount_to_distribute = broker.QUOTE_AMOUNT
    # print(total_amount_to_distribute)

    # Buy to rebalance
    total_increase_value = symbols_to_increase['target_quote_change'].sum()
    symbols_to_increase['increase_distr_perc'] = symbols_to_increase[
        'target_quote_change'] / total_increase_value
    symbols_to_increase['buy_amount'] = symbols_to_increase[
        'increase_distr_perc'] * total_amount_to_distribute
    # print("symbols_to_increase: \n {}".format(symbols_to_increase))
    for base, row in symbols_to_increase.iterrows():
        broker.buy(
            data=data,
            date=date,
            base=base,
            quote_buy_amount=row['buy_amount'])

    # get portfolio value
    final_total_value, portfolio_values = broker.get_value_at(data, date)
    # portfolio_values[
    #     'distribution_change_USDT'] = even_usd_value_distribution - portfolio_values['USDT_amount']
    # portfolio_values['distribution_perc'] = portfolio_values[
    #     'USDT_amount'] / final_total_value
    # print("Portfolio values at {}: \n {}".format(date, portfolio_values))
    # print("Total portfolio value: {}".format(round(final_total_value, 2)))
    # print("Free quote amount: {}".format(round(broker.QUOTE_AMOUNT, 2)))