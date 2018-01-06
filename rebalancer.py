import pandas as pd
from logger import log
import broker

def rebalance_positions(data: pd.DataFrame(), date: str,
                        composition: pd.DataFrame()):

    # get old portfolio value
    final_total_value, portfolio_values = broker.get_value_at(data, date)

    # sell everything not in composition
    old_symbols_with_amount = portfolio_values.loc[
        portfolio_values['base_amount'] > 0]
    old_symbols = set(old_symbols_with_amount.index.values)
    new_symbols = set(composition.index.values)
    symbols_to_sell_set = old_symbols - new_symbols
    symbols_to_sell_dfr = portfolio_values.loc[symbols_to_sell_set]
    for symbol, row in symbols_to_sell_dfr.iterrows():
        broker.sell(
            data=data,
            date=date,
            base=symbol,
            base_sell_amount=row['base_amount'])
    final_total_value, portfolio_values = broker.get_value_at(data, date)

    # Merge old and new composition
    new_portfolio = pd.concat(
        [portfolio_values, composition], axis=1).fillna(0)

    # calculate full usd portfolio value
    total_quote_amount = broker.QUOTE_AMOUNT + new_portfolio['USDT_amount'].sum(
    )

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
        # Sell
        broker.sell(
            data=data, date=date, base=base, base_sell_amount=sell_amount_base)

    # Turn off chained assignment warning for pandas
    # https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
    pd.options.mode.chained_assignment = None

    # how much should be redistributed
    total_amount_to_distribute = broker.QUOTE_AMOUNT

    # Buy to rebalance
    total_increase_value = symbols_to_increase['target_quote_change'].sum()
    symbols_to_increase['increase_distr_perc'] = symbols_to_increase[
        'target_quote_change'] / total_increase_value
    symbols_to_increase['buy_amount'] = symbols_to_increase[
        'increase_distr_perc'] * total_amount_to_distribute
    for base, row in symbols_to_increase.iterrows():
        broker.buy(
            data=data,
            date=date,
            base=base,
            quote_buy_amount=row['buy_amount'])