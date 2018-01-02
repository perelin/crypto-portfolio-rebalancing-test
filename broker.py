import pandas as pd
from logger import log

POSITIONS = {}
QUOTE_AMOUNT = 1000.0


def sell(data: pd.DataFrame(), date: str, base: str, base_sell_amount: float):
    ''' Simulate selling '''
    global QUOTE_AMOUNT, POSITIONS

    if base in POSITIONS:
        base_position = POSITIONS[base]
    else:
        log.warning("Trying to sell from not existing position. Breaking.")
        return

    # Check if enough base is available
    if base_position['base_amount'] < base_sell_amount:
        log.warning(
            "Not enough BASE_AMOUNT ({} {}) to sell inteded amount of {}".
            format(base_position['base_amount'], base, base_sell_amount))
        base_sell_amount = base_position['base_amount']

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

    log.debug(
        "Sold {} {} base for {} quote. Price was {}, fees {}, date {}".format(
            base_sell_amount, base, quote_amount.get(base), price[0], fees,
            date))


def buy(data: pd.DataFrame(), date: str, base: str, quote_buy_amount: float):
    ''' Simulate buying '''
    global QUOTE_AMOUNT, POSITIONS

    # Check if enough quote money is available
    if QUOTE_AMOUNT < quote_buy_amount:
        log.debug("Not enough QUOTE_AMOUNT ({}) to buy {} of {}".format(
            QUOTE_AMOUNT, quote_buy_amount, base))
        quote_buy_amount = QUOTE_AMOUNT

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

    log.debug("Bought {} {} base for {} quote. Price was {}, fees {}, date {}".
              format(
                  base_amount.get(base), base, quote_buy_amount, price[0],
                  fees, date))


def get_total_fees():
    total_fees = 0
    for base, row in POSITIONS.items():
        for trade in row['trades']:
            total_fees += trade['fees_in_quote']
    return total_fees


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