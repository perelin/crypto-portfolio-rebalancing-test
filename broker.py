import pandas as pd
from logger import log


def sell(data: pd.DataFrame(), date: str, base: str, base_sell_amount: float,
         QUOTE_AMOUNT: float, POSITIONS: dict):
    ''' Simulate selling '''
    #global QUOTE_AMOUNT, POSITIONS

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

    return QUOTE_AMOUNT, POSITIONS


def buy(data: pd.DataFrame(), date: str, base: str, quote_buy_amount: float,
        QUOTE_AMOUNT: float, POSITIONS: dict):

    ''' Simulate buying '''
    #global QUOTE_AMOUNT, POSITIONS

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
    return QUOTE_AMOUNT, POSITIONS
