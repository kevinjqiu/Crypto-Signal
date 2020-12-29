""" Handles outputting results to the terminal.
"""

import json
import psycopg2
import datetime

import structlog

from decimal import Decimal


class Output():
    """ Handles outputting results to the terminal.
    """

    def __init__(self):
        """Initializes Output class.
        """

        self.logger = structlog.get_logger()
        self.dispatcher = {
            'cli': self.to_cli,
            'csv': self.to_csv,
            'json': self.to_json,
            'postgres': self.to_postgres,
        }

    def to_postgres(self, results, market_pair):
        d = lambda v: Decimal(str(v))

        def get_indicator(indicator, idx, signal):
            try:
                return list(results['indicators'][indicator][idx]['result'].to_dict()[signal].items())[-1][1]
            except IndexError:
                return None

        yield_result = results['yield']

        try:
            conn = psycopg2.connect("host=192.168.1.101 dbname=crypto user=postgres password=password")
            analysis_id = int(datetime.datetime.utcnow().timestamp() * 1000)
            base, quote = market_pair.split('/')
            exchange = 'binance'

            try:
                cur = conn.cursor()

                for interval in yield_result:
                    for ts, o, h, l, c, v in yield_result[interval]['data']:
                        ts = datetime.datetime.utcfromtimestamp(ts / 1e3)
                        cur.execute(
                            'INSERT INTO historical_data (time, exchange_id, base, quote, open, high, low, close, volume) '
                            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                            'ON CONFLICT ("time", exchange_id, base, quote) DO NOTHING;',
                            (ts, exchange, base, quote, d(o), d(h), d(l), d(c), d(v))
                        )
            finally:
                conn.commit()
                cur.close()

            try:
                cur = conn.cursor()

                args = [
                    datetime.datetime.utcnow(),
                    analysis_id,
                    exchange,
                    base,
                    quote,
                ]

                args.extend([
                    get_indicator('momentum', 0, 'momentum'),
                    get_indicator('momentum', 1, 'momentum'),

                    get_indicator('mfi', 0, 'mfi'),
                    get_indicator('mfi', 1, 'mfi'),

                    get_indicator('obv', 0, 'obv'),
                    get_indicator('obv', 1, 'obv'),

                    get_indicator('rsi', 0, 'rsi'),
                    get_indicator('rsi', 1, 'rsi'),

                    get_indicator('stoch_rsi', 0, 'stoch_rsi'),
                    get_indicator('stoch_rsi', 1, 'stoch_rsi'),

                    get_indicator('macd', 0, 'macd'),
                    get_indicator('macd', 1, 'macd'),

                    get_indicator('ichimoku', 0, 'leading_span_a'),
                    get_indicator('ichimoku', 0, 'leading_span_b'),

                    get_indicator('ichimoku', 1, 'leading_span_a'),
                    get_indicator('ichimoku', 1, 'leading_span_b'),

                    yield_result['1m']['amount'],
                    yield_result['1m']['percent'],
                    
                    yield_result['5m']['amount'],
                    yield_result['5m']['percent'],
                ])

                cur.execute(
                    'INSERT INTO indicators ('
                    'time, analysis_id, exchange, base, quote, '
                    'momentum_1m, momentum_5m, '
                    'mfi_1m, mfi_5m, '
                    'obv_1m, obv_5m, '
                    'rsi_1m, rsi_5m, '
                    'stoch_rsi_1m, stoch_rsi_5m, '
                    'macd_1m, macd_5m, '
                    'ichimoku_a_1m, ichimoku_b_1m, '
                    'ichimoku_a_5m, ichimoku_b_5m, '
                    'profit_amount_1m, profit_percent_1m, '
                    'profit_amount_5m, profit_percent_5m) '
                    'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    args)
            finally:
                conn.commit()
                cur.close()
        finally:
            conn.close()

    def to_cli(self, results, market_pair):
        """Creates the message to output to the CLI

        Args:
            market_pair (str): Market pair that this message relates to.
            results (dict): The result of the completed analysis to output.

        Returns:
            str: Completed cli message
        """

        normal_colour = '\u001b[0m'
        hot_colour = '\u001b[31m'
        cold_colour = '\u001b[36m'

        output = "{}:\t\n".format(market_pair)
        if 'yield' in results:
            print('Yield %f' % results['yield'])
            del results['yield']

        for indicator_type in results:
            output += '\n{}:\t'.format(indicator_type)
            for indicator in results[indicator_type]:
                for i, analysis in enumerate(results[indicator_type][indicator]):
                    if analysis['result'].shape[0] == 0:
                        self.logger.info('No results for %s #%s', indicator, i)
                        continue

                    colour_code = normal_colour

                    if 'is_hot' in analysis['result'].iloc[-1]:
                        if analysis['result'].iloc[-1]['is_hot']:
                            colour_code = hot_colour

                    if 'is_cold' in analysis['result'].iloc[-1]:
                        if analysis['result'].iloc[-1]['is_cold']:
                            colour_code = cold_colour

                    if indicator_type == 'crossovers':
                        key_signal = '{}_{}'.format(
                            analysis['config']['key_signal'],
                            analysis['config']['key_indicator_index']
                        )

                        key_value = analysis['result'].iloc[-1][key_signal]

                        crossed_signal = '{}_{}'.format(
                            analysis['config']['crossed_signal'],
                            analysis['config']['crossed_indicator_index']
                        )

                        crossed_value = analysis['result'].iloc[-1][crossed_signal]

                        if isinstance(key_value, float):
                            key_value = format(key_value, '.8f')

                        if isinstance(crossed_value, float):
                            crossed_value = format(crossed_value, '.8f')

                        formatted_string = '{}/{}'.format(key_value, crossed_value)
                        output += "{}{}: {}{} \t".format(
                            colour_code,
                            '{} #{}'.format(indicator, i),
                            formatted_string,
                            normal_colour
                        )
                    else:
                        formatted_values = list()
                        for signal in analysis['config']['signal']:
                            value = analysis['result'].iloc[-1][signal]
                            if isinstance(value, float):
                                formatted_values.append(format(value, '.8f'))
                            else:
                                formatted_values.append(value)
                            formatted_string = '/'.join(formatted_values)

                        output += "{}{}: {}{} \t".format(
                            colour_code,
                            '{} #{}'.format(indicator, i),
                            formatted_string,
                            normal_colour
                        )

        output += '\n\n'
        return output


    def to_csv(self, results, market_pair):
        """Creates the csv to output to the CLI

        Args:
            market_pair (str): Market pair that this message relates to.
            results (dict): The result of the completed analysis to output.

        Returns:
            str: Completed CSV message
        """

        logger.warn('WARNING: CSV output is deprecated and will be removed in a future version')

        output = str()
        for indicator_type in results:
            for indicator in results[indicator_type]:
                for i, analysis in enumerate(results[indicator_type][indicator]):
                    value = str()

                    if indicator_type == 'crossovers':
                        key_signal = '{}_{}'.format(
                            analysis['config']['key_signal'],
                            analysis['config']['key_indicator_index']
                        )

                        key_value = analysis['result'].iloc[-1][key_signal]

                        crossed_signal = '{}_{}'.format(
                            analysis['config']['crossed_signal'],
                            analysis['config']['crossed_indicator_index']
                        )

                        crossed_value = analysis['result'].iloc[-1][crossed_signal]

                        if isinstance(key_value, float):
                            key_value = format(key_value, '.8f')

                        if isinstance(crossed_value, float):
                            crossed_value = format(crossed_value, '.8f')

                        value = '/'.join([key_value, crossed_value])
                    else:
                        for signal in analysis['config']['signal']:
                            value = analysis['result'].iloc[-1][signal]
                            if isinstance(value, float):
                                value = format(value, '.8f')

                    is_hot = str()
                    if 'is_hot' in analysis['result'].iloc[-1]:
                        is_hot = str(analysis['result'].iloc[-1]['is_hot'])

                    is_cold = str()
                    if 'is_cold' in analysis['result'].iloc[-1]:
                        is_cold = str(analysis['result'].iloc[-1]['is_cold'])

                    new_output = ','.join([
                        market_pair,
                        indicator_type,
                        indicator,
                        str(i),
                        value,
                        is_hot,
                        is_cold
                    ])

                    output += '\n{}'.format(new_output)

        return output


    def to_json(self, results, market_pair):
        """Creates the JSON to output to the CLI

        Args:
            market_pair (str): Market pair that this message relates to.
            results (dict): The result of the completed analysis to output.

        Returns:
            str: Completed JSON message
        """

        logger.warn('WARNING: JSON output is deprecated and will be removed in a future version')

        for indicator_type in results:
            for indicator in results[indicator_type]:
                for index, analysis in enumerate(results[indicator_type][indicator]):
                    results[indicator_type][indicator][index]['result'] = analysis['result'].to_dict(
                        orient='records'
                    )[-1]

        formatted_results = { 'pair': market_pair, 'results': results }
        output = json.dumps(formatted_results)
        output += '\n'
        return output
