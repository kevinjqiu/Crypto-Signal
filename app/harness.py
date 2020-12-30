from conf import Configuration
import traceback
import os
import structlog
import time
from multiprocessing import Pool
from analysis import StrategyAnalyzer
from exchange import ExchangeInterface
from ccxt import ExchangeError
from tenacity import RetryError
import datetime
from decimal import Decimal
import psycopg2


logger = structlog.get_logger()


historical_data_cache = dict()

def _get_indicator_results(config, exchange, market_pair):
    strategy_analyzer = StrategyAnalyzer()
    indicator_dispatcher = strategy_analyzer.indicator_dispatcher()
    indicator_confs = config.indicators
    # indicator_confs = Configuration().indicators

    results = { indicator: list() for indicator in indicator_confs.keys() }
    for indicator in indicator_confs:
        if indicator not in indicator_dispatcher:
            logger.warn("No such indicator %s, skipping.", indicator)
            continue

        for indicator_conf in indicator_confs[indicator]:
            if indicator_conf['enabled']:
                candle_period = indicator_conf['candle_period']
            else:
                logger.debug("%s is disabled, skipping.", indicator)
                continue

            if candle_period not in historical_data_cache:
                historical_data_cache[candle_period] = _get_historical_data(
                    config,
                    market_pair,
                    exchange,
                    candle_period
                )

            if historical_data_cache[candle_period]:
                analysis_args = {
                    'historical_data': historical_data_cache[candle_period],
                    'signal': indicator_conf['signal'],
                    'hot_thresh': indicator_conf['hot'],
                    'cold_thresh': indicator_conf['cold']
                }

                if 'period_count' in indicator_conf:
                    analysis_args['period_count'] = indicator_conf['period_count']

                results[indicator].append({
                    'result': _get_analysis_result(
                        indicator_dispatcher,
                        indicator,
                        analysis_args,
                        market_pair
                    ),
                    'config': indicator_conf
                })
    return results


def _get_analysis_result(dispatcher, indicator, dispatcher_args, market_pair):
    try:
        results = dispatcher[indicator](**dispatcher_args)
    except TypeError:
        logger.info(
            'Invalid type encountered while processing pair %s for indicator %s, skipping',
            market_pair,
            indicator
        )
        logger.info(traceback.format_exc())
        results = str()
    return results


def _get_historical_data(config, market_pair, exchange, candle_period):
    exchange_interface = ExchangeInterface(config.exchanges)

    historical_data = list()
    try:
        historical_data = exchange_interface.get_historical_data(
            market_pair,
            exchange,
            candle_period
        )
    except RetryError:
        logger.error(
            'Too many retries fetching information for pair %s, skipping',
            market_pair
        )
    except ExchangeError:
        logger.error(
            'Exchange supplied bad data for pair %s, skipping',
            market_pair
        )
    except ValueError as e:
        logger.error(e)
        logger.error(
            'Invalid data encountered while processing pair %s, skipping',
            market_pair
        )
        logger.debug(traceback.format_exc())
    except AttributeError:
        logger.error(
            'Something went wrong fetching data for %s, skipping',
            market_pair
        )
        logger.debug(traceback.format_exc())
    return historical_data


def _get_pnl(exchange, market_pair):
    result = {
        '1m': {
            'data': None,
            'amount': None,
            'percent': None,
        },
        '5m': {
            'data': None,
            'amount': None,
            'percent': None,
        }
    }

    for interval in ('1m', '5m'):
        historical_data = historical_data_cache[interval]
        # historical_data = self._get_historical_data(
        #         market_pair,
        #         exchange,
        #         interval,
        #     )
        result[interval]['data'] = historical_data
        if len(historical_data) == 0:
            continue
        opening_price = historical_data[-1][1]
        closing_price = historical_data[-1][4]
        result[interval]['amount'] = closing_price - opening_price
        result[interval]['percent'] = 100 * closing_price / opening_price
    return result


def _test_strategies(config, exchange, market_pair):
    new_result = dict()

    logger.info("Beginning analysis of %s", market_pair)
    new_result['indicators'] = _get_indicator_results(
        config,
        exchange,
        market_pair
    )

    # new_result['informants'] = self._get_informant_results(
    #     exchange,
    #     market_pair
    # )

    # new_result['crossovers'] = self._get_crossover_results(
    #     new_result[exchange][market_pair]
    # )

    new_result['yield'] = _get_pnl(exchange, market_pair)
    return (exchange, market_pair, new_result)


def run(args):
    config, exchange, market_pair = args
    logger = structlog.get_logger()
    logger.info('Processing %s-%s' % (exchange, market_pair))

    new_result = _test_strategies(config, exchange, market_pair)
    return new_result


class Harness():
    def __init__(self, config, exchange_interface, notifier):
        self.logger = structlog.get_logger()
        self.config = config
        self.exchange_interface = exchange_interface
        self.notifier = notifier

    def _get_filtered_market_pairs(self, market_data, requested_market_pairs):
        ret = []
        for exchange in market_data:
            for pair in market_data[exchange]:
                base, quote = pair.split('/')

                if not requested_market_pairs:
                    ret.append((exchange, pair))
                    continue

                for requested_market_pair in requested_market_pairs:
                    if base == requested_market_pair or quote == requested_market_pair:
                        ret.append((exchange, pair))
        return ret

    def run(self):
        market_pairs = self.config.settings['market_pairs']
        market_data = self.exchange_interface.get_exchange_markets(markets=market_pairs)

        filtered_market_pairs = self._get_filtered_market_pairs(market_data, market_pairs)

        while True:
            self.logger.info('Starting harness...')

            args = []
            for exchange, market_pair in filtered_market_pairs:
                args.append((self.config, exchange, market_pair))

            with Pool(os.cpu_count() * 2 + 1) as p:
                results = p.map(run, args)

            for exchange, market_pair, result in results:
                to_postgres(result, exchange, market_pair)
                
            self.logger.info("Sleeping for %s seconds", self.config.settings['update_interval'])
            time.sleep(self.config.settings['update_interval'])


def to_postgres(results, exchange, market_pair):
    logger.info('Writing results to database for %s-%s' % (exchange, market_pair))
    d = lambda v: Decimal(str(v))

    def get_indicator(indicator, idx, signal):
        try:
            return list(results['indicators'][indicator][idx]['result'].to_dict()[signal].items())[-1][1]
        except IndexError:
            return None

    yield_result = results['yield']

    conn = psycopg2.connect("host=192.168.1.101 dbname=crypto user=postgres password=password")
    try:
        analysis_id = int(datetime.datetime.utcnow().timestamp() * 1000)
        base, quote = market_pair.split('/')

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

            if all(v is None for v in args):
                return

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