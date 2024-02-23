import datetime
import logging
import time

import utils


class MainHandlerFilter(logging.Filter):
    def filter(self, record):
        return record.levelno > logging.DEBUG


class DebugHandlerFilter(logging.Filter):
    def filter(self, record):
        return record.levelno <= logging.DEBUG


asset = None
use_real_account = None
amount_per_order = None
target_player_number = None
player_amount_position = None
account_number = None
agent_number = None
operation_ongoing = False
player_position_when_operation_started = None
log_file_name = f"{datetime.datetime.now().strftime('%Y-%m-%d')}"
log_file_path = utils.get_dir_path('logs\\' + log_file_name)
logging_main_handler = logging.FileHandler(log_file_path + '.log')
logging_main_handler.setLevel(logging.INFO)
logging_main_handler.addFilter(MainHandlerFilter())
logging_debug_handler = logging.FileHandler(log_file_path + '_players_position.log')
logging_debug_handler.setLevel(logging.DEBUG)
logging_debug_handler.addFilter(DebugHandlerFilter())
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
debug_formatter = logging.Formatter('%(asctime)s - %(message)s')
logging_main_handler.setFormatter(formatter)
logging_debug_handler.setFormatter(debug_formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging_main_handler)
logger.addHandler(logging_debug_handler)
import nelogica_data_feed_api


def main():
    try:
        global asset, operation_ongoing
        logger.info('Starting app...')
        start_time = datetime.datetime.now().replace(hour=8, minute=59, second=0, microsecond=0)
        end_time_hour_min = utils.read_from_file('configs.txt', 'operations_end_time')
        hour = int(end_time_hour_min[0: end_time_hour_min.find('h')])
        minute = int(end_time_hour_min[end_time_hour_min.find('h') + 1: end_time_hour_min.find('m')])
        end_time = datetime.datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        asset = utils.read_from_file('configs.txt', 'asset').strip().upper()
        init = True
        disconnect_dll = False
        while True:
            time.sleep(1)
            while start_time < datetime.datetime.now() < end_time:
                loop_start_time = datetime.datetime.now()
                if init:
                    nelogica_data_feed_api.init_dll_and_subscribe(asset, 'F')
                    disconnect_dll = True
                    load_properties()
                    init = False
                else:
                    if operation_ongoing:
                        process_operation_end()
                    else:
                        operation_start_trigger()
                time_elapsed = (datetime.datetime.now() - loop_start_time).total_seconds()
                sleep_time = 0 if time_elapsed >= 1 else 1 - time_elapsed
                time.sleep(sleep_time)
                players_position_log()
            if operation_ongoing:
                close_ongoing_operation()
            if disconnect_dll:
                nelogica_data_feed_api.dll_disconnect()
                logger.info('App finished!')
                disconnect_dll = False
    except Exception as e:
        logger.error('Error when running main method with: ' + str(e))


def load_properties():
    global use_real_account, amount_per_order, target_player_number, player_amount_position, account_number, agent_number
    use_real_account = utils.str_to_bool(utils.read_from_file('configs.txt', 'use_real_account'))
    amount_per_order = int(utils.read_from_file('configs.txt', 'quantity').strip())
    target_player_number = int(utils.read_from_file('configs.txt', 'target_player_number').strip())
    player_amount_position = int(utils.read_from_file('configs.txt', 'player_quantity_position').strip())
    if use_real_account:
        account_number = utils.read_from_file('configs.txt', 'real_account').strip()
        agent_number = utils.read_from_file('configs.txt', 'real_account_agent').strip()
    else:
        account_number = utils.read_from_file('configs.txt', 'sim_account').strip()
        agent_number = utils.read_from_file('configs.txt', 'sim_account_agent').strip()


def operation_start_trigger():
    try:
        global player_amount_position, operation_ongoing, player_position_when_operation_started
        if operation_ongoing is False and target_player_number in nelogica_data_feed_api.players_position:
            target_player_position = nelogica_data_feed_api.players_position[target_player_number]
            print('target_player_position:', target_player_position)
            if abs(target_player_position) >= player_amount_position:
                operation_ongoing = True
                logger.info(f'Starting operation as current player position is: {target_player_position}')
                player_position_when_operation_started = target_player_position
                if target_player_position > 0:
                    # sending sell price 1% lower, to make sure the order will be executed at the best buy price
                    price = int(nelogica_data_feed_api.tickers_last_price[asset])
                    logger.info('current asset price is: ' + str(price))
                    result = nelogica_data_feed_api.send_market_sell_order(account_number, agent_number, asset,
                                                                           amount_per_order,
                                                                           'F')
                else:
                    # sending buy price 1% higher, to make sure the order will be executed at the best sell price
                    price = int(nelogica_data_feed_api.tickers_last_price[asset])
                    logger.info('current asset price is: ' + str(price))
                    result = nelogica_data_feed_api.send_market_buy_order(account_number, agent_number, asset,
                                                                          amount_per_order, 'F')
    except Exception as e:
        logger.error('Error when processing operation start trigger with: ' + str(e))


def process_operation_end():
    try:
        global operation_ongoing, player_position_when_operation_started, asset
        if operation_ongoing and target_player_number in nelogica_data_feed_api.players_position:
            target_player_position = nelogica_data_feed_api.players_position[target_player_number]
            print('target_player_position:', target_player_position)
            if player_changed_side(target_player_position):
                logger.info(f'Closing operation as player changed its operation side')
                close_ongoing_operation(target_player_position)
    except Exception as e:
        logger.error('Error when processing operation end with: ' + str(e))


def close_ongoing_operation(target_player_position=None):
    try:
        global operation_ongoing, player_position_when_operation_started, asset
        if target_player_position is None:
            target_player_position = nelogica_data_feed_api.players_position[target_player_number]
        logger.info(f'Closing operation. Current player position is: {target_player_position}')
        if player_position_when_operation_started is not None:
            if player_position_when_operation_started > 0:
                # sending buy price 1% higher, to make sure the order will be executed at the best sell price
                price = int(nelogica_data_feed_api.tickers_last_price[asset])
                logger.info('current asset price is: ' + str(price))
                nelogica_data_feed_api.send_market_buy_order(account_number, agent_number, asset, amount_per_order,
                                                             'F')
            else:
                # sending sell price 1% lower, to make sure the order will be executed at the best buy price
                price = int(nelogica_data_feed_api.tickers_last_price[asset])
                logger.info('current asset price is: ' + str(price))
                nelogica_data_feed_api.send_market_sell_order(account_number, agent_number, asset, amount_per_order,
                                                              'F')
            operation_ongoing = False
        else:
            logger.error('Could not close ongoing operation. player_position_when_operation_started is None')
    except Exception as e:
        logger.error('Error when processing close_ongoing_operation with: ' + str(e))


def player_changed_side(current_player_position):
    global player_position_when_operation_started
    if player_position_when_operation_started is not None:
        if ((player_position_when_operation_started > 0 and current_player_position <= 0) or
                (player_position_when_operation_started < 0 and current_player_position >= 0)):
            return True
        else:
            return False
    else:
        logger.error('Error as player_position_when_operation_started is none')
        return False


def players_position_log():
    try:
        players_position = nelogica_data_feed_api.players_position.copy()
        players_position = dict(sorted(players_position.items(), key=lambda item: item[1], reverse=True))
        if players_position is not None and len(players_position) > 0:
            for key, value in players_position.items():
                logger.debug('player: ' + str(key) + ' position: ' + str(value))
            logger.debug('---------------------------------------------------------------------')
    except Exception as e:
        logger.error('Error when processing players_position_log with: ' + str(e))


if __name__ == "__main__":
    main()
