import datetime
import logging
import time

import utils
import nelogica_data_feed_api

asset = None
use_real_account = None
amount_per_order = None
target_player_number = None
player_amount_position = None
account_number = None
agent_number = None
operation_ongoing = False
player_position_when_operation_started = None


def main():
    log_file_name = f"{datetime.datetime.now().strftime('%Y-%m-%d')}.log"
    log_file_path = utils.get_dir_path('logs\\' + log_file_name)
    logging.basicConfig(filename=log_file_path,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.DEBUG)
    try:
        global asset, operation_ongoing
        logging.info('Starting application...')
        start_time = datetime.datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
        end_time_hour = utils.read_from_file('configs.txt', 'operations_end_time')
        hour = int(end_time_hour[0: end_time_hour.find('h')])
        minute = int(end_time_hour[end_time_hour.find('h') + 1: end_time_hour.find('m')])
        end_time = datetime.datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        asset = utils.read_from_file('configs.txt', 'asset')
        init = True
        while start_time < datetime.datetime.now() < end_time:
            if init:
                nelogica_data_feed_api.init_dll_and_subscribe(asset, 'F')
                load_properties()
                init = False
            else:
                if operation_ongoing:
                    process_operation_end()
                else:
                    operation_start_trigger()
            time.sleep(1)
        logging.info('Application finished!')
    except Exception as e:
        logging.error('Error when running main method with: ' + str(e))


def load_properties():
    global use_real_account, amount_per_order, target_player_number, player_amount_position, account_number, agent_number
    use_real_account = utils.str_to_bool(utils.read_from_file('configs.txt', 'use_real_account'))
    amount_per_order = int(utils.read_from_file('configs.txt', 'quantity'))
    target_player_number = int(utils.read_from_file('configs.txt', 'target_player_number'))
    player_amount_position = int(utils.read_from_file('configs.txt', 'player_quantity_position'))
    if use_real_account:
        account_number = utils.read_from_file('configs.txt', 'real_account')
        agent_number = utils.read_from_file('configs.txt', 'real_account_agent')
    else:
        account_number = utils.read_from_file('configs.txt', 'sim_account')
        agent_number = utils.read_from_file('configs.txt', 'sim_account_agent')


def operation_start_trigger():
    try:
        global operation_ongoing, player_position_when_operation_started
        if operation_ongoing is False and target_player_number in nelogica_data_feed_api.players_position:
            target_player_position = nelogica_data_feed_api.players_position[target_player_number]
            if player_amount_position >= abs(target_player_position):
                operation_ongoing = True
                logging.info(f'Starting operation as current player position is: {target_player_position}')
                player_position_when_operation_started = target_player_position
                if target_player_position > 0:
                    # sending sell price 1% lower, to make sure the order will be executed at the best buy price
                    price = int(nelogica_data_feed_api.tickers_last_price[asset] * 0.99)
                    nelogica_data_feed_api.send_sell_order(account_number, agent_number, asset, price, amount_per_order,
                                                           'F')
                else:
                    # sending buy price 1% higher, to make sure the order will be executed at the best sell price
                    price = int(nelogica_data_feed_api.tickers_last_price[asset] * 1.01)
                    nelogica_data_feed_api.send_buy_order(account_number, agent_number, asset, price, amount_per_order,
                                                          'F')
    except Exception as e:
        logging.error('Error when processing operation start trigger with: ' + str(e))


def process_operation_end():
    try:
        global operation_ongoing, player_position_when_operation_started, asset
        if operation_ongoing and target_player_number in nelogica_data_feed_api.players_position:
            target_player_position = nelogica_data_feed_api.players_position[target_player_number]
            if player_changed_side(target_player_position):
                logging.info(f'Closing operation as current player position is: {target_player_position}')
                player_position_when_operation_started = target_player_position
                if player_position_when_operation_started > 0:
                    # sending buy price 1% higher, to make sure the order will be executed at the best sell price
                    price = int(nelogica_data_feed_api.tickers_last_price[asset] * 1.01)
                    nelogica_data_feed_api.send_buy_order(account_number, agent_number, asset, price, amount_per_order,
                                                          'F')
                else:
                    # sending sell price 1% lower, to make sure the order will be executed at the best buy price
                    price = int(nelogica_data_feed_api.tickers_last_price[asset] * 0.99)
                    nelogica_data_feed_api.send_sell_order(account_number, agent_number, asset, price, amount_per_order,
                                                           'F')
                operation_ongoing = False
    except Exception as e:
        logging.error('Error when processing operation end with: ' + str(e))


def player_changed_side(current_player_position):
    global player_position_when_operation_started
    if player_position_when_operation_started is not None:
        if ((player_position_when_operation_started > 0 and current_player_position <= 0) or
                (player_position_when_operation_started < 0 and current_player_position >= 0)):
            return True
        else:
            return False
    else:
        logging.error('Error as player_position_when_operation_started is none')
        return False




if __name__ == "__main__":
    main()
