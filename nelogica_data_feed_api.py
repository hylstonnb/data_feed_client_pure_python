import time
from ctypes import *

import utils
import logging

# Error Codes
NL_ERR_INIT = 80
NL_OK = 0
NL_ERR_INVALID_ARGS = 90
NL_ERR_INTERNAL_ERROR = 100
is_active = False
is_broker_connected = False
is_market_connected = False
is_login_connected = False
path = utils.get_dir_path('ProfitDLL64.dll')
profit_dll = WinDLL(path)
configs_file_path = utils.get_dir_path("configs.txt")
broker_id = utils.read_from_file(configs_file_path, "broker_id")
broker_account_id = utils.read_from_file(configs_file_path, "broker_account_id")
routing_password = utils.read_from_file(configs_file_path, "routing_key")
nelogica_data_feed_key = utils.read_from_file(configs_file_path, "data_feed_key")
nelogica_acc_login = utils.read_from_file(configs_file_path, "nelogica_account_login")
nelogica_acc_password = utils.read_from_file(configs_file_path, "nelogica_account_password")
is_progress_finished = False
win_fut_ticker_state = -1
players_position = {}
tickers_last_price = {}


class OrderChange:
    stock_code = str
    broker_code: int
    qtd: int
    trade_qtd: int
    leaves_qtd: int
    side: int
    price: float
    stop_price: float
    avg_price: float
    profit_id: int
    order_type: int
    account: int
    account_holder: str
    order_id: int
    status: str
    date: str
    message: str


class TAssetID(Structure):
    _fields_ = [("ticker", c_wchar_p),
                ("bolsa", c_wchar_p),
                ("feed", c_int)]


# BEGIN DEF
@WINFUNCTYPE(None, c_int32, c_int32)
def state_callback(nType, nResult):
    try:
        global is_active
        global is_market_connected
        global is_login_connected
        global is_broker_connected

        nConnStateType = nType
        result = nResult

        if nConnStateType == 0:  # notificacoes de login
            # not working currently
            if result == 0:
                is_login_connected = True
                print("Login: conectado")
            else:
                is_login_connected = False
                print('Login: ' + str(result))
        elif nConnStateType == 1:
            if result == 5:
                # bBrokerConnected = True
                print("Broker: Conectado.")
                is_broker_connected = True
            elif result > 2:
                # bBrokerConnected = False
                print("Broker: Sem conexão com corretora.")
                is_broker_connected = False
            else:
                # bBrokerConnected = False
                print("Broker: Sem conexão com servidores (" + str(result) + ")")
                is_broker_connected = False
        elif nConnStateType == 2:  # notificacoes de login no Market
            if result == 4:
                print("Market: Conectado")
                is_market_connected = True
            else:
                print("Market: " + str(result))
                is_market_connected = False

        elif nConnStateType == 3:  # notificacoes de login
            if result == 0:
                print("Ativacao: OK")
                is_active = True
            else:
                print("Ativacao: " + str(result))
                is_active = False

        if is_market_connected and is_active and is_login_connected:
            print("Servicos Conectados")
    except Exception as e:
        logging.error("Exception occurred on method state_callback with " + str(e))


@WINFUNCTYPE(None, POINTER(c_int), POINTER(c_int), c_int, c_int, c_int, c_int, c_int, c_int, c_float, c_float, c_float,
             c_longlong, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int))
def history_callback(ticker, bolsa, feed, broker_code, nQtd, nTradedQtd, nLeavesQtd, side, sPrice, sStopPrice,
                     sAvgPrice, nProfitID, order_type, account, account_holder, order_id, status, date):
    return


@WINFUNCTYPE(None, TAssetID, c_wchar_p, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double,
             c_double, c_double,
             c_int, c_int, c_int, c_int, c_int, c_int, c_int)
def new_daily_callback(tAssetIDRec, date, sOpen, sHigh, sLow, sClose, sVol, sAjuste, sMaxLimit, sMinLimit, sVolBuyer,
                       sVolSeller, nQtd, nNegocios, nContratosOpen, nQtdBuyer, nQtdSeller, nNegBuyer, nNegSeller):
    return


@WINFUNCTYPE(None, TAssetID, c_int, c_int, c_int, c_int, c_int, c_float, POINTER(c_int), POINTER(c_int))
def priceBookCallback(asset_id, nAction, nPosition, Side, nQtd, nCount, sPrice, pArraySell, pArrayBuy):
    return


@WINFUNCTYPE(None, TAssetID, c_int, c_int, c_int, c_int, c_int, c_longlong, c_double, c_int, c_int, c_int, c_int,
             c_int, c_wchar_p, POINTER(c_int), POINTER(c_int))
def offerBookCallback(asset_id, action, position, side, qtd, agent, offer_id, price, has_price,
                      has_qtd, has_date, has_offer_id, has_agent, date, p_array_sell, p_array_buy):
    return


@WINFUNCTYPE(None, TAssetID, c_double, c_int, c_int)
def tiny_book_callback(asset, price, amount, side):
    return


@WINFUNCTYPE(None, TAssetID, c_int, c_int, c_int, c_int, c_int, c_double, c_double, c_double, c_longlong, c_wchar_p,
             c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p, c_wchar_p)
def order_change_callback(asset, agent, nQtd, nTradedQtd, nLeavesQtd, side, sPrice, sStopPrice, sAvgPrice,
                          nProfitID, tipoOrdem, conta, titular, clOrdID, status, date, textMessage):
    try:
        order_change = OrderChange()
        order_change.stock_code = asset.ticker
        order_change.broker_code = agent
        order_change.qtd = nQtd
        order_change.trade_qtd = nTradedQtd
        order_change.leaves_qtd = nLeavesQtd
        order_change.side = side
        order_change.price = sPrice
        order_change.stop_price = sStopPrice
        order_change.avg_price = sAvgPrice
        order_change.profit_id = nProfitID
        order_change.order_id = clOrdID
        order_change.status = status
        order_change.date = date
        order_change.message = textMessage
    except Exception as e:
        logging.error("Exception occurred on method order_change_callback with " + str(e))


@WINFUNCTYPE(None, c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int))
def account_callback(broker, broker_name, broker_account_id, account_holder):
    try:
        # if False:
        print("accountCallback")
        print("Broker:", broker)
        if broker_name is not None:
            print("Broker name:", wstring_at(broker_name))
        if broker_account_id is not None:
            print("Account id:", wstring_at(broker_account_id))
        if account_holder is not None:
            print("Account holder:", wstring_at(account_holder))
    except Exception as e:
        logging.error("Exception occurred on method account_callback with " + str(e))


@WINFUNCTYPE(None, TAssetID, c_wchar_p, c_uint, c_double, c_double, c_int, c_int, c_int, c_int, c_wchar)
def new_trade_callback(asset_id, date, trade_number, price, vol, qtd, buy_agent, sell_agent, trade_type, bIsEdit):
    try:
        global tickers_last_price
        if buy_agent in players_position:
            players_position[buy_agent] += qtd
        else:
            players_position[buy_agent] = qtd
        if sell_agent in players_position:
            players_position[sell_agent] -= qtd
        else:
            players_position[sell_agent] = -qtd
        asset = wstring_at(TAssetID.from_param(asset_id).ticker)
        tickers_last_price[asset] = price
    except Exception as e:
        logging.warning('Error when processing new_trade_callback with: ' + str(e))


@WINFUNCTYPE(None, TAssetID, c_int)
def progress_callback(assetId, nProgress):
    try:
        print(
            "Progress is: " + str(nProgress) + " for assetId: " + str(wstring_at(TAssetID.from_param(assetId).ticker)))
    except Exception as e:
        print('Error when processing progress_callback with: ' + str(e))


@WINFUNCTYPE(None, TAssetID, c_wchar_p, c_uint, c_double, c_double, c_int, c_int, c_int, c_int)
def trade_history_callback(asset_id, date, trade_number, price, vol, qtd, buy_agent, sell_agent, trade_type):
    return


@WINFUNCTYPE(None, TAssetID, c_wchar_p, c_int)
def change_state_ticker_callback(asset_id, date, state):
    try:
        global ticker_state_dict
        ticker: str = wstring_at(TAssetID.from_param(asset_id).ticker)
        ticker_state_dict[ticker] = state
    except Exception as e:
        logging.warning('Error when processing change_state_ticker_callback with: ' + str(e))


def wait_login():
    time.sleep(10)
    profit_dll.SetChangeStateTickerCallback(change_state_ticker_callback)
    profit_dll.SendSellOrder.restype = c_longlong
    profit_dll.SendBuyOrder.restype = c_longlong
    profit_dll.SendStopBuyOrder.restype = c_longlong
    profit_dll.SendStopSellOrder.restype = c_longlong
    profit_dll.SendZeroPosition.restype = c_longlong
    profit_dll.GetAgentNameByID.restype = c_wchar_p
    profit_dll.GetAgentShortNameByID.restype = c_wchar_p
    profit_dll.GetPosition.restype = POINTER(c_int)
    profit_dll.SendCancelOrder.restype = c_longlong
    print("DLL Connected")
    return True


def wait_broker_login():
    time.sleep(10)


def dll_initialize():
    try:
        init_status = profit_dll.DLLInitializeLogin(c_wchar_p(nelogica_data_feed_key), c_wchar_p(nelogica_acc_login),
                                                    c_wchar_p(nelogica_acc_password), state_callback,
                                                    history_callback, order_change_callback,
                                                    account_callback, new_trade_callback, new_daily_callback,
                                                    priceBookCallback, offerBookCallback, trade_history_callback,
                                                    progress_callback, tiny_book_callback)
        if init_status == NL_OK:
            return wait_login()
        else:
            print("Error when trying to initialize dll with error code = " + str(init_status))
            return False
    except Exception as e:
        print(str(e))


# bolsa F or B
def init_dll_and_subscribe(ticker, bolsa):
    if dll_initialize():
        profit_dll.SubscribeTicker(c_wchar_p(ticker), c_wchar_p(bolsa))
        logging.info(f"Subscribed for the ticker: {ticker}")
    else:
        logging.warning(f"Error when initializing ddl and subscribing for ticker: {ticker}")


# bolsa F or B
def subscribe_ticker(ticker, bolsa):
    try:
        profit_dll.SubscribeTicker(c_wchar_p(ticker), c_wchar_p(bolsa))
        print("subscribed for the ticker:", ticker)
    except Exception as e:
        logging.error('Error when processing subscribe_ticker with: ' + str(e))


# bolsa F or B
def send_sell_order(account_number, agent_number, ticker, price, amount, bolsa):
    try:
        logging.info(
            f'Sending sell order for the asset: {ticker} price: {price} amount: {amount} account: {account_number}')
        order_id = profit_dll.SendSellOrder(account_number, str(agent_number), routing_password,
                                            ticker, bolsa, c_double(price), amount)
        return order_id
    except Exception as e:
        logging.error("Send sell order failed with: " + str(e))


# bolsa F or B
def send_buy_order(account_number, agent_number, ticker, price, amount, bolsa):
    try:
        logging.info(
            f'Sending buy order for the asset: {ticker} price: {price} amount: {amount} account: {account_number}')
        profit_order_id = profit_dll.SendBuyOrder(account_number, str(agent_number), routing_password,
                                                  ticker, bolsa, c_double(price), amount)
        return profit_order_id
    except Exception as e:
        logging.error("Send buy order failed with: " + str(e))


def get_account():
    profit_dll.GetAccount()
