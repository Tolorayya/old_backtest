import datetime
from eventGeneratorNewData import EventGenerator
from collections import deque
import random

class Account():
    def __init__(self, account_name):
        self.market = account_name[0]
        self.balance = account_name[1].copy()
        self.total = []
        self.operation = []
        self.trading_volume = 0
        
        
class Strategy():
    def __init__(self, leader_names, follower_name):
        self.orders = []
        self.leader_names = leader_names
        self.follower_name = follower_name
       
        
                                       
    def action(self, current_market_prices, ts):
        new_messages = []
        
        for leader_name in self.leader_names:
            if len(current_market_prices[leader_name]['bids']) == 0 or len(current_market_prices[self.follower_name]['bids']) == 0:
                return []
            
            buy_condition = False
            sell_condition = False
            
            if buy_condition:
                
                #по некоторым легаси-причинам стратегия отправляет 5 одинаковых ордеров. Это несущественно для дальнейшей разработки бэктеста для курса
                order = [Order(id_number = 0, side = 'BUY', price = 0, volume = 0, time_in_force = 'IOC', market_name = self.follower_name) for i in range(5)]
                new_messages.extend([Message(m_type = 'create', id_number = 0, order = order[i], creation_ts = ts) for i in range(5)])
                self.last_operation = {'side': 'BUY', 'price': midprice}
                self.last_ts = ts
            elif sell_condition:
                order = [Order(id_number = 0, side = 'SELL', price = 0, volume = 0, time_in_force = 'IOC', market_name = self.follower_name) for i in range(5)]
                new_messages.extend([Message(m_type = 'create', id_number = 0, order = order[i], creation_ts = ts) for i in range(5)])
                self.last_operation = {'side': 'SELL', 'price': midprice}
                self.last_ts = ts
        return new_messages
                                       
        
        
class Order():
    def __init__(self, id_number, side, price, volume, time_in_force, market_name):
        self.id = id_number
        self.side = side
        self.price = price
        self.volume = volume
        self.time_in_force = time_in_force
        self.market_name = market_name
        
class Message():
    def __init__(self, m_type, id_number, order, creation_ts):
        self.m_type = m_type
        self.id_number = id_number
        self.order = order
        self.creation_ts = creation_ts
        
class MarketEngine():
    def __init__(self, event_generator, comission = 0.0, delay = 0.1, \
                 market_names = [('hitbtc', 'BTCUSDT', 'btc', 'usdt'), ('cryptocom', 'BTC_USDT', 'btc', 'usdt')], \
                 accounts = [('hitbtc', {'btc':0, 'usdt':700, '-btc':-0.005}, ['btc', 'usdt', '-btc'])], strategies = [Strategy(leader_names = [('cryptocom', 'BTC_USDT', 'orderbook')], follower_name = ('hitbtc', 'BTCUSDT', 'orderbook'))]):
        self.event_generator = event_generator
        self.market_names = market_names
        self.current_market_prices = {(i[0], i[1], 'orderbook'): {'bids':[], 'asks':[]} for i in market_names}
        self.current_market_volumes = {(i[0], i[1], 'orderbook'): {'bids':[], 'asks':[]} for i in market_names}
        self.trade = None
        self.trade_eat_levels = 0
        
        self.currencies = accounts[0][2]
        self.accounts = {i[0]:Account(i) for i in accounts}
        self.strategies = strategies
        self.messages = []
        self.orders = []
        self.current_ts = None
        self.delay = delay
        self.comission = comission
        
    def trade_parser(self, event):
        '''
        self.current_ts = event['ts']
        self.trade = event['data']
        if self.trade[2] == 'SELL':
            for price, level in zip(self.current_market_prices[order.market_name]['asks'],\
                                         self.current_market_volumes[order.market_name]['asks']):
                if price <= 
        elif self.trade[2] == 'BUY':
        self.execute_orders()
        self.trade = None
        self.trade_eat_levels = 0
        '''
        pass
    
    def orderbook_parser(self, event):
        self.current_market_prices[event['name2']]['bids'] = [float(event['data'][i]) for i in range(0, 20, 2)]
        self.current_market_volumes[event['name2']]['bids'] = [float(event['data'][i]) for i in range(1, 20, 2)]
        self.current_market_prices[event['name2']]['asks'] = [float(event['data'][i]) for i in range(20, 40, 2)]
        self.current_market_volumes[event['name2']]['asks'] = [float(event['data'][i]) for i in range(20, 40, 2)]
        self.current_ts = event['ts']
        if event['name']['market'] == 'binance':
            pass
        elif event['name']['market'] == 'cryptocom':
            pass
        elif event['name']['market'] == 'ftx':
            pass
    
    def messages_to_orders(self):
        while True:
            if len(self.messages) == 0:
                break
            if self.messages[0].creation_ts + self.delay < self.current_ts:
                if self.messages[0].m_type == 'create':
                    #add rejection if not enough funds
                    self.create_order(self.messages[0])
                elif self.messages[0].m_type == 'cancel':
                    self.cancel_order(self.messages[0])
                del self.messages[0]
            else:
                break
        
    def create_order(self, message):
        self.orders.append(message.order)
                                       
    def cancel_order(self, message):
        pass

    def execute_order(self, order):
        traded = False
        
        coin = self.currencies[0]
        base = self.currencies[1]
        short_coin = self.currencies[2]
        
        if order.time_in_force == 'IOC':
            if order.side == 'BUY':
                #asks
                if order.volume > self.accounts[order.market_name[0]].balance[base]/order.price:
                    order.volume = 0
                for price, volume in zip(self.current_market_prices[order.market_name]['asks'],\
                                         self.current_market_volumes[order.market_name]['asks']):
                    if price < order.price and order.volume > 0:
                        v = min([order.volume, volume, self.accounts[order.market_name[0]].balance[base]/price])
                        volume -= v
                        order.volume -= v
                        self.accounts[order.market_name[0]].balance[base] -= v * price * (1 + self.comission)
                        self.accounts[order.market_name[0]].balance[coin] += v
                        self.accounts[order.market_name[0]].trading_volume += v
                        traded = True
                    else:
                        break
            
            elif order.side == 'SELL':
                #bids
                if order.volume > self.accounts[order.market_name[0]].balance[coin]:
                    order.volume = 0
                for price, volume in zip(self.current_market_prices[order.market_name]['bids'],\
                                         self.current_market_volumes[order.market_name]['bids']):
                    if price > order.price and order.volume > 0:
                        v = min([order.volume, volume, self.accounts[order.market_name[0]].balance[coin]])
                        volume -= v
                        order.volume -= v
                        self.accounts[order.market_name[0]].balance[base] += v * price * (1 - self.comission)
                        self.accounts[order.market_name[0]].balance[coin] -= v
                        self.accounts[order.market_name[0]].trading_volume += v
                        traded = True
                    else:
                        break 
        if order.volume < 0.00001 or order.time_in_force == 'IOC':
            order.volume = None
        return traded
            
    def execute_order_list(self):
        traded = False
        for order in self.orders:
            traded_ = self.execute_order(order)
            if not traded:
                traded = traded_
        self.orders = [order for order in self.orders if order.volume is not None]
        return traded
                                       
    def process(self):
        coin = self.currencies[0]
        base = self.currencies[1]
        short_coin = self.currencies[2]
        leader_coin = self.currencies[3]
        i = 0
        m_price = 0
        m_price_leader = 0
        last_max_pose = None
        for event in self.event_generator.event_list:
            if event['name']['type'] == 'orderbook' and event['name']['market'] in [n[0] for n in self.market_names]:
                if len(event['data']) < 40:
                    continue
                self.orderbook_parser(event)
            else:
                self.trade_parser(event)
            
            if event['name']['market'] == self.strategies[0].leader_names[0][0] and event['name']['symbol'] == self.strategies[0].leader_names[0][1] and event['name']['type'] == 'orderbook':
                m_price_leader = (self.current_market_prices[event['name2']]['asks'][0] + self.current_market_prices[event['name2']]['bids'][0])/2
                
            if event['name']['market'] == self.strategies[0].follower_name[0] and event['name']['symbol'] == self.strategies[0].follower_name[1] and event['name']['type'] == 'orderbook':
                i += 1
                #new_messages = strategy.action(self.current_market_prices, ts = self.current_ts)
                #self.messages.extend(new_messages)
                #self.messages_to_orders()
                #self.execute_order_list()
                traded = False
                for strategy in self.strategies:
                #if event['name']['market'] in [n[0] for n in strategy.leader_names] and event['name']['type'] == 'orderbook':
                    new_messages = strategy.action(self.current_market_prices, ts = self.current_ts)
                    self.messages.extend(new_messages)
                    self.messages_to_orders()
                    traded_ = self.execute_order_list()
                    if not traded:
                        traded = traded_
                    m_price = (self.current_market_prices[event['name2']]['asks'][0] + self.current_market_prices[event['name2']]['bids'][0])/2
                #if i % 60 == 0:
                for acc in self.accounts.keys():        
                    self.accounts[acc].total.append(self.accounts[acc].balance[base] + \
                                                (self.accounts[acc].balance[coin] + self.accounts[acc].balance[short_coin]) * m_price +
                                                 self.accounts[acc].balance[leader_coin] * m_price_leader)
                    self.accounts[acc].operation.append(450)
                    pose = self.accounts[acc].total[-1]
                    if traded:
                        last_max_pose = self.accounts[acc].total[-1]
                coins_volume = self.accounts[acc].balance[coin] + self.accounts[acc].balance[short_coin]

        
