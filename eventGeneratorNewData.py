import csv
import datetime

class MetaFile():
    def __init__(self, name = {'market': 'binance', 'symbol': 'btcusdt', 'type':'orderbook'}, \
                 start = datetime.datetime(2021, 9, 2, 18), finish = datetime.datetime(2021, 9, 4, 18)):
        self.name = name
        self.moment = start
        self.finish = finish
        self.reader = 'Not initialized'
        
    def _shift_moment(self):
        if self.moment < self.finish:
            #print('shifted')
            self.moment = self.moment + datetime.timedelta(hours = 1)
            return 0
        else:
            self.moment = None
            return 1
            
    def _return_reader(self):
        if self.moment is not None:
            return csv.reader(open(f"""data/{self.name['market']}/{self.name['symbol']}/{self.name['type']}/{str(self.moment.year) + '_' + str(self.moment.month)}/{self.moment.day}/{self.moment.hour}.csv"""))
        else:
            return None
        
    def get_next_string(self):
        if self.reader == 'Not initialized':
            self.reader = self._return_reader()
        elif self.reader is None:
            return None
        string = next(self.reader, None)
        if string is None:
            res = self._shift_moment()
            if res == 0:
                self.reader = self._return_reader()
                return self.get_next_string()
            else:
                return {'name': None, 'data': None, 'ts': None}
        else:
            #ts = float(string[-1])
            #if self.name['market'] == 'cryptocom':
            ts = float(string[-2])
            ts = ts / 1000
            date = string[-1]
            return {'name': self.name, 'name2': (self.name['market'], self.name['symbol'], self.name['type']), 'data': string, 'ts': ts, 'date': date}
            


class EventGenerator():
    def __init__(self, file_names = [{'market': 'binance', 'symbol': 'btcusdt', 'type':'orderbook'},\
                                     {'market': 'cryptocom', 'symbol': 'BTC_USDT', 'type':'orderbook'}, \
                                     {'market': 'cryptocom', 'symbol': 'BTC_USDT', 'type':'trades'}], \
                 start = datetime.datetime(2021, 9, 2, 18), finish = None):
        self.file_names = file_names
        self.start = start
        self.finish = finish
        if self.finish is None:
            self.finish = self.start
        self.event_list = []
        self.next_event = None
        
        
    def initialize_metafiles(self):
        self.metafiles = {}
        self.current_events = {}
        for bn in self.file_names:
            self.metafiles[(bn['market'], bn['symbol'], bn['type'])] = MetaFile(bn, self.start, self.finish)
            self.current_events[(bn['market'], bn['symbol'], bn['type'])] = \
                self.metafiles[(bn['market'], bn['symbol'], bn['type'])].get_next_string()
            
    
    def update_next_event(self):
        min_ts_name = (self.file_names[0]['market'], self.file_names[0]['symbol'], self.file_names[0]['type'])
        if self.current_events[min_ts_name] is not None:
            min_ts = self.current_events[min_ts_name]['ts']
        for file_name in self.file_names:
            name = (file_name['market'], file_name['symbol'], file_name['type'])
            if self.current_events[name]['ts'] is None:
                self.next_event = 'End'
                return None
            if self.current_events[name]['ts'] < min_ts:
                min_ts = self.current_events[name]['ts']
                min_ts_name = name
        self.next_event = self.current_events[min_ts_name].copy()
        self.current_events[min_ts_name] = self.metafiles[min_ts_name].get_next_string()
        return 0
    
    def construct_event_list(self):
        self.initialize_metafiles()
        while True:
            res = self.update_next_event()
            if res is None:
                break
            else:
                self.event_list.append(self.next_event.copy())
            
        