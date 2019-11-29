from utils_spark import  assert_msg, crossover, SMA

class SmaCross:
    def __init__(self, broker, data, fast, slow):
        self._broker = broker;
        self._data = data;
        self._tick = 0;
        condition = (fast > 0 and fast <200 and slow > 0 and slow < 500 and fast < slow)
        assert_msg(condition, "Error: Invalid size of rolling windows")
        self._fast = fast
        self._slow = slow
        self.count = 0
        self.sma1 = []
        self.sma2 = []
        
    def buy(self):
        self._broker.buy()
        self.count += 1
        
    def sell(self):
        self._broker.sell()
        self.count += 1
        
    def init(self):
        self.sma1 = SMA(self._data.select(self._data.Close), self._fast)
        self.sma2 = SMA(self._data.select(self._data.Close), self._slow)
        
    def next(self, tick):
        if crossover(self.sma1[:tick], self.sma2[:tick]):
            self.buy()
        elif crossover(self.sma2[:tick], self.sma1[:tick]):
            self.sell()
        else:
            pass