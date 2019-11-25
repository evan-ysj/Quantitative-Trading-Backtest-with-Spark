from utils_spark import assert_msg

class Exchange:
    def __init__(self, data, cash, commission):
        assert_msg(cash > 0, 'Initial cash must be greater than 0!')
        assert_msg(0 <= commission <= 0.05, 
                   'Commission must be a positive integer which is usually smaller that 0.05')
        self._initial_cash = cash
        self._cash = cash
        self._position = 0
        self._data = data
        self._commission = commission
        self._tick = 0
        self.values = self._data.select(self._data.Close).rdd.map(lambda x: x[0]).collect()
        
    @property
    def initial_cash(self):
        return self._initial_cash;
                
    @property
    def current_price(self):
        return self.values[self._tick]
    
    @property
    def market_value(self):
        return self._cash + self.current_price * self._position
    
    def buy(self):
        self._position = float((self._cash * (1 - self._commission)) / self.current_price)
        self._cash = 0
        
    def sell(self):
        self._cash += float(self._position * self.current_price * (1 - self._commission))
        self._position = 0
        
    def next(self, tick):
        self._tick = tick
