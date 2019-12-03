from utils_spark import assert_msg

class Exchange:
    def __init__(self, data, cash, commission):
        # If initial cash or commission is invalid then throw corresponding exception.
        assert_msg(cash > 0, 'Initial cash must be greater than 0!')
        assert_msg(0 <= commission <= 0.05, 
                   'Commission must be a positive integer which is usually smaller that 0.05')
        self._initial_cash = cash
        self._cash = cash
        # The current number of shares of a stock
        self._position = 0
        self._data = data
        self._commission = commission
        self._tick = 0
        # Take the column "Close" out of source data.
        self.values = self._data.select(self._data.Close).rdd.map(lambda x: x[0]).collect()
        
    @property
    def initial_cash(self):
        return self._initial_cash;
                
    @property
    def current_price(self):
        return self.values[self._tick]
    
    @property
    def market_value(self):
        """
        Calculate the current total value:
        Current balence + current value of stock.
        """
        return self._cash + self.current_price * self._position
    
    def buy(self):
        """
        Execute "buy" instruction: Recalculate the balance and position in account.
        """
        self._position = float((self._cash * (1 - self._commission)) / self.current_price)
        self._cash = 0
        
    def sell(self):
        """
        Execute "sell" instruction: Recalculate the balance and position in account.
        """
        self._cash += float(self._position * self.current_price * (1 - self._commission))
        self._position = 0
        
    def next(self, tick):
        self._tick = tick
