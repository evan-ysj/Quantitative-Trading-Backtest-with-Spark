from utils_spark import  assert_msg, crossover, SMA

class SmaCross:
    def __init__(self, broker, data, fast, slow):
        # Count the transaction frequency
        self.count = 0
        self.sma1 = []
        self.sma2 = []
        # The object of the class "Exchange"
        self._broker = broker;
        self._data = data;
        self._tick = 0;
        # If the sizes of windows are invalid, throw an exception 
        condition = (fast > 0 and fast <200 and slow > 0 and slow < 500 and fast < slow)
        assert_msg(condition, "Error: Invalid size of rolling windows")
        self._fast = fast
        self._slow = slow
        
    def buy(self):
        """
        Call buy() function by the object of "Exchange".
        """
        self._broker.buy()
        self.count += 1
        
    def sell(self):
        """
        Call sell() function by the object of "Exchange".
        """
        self._broker.sell()
        self.count += 1
        
    def init(self):
        """
        Initialize two SMA arrays from original price sequence by calling SMA().
        """
        self.sma1 = SMA(self._data.select(self._data.Close), self._fast)
        self.sma2 = SMA(self._data.select(self._data.Close), self._slow)
        
    def next(self, tick):
        """
        Decide if there is an intersection at a paticular tick;
        If sma1 passes sma2 from below, call buy();
        If sma1 passes sma2 from top, call sell().
        """
        if crossover(self.sma1[:tick], self.sma2[:tick]):
            self.buy()
        elif crossover(self.sma2[:tick], self.sma1[:tick]):
            self.sell()
        else:
            pass