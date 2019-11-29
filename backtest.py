from strategy import SmaCross
from exchange import Exchange
from utils_spark import assert_msg, crossover, SMA, read_file


class Backtest:
    def __init__(self, data, cash=10000, commission=0.0005, fast=30, slow=90):
        self.sma1 = []
        self.sma2 = []
        self._data = data
        self._cash = cash
        self._commission = commission
        self._fast = fast
        self._slow = slow
        self._results = {}
        self._broker = Exchange(data, cash, commission)
        self._strategy = SmaCross(self._broker, data, fast, slow)
        self.values = self._broker.values
        
    def run(self):
        self._strategy.init()
        self.sma1 = self._strategy.sma1
        self.sma2 = self._strategy.sma2
        
        start = self._slow + 1
        end = len(self._broker.values)
        for i in range(start, end):
            self._broker.next(i)
            self._strategy.next(i)
        
        self._results['Initial value:'] = self._broker.initial_cash
        self._results['Final value:'] = self._broker.market_value
        self._results['Transaction times:'] = self._strategy.count
        self._results['Profit:'] = self._broker.market_value - self._broker.initial_cash
        
        return self._results
    
    def print_results(self):
        for k, v in self._results.items():
            print(k, v)
            
def main():
    data = read_file('600602.SH.csv')
    backtest = Backtest(data, 10000.0, 0, 30, 90)
    profit = backtest.run()
    backtest.print_results()
    print(backtest.sma1[200:230])
    

if __name__ == '__main__':
    main()

