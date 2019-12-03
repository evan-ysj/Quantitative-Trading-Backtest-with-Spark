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
        # Define and initialize objects of Exchange and SmaCross.
        self._broker = Exchange(data, cash, commission)
        self._strategy = SmaCross(self._broker, data, fast, slow)
        self.values = self._broker.values
        
    def run(self):
        """
        Simulate the transaction with the price data "Close";
        -- Get the price of current tick by broker;
        -- Calculate with the strategy at current tick.
        """
        # Initialize the strategy to get sma1 and sma2.
        self._strategy.init()
        self.sma1 = self._strategy.sma1
        self.sma2 = self._strategy.sma2
        # specify the start and end point of the simulation process.
        start = self._slow + 1
        end = len(self._broker.values)
        # Increase the tick by one at each iteration.
        for i in range(start, end):
            # Update the value of tick and get current price.
            self._broker.next(i)
            # Execute the strategy.
            self._strategy.next(i)
        # Record computing results in a dictionary.
        self._results['Commission:'] = self._commission
        self._results['SMA1 window size:'] = self._fast
        self._results['SMA2 window size:'] = self._slow
        self._results['Initial value:'] = self._broker.initial_cash
        self._results['Final value:'] = self._broker.market_value
        self._results['Transaction frequency:'] = self._strategy.count
        self._results['Profit:'] = self._broker.market_value - self._broker.initial_cash       
        return self._results
    
    def print_results(self):
        for k, v in self._results.items():
            print('  ', k, v)


def main():
    data = read_file('data/600602.SH.csv')
    backtest = Backtest(data, 10000.0, 0, 30, 90)
    profit = backtest.run()
    backtest.print_results()
    

if __name__ == '__main__':
    main()

