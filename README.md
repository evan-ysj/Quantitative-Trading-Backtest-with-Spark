# Quantitative-Trading-Backtest-with-Spark

## Overview
A small backtest system of quantitative trading that is extended from some open source ideas about simple moving average strategy.  

![image](https://github.com/evan-ysj/Quantitative-Trading-Backtest-with-Spark/raw/master/Picture1.png)

## Core system
The system includes three classes and some independent functions.
* SmaCross
  This class defines the core strategy of the test method. Here we use a very simple strategy called SMA (Simple Moving Average). Two different size of windows are defined in the class, which can be modified to get different test results.
* Exchange
  This class simulates the real exchange in the backtest. The main function of this class is calculating current position and balance in the account. It also provides buy and sell API for strategy class to call. 
* Backtest
  This class accepts all parameters, executes the transaction under SMA strategy and return the final results.
There are also some other independent functions that not belong to above three classes. They are written in the file utils_spark.py and been called by the classes.

## Big data extension
The SMA strategy is implemented by spark context and dataframe. As the previous price information is available we can calculate the moving average parallelly. To accomplish this idea we also need to read the original data by spark and transfer it to be spark dataframe. 
