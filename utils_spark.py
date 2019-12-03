from numbers import Number
import os.path as path
import findspark
findspark.init("/Users/Evan/spark-3.0.0-preview-bin-hadoop2.7")

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

sc = SparkContext(appName="MySpark", master="local", conf=SparkConf().set('spark.ui.port', 4000))
sdf = SparkSession.builder.appName("MySparkDF").master("local").config('spark.ui.port', 5000).getOrCreate()

def assert_msg(condition, message):
    if not condition:
        raise Exception(message)
        

def read_file(filename):
    """
    Read source data from csv file.
    The data is cleaned and formated to the following schema:
    |--Date:  data
    |--Open:  double
    |--High:  double
    |--Low:   double
    |--Close: double
    """
    file_path = path.join(path.dirname(__file__), filename)
    
    # Check whether the file exists or not
    assert_msg(path.exists(file_path), 'File does not exist!{0}'.format(file_path))
    
    # Read CSV file
    df_raw = sdf.read.csv(file_path,sep=',',inferSchema=True)
    df = df_raw.drop('_c8','_c9','_c10','_c11','_c12','_c13','_c14','_c15',
                    '_c16','_c17','_c18','_c19','_c20','_c21','_c22','_c23', '_c24')
    df = df.toDF('ID','Name','Date','Pre_Close','Open','High','Low','Close')
    df = df.withColumn('Date', df.Date.cast('date'))\
            .withColumn('Open', df.Open.cast('double'))\
            .withColumn('High', df.High.cast('double'))\
            .withColumn('Low', df.Low.cast('double'))\
            .withColumn('Close', df.Close.cast('double'))\
            .sort(df.Date)\
            .select('Date','Open','High','Low','Close')\
            .na.drop()
    #df.printSchema()
    #df.show()
    return df


def SMA(col, k):
    """
    Return the Simple Moving Average
    """
    values = col.rdd.map(lambda x: x[0]).collect()
    ref = list(range(len(values)))
    mean_list = sc.parallelize(ref)\
                .map(lambda x: (x, values[x-k+1:x+1] if x >= k-1 else [0]))\
                .map(lambda x: sum(x[1])/k)\
                .collect()
    return mean_list


def crossover(series1, series2) -> bool:
    """
    Check whether two series cross at the end
    :param series1:  series1
    :param series2:  series2
    :return:         True when cross, False otherwise
    """    
    return series1[-2] < series2[-2] and series1[-1] > series2[-1]