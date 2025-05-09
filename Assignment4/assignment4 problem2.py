import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg
from pyspark.sql.types import IntegerType
import pandas as pd
import sys

@udf(returnType=IntegerType())
def jdn(dt):
    """
    Computes the Julian date number for a given date.
    Parameters:
    - dt, datetime : the Gregorian date for which to compute the number

    Return value: an integer denoting the number of days since January 1, 
    4714 BC in the proleptic Julian calendar.
    """
    y = dt.year
    m = dt.month
    d = dt.day
    if m < 3:
        y -= 1
        m += 12
    a = y//100
    b = a//4
    c = 2-a+b
    e = int(365.25*(y+4716))
    f = int(30.6001*(m+1))
    jd = c+d+e+f-1524
    return jd
   
@udf(returnType=IntegerType())
def decade(x):
    return x.year//10*10

# you probably want to use a function with this signature for computing the
# simple linear regression with least squares using applyInPandas()
# key is the group key, df is a Pandas dataframe
# should return a Pandas dataframe
def lsq(key,df):
    x_mean = df['JDN'].mean()
    y_mean = df['AVG_T'].mean()

    df['JDN'] = df['JDN'].apply(lambda x: x-x_mean)
    df['AVG_T'] = df['AVG_T'].apply(lambda y: y-y_mean)
    df['SUM'] = df['JDN'] * df['AVG_T']
    df['x2'] = df['JDN']*df['JDN']
    beta = df['SUM'].sum() / df['x2'].sum()

    return pd.DataFrame({'STATION' : key, 'NAME' : df['NAME'].iloc[0],'BETA' : beta})

if __name__ == '__main__':
    # do not change the interface
    parser = argparse.ArgumentParser(description = \
                                    'Compute climate data.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    # this bit is important: by default, Spark only allocates 1 GiB of memory 
    # which will likely cause an out of memory exception with the full data
    spark = SparkSession.builder \
            .master(f'local[{args.num_workers}]') \
            .config("spark.driver.memory", "16g") \
            .getOrCreate()
    
    # read the CSV file into a pyspark.sql dataframe and compute the things you need
    start = time.time()
    df = spark.read.csv(args.filename,header=True,inferSchema=True)
    reading_time = time.time() - start
    df = df.withColumn('JDN', jdn(df.DATE))

    df = df.withColumn('DECADE', decade(df.DATE))

    df = df.withColumn('AVG_T', (df.TMIN+df.TMAX)/2).cache()

    linear_reg = df.select('STATION', 'NAME', 'JDN', 'AVG_T').groupBy('STATION')\
        .applyInPandas(lsq, schema = 'STATION string, NAME string, BETA double').cache()

    # top 5 slopes are printed here
    # replace None with your dataframe, list, or an appropriate expression
    # replace STATIONCODE, STATIONNAME, and BETA with appropriate expressions
    print('Top 5 coefficients:')
    for row in linear_reg.sort('BETA', ascending=False).limit(5).collect():
        print(f'{row.STATION} at {row.NAME} BETA={row.BETA:0.3e} °F/d')

    # replace None with an appropriate expression
    total_rows = linear_reg.count()
    positive_betas = linear_reg[linear_reg['BETA'] > 0].count()
    print('Fraction of positive coefficients:')
    print(positive_betas/total_rows)

    # Five-number summary of slopes, replace with appropriate expressions
    print('Five-number summary of BETA values:')

    beta_min, beta_q1, beta_median, beta_q3, beta_max= linear_reg.approxQuantile("BETA", [0.0,0.25, 0.5, 0.75,1.0], 0.01)
    print(f'beta_min {beta_min:0.3e}')
    print(f'beta_q1 {beta_q1:0.3e}')
    print(f'beta_median {beta_median:0.3e}')
    print(f'beta_q3 {beta_q3:0.3e}')
    print(f'beta_max {beta_max:0.3e}')

    # Here you will need to implement computing the decadewise differences 
    # between the average temperatures of 1910s and 2010s

    # There should probably be an if statement to check if any such values were 
    # computed (no suitable stations in the tiny dataset!)

    # Note that values should be printed in celsius

    # Replace None with an appropriate expression
    # Replace STATION, STATIONNAME, and TAVGDIFF with appropriate expressions
    avg_t_decades = df.filter((col('DECADE').isin(1910, 2010))).groupBy('STATION', 'NAME', 'DECADE').agg(avg('AVG_T').alias("DECADE_AVG"))

    temp_diff = (
        avg_t_decades.alias("a")
        .join(avg_t_decades.alias("b"), on="STATION")
        .filter((col("a.DECADE") == 1910) & (col("b.DECADE") == 2010))
        .select(
            col("a.STATION"),
            col("a.NAME"),
            col("a.DECADE_AVG").alias("TEMP_1910"),
            col("b.DECADE_AVG").alias("TEMP_2010"),
            ((col("b.DECADE_AVG")-32) * (5/9) - (col("a.DECADE_AVG")-32) * (5/9)).alias("TEMP_DIFF_C")
        ).cache()
    )
    #temp_diff.show()    

    print('Top 5 differences:')
    for row in temp_diff.sort('TEMP_DIFF_C', ascending=False).limit(5).collect():
        print(f'{row.STATION} at {row.NAME} difference {row.TEMP_DIFF_C:0.1f} °C)')

    # replace None with an appropriate expression
    total_rows = temp_diff.count()
    positive_rows = temp_diff.filter(col('TEMP_DIFF_C') > 0).count()
    print('Fraction of positive differences:')
    print(positive_rows / total_rows)

    # Five-number summary of temperature differences, replace with appropriate expressions
    print('Five-number summary of decade average difference values:')
    tdiff_min, tdiff_q1, tdiff_median, tdiff_q3, tdiff_max = temp_diff.approxQuantile("TEMP_DIFF_C", [0.0,0.25, 0.5, 0.75,1.0], 0.01)
    print(f'tdiff_min {tdiff_min:0.1f} °C')
    print(f'tdiff_q1 {tdiff_q1:0.1f} °C')
    print(f'tdiff_median {tdiff_median:0.1f} °C')
    print(f'tdiff_q3 {tdiff_q3:0.1f} °C')
    print(f'tdiff_max {tdiff_max:0.1f} °C')

    # Add your time measurements here
    end = time.time() - start
    
    # It may be interesting to also record more fine-grained times (e.g., how 
    # much time was spent computing vs. reading data)
    print(f'num workers: {args.num_workers}')
    print(f'total time: {end:0.1f} s')
    print(f'read: {reading_time}')
    print(f'computations: {end - reading_time}')
