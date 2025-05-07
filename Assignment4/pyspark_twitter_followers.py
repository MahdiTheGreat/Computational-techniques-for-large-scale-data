#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext

def line_parser(line):
    user, follows = line.split(":")
    users = [(user, 0)]
    for u in follows.split():
        users.append((u,1))

    return users




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter followers.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)

    # fill in your code here
    data = lines.flatMap(line_parser).reduceByKey(lambda x,y: x+y).cache()

    total_users = data.count()

    data = data.filter(lambda x: x[1]>0)
    total_followers =  data.map(lambda x: x[1]).reduce(lambda a, b: a+b)
    average = total_followers/total_users

    no_followers = total_users - data.count()
    max_user = data.max(key=lambda x:x[1])
    
    end = time.time()
    
    total_time = end - start

    # the first ??? should be the twitter id
    print(f'max followers: {max_user[0]} has {max_user[1]} followers')
    print(f'followers on average: {average}')
    print(f'number of user with no followers: {no_followers}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')

