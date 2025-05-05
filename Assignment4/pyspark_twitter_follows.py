#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext

def parse_line(line):
    user_id, follows = line.split(": ")
    if len(follows) > 0:
        return (len(follows.split(" ")), user_id)
    else:
        return (0, user_id)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter follows.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)

    # fill in your code here
    data = lines.map(parse_line)

    total_users = data.count()
    max_user = data.max()
    
    total_follows =  data.map(lambda x: x[0]).reduce(lambda a, b: a+b)
    average = total_follows / total_users
    no_follows = data.filter(lambda x: x[0] == 0).count()

    end = time.time()
   
    total_time = end - start

    # the first ??? should be the twitter id
    print(f'max follows: {max_user[1]} follows {max_user[0]}')
    print(f'users follow on average: {average}')
    print(f'number of user who follow no-one: {no_follows}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')

