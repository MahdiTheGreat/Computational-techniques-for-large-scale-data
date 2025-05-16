#!/usr/bin/env python3

import argparse
import sys
import os
from pyspark import SparkContext, SparkConf
import math
import time
import struct

def rol32(x,k):
    """Auxiliary function (left rotation for 32-bit words)"""
    return ((x << k) | (x >> (32-k))) & 0xffffffff

    
def murmur3_32(key, seed):
    """Computes the 32-bit murmur3 hash"""
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    r1 = 15
    r2 = 13
    m = 5
    n = 0xe6546b64

    hash_value = seed
    length = len(key)
    # a simple way of rounding is to use bitwise AND to clear the last 2 bits
    rounded_end = (length & ~0x3)  # round down to 4 byte block

    # Process the 4-byte chunks
    for i in range(0, rounded_end, 4):

        # Extract a 4-byte chunk (32 bits) from a bytes object (key) at offset i, 
        # and interpret it as an unsigned 32-bit integer in little-endian byte order.
        k = struct.unpack_from("<I", key, offset = i)[0]
        # "<" → Little-endian (least significant byte first)
        # "I" → Unsigned 32-bit integer (uint32)

        # Using AND with 0xFFFFFFFF to ensure the result always stays within 32 bits — effectively simulating overflow.
        k = (k * c1) & 0xFFFFFFFF
        k = rol32(k, r1)
        k = (k * c2) & 0xFFFFFFFF

        hash_value ^= k
        hash_value = rol32(hash_value, r2)
        hash_value = ((hash_value * m) + n) & 0xFFFFFFFF

    # Handle the remaining bytes
    remaining = key[rounded_end:]
    if remaining:
        k1 = 0
        # For big endian, we would use:
        # for i in range(len(remaining)):
        for i in range(len(remaining) - 1, -1, -1):
            k1 <<= 8
            k1 |= remaining[i]
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = rol32(k1, r1)
        k1 = (k1 * c2) & 0xFFFFFFFF
        hash_value ^= k1

    # Finalization
    hash_value ^= length
    hash_value ^= (hash_value >> 16)
    hash_value = (hash_value * 0x85ebca6b) & 0xFFFFFFFF
    hash_value ^= (hash_value >> 13)
    hash_value = (hash_value * 0xc2b2ae35) & 0xFFFFFFFF
    hash_value ^= (hash_value >> 16)

    return hash_value

def auto_int(x):
    """Auxiliary function to help convert e.g. hex integers"""
    return int(x,0)

def dlog2(n):
    return n.bit_length() - 1

def rho(n):
    """Given a 32-bit number n, return the 1-based position of the first
    1-bit"""
    if n == 0:
        return 0
    else:
        return 32-n.bit_length()+1
    

def compute_jr(key,seed,log2m):
    """hash the string key with murmur3_32, using the given seed
    then take the **least significant** log2(m) bits as j
    then compute the rho value **from the left**

    E.g., if m = 1024 and we compute hash value 0x70ffec73
    or 0b01110000111111111110110001110011
    then j = 0b0001110011 = 115
         r = 2
         since the 2nd digit of 0111000011111111111011 is the first 1

    Return a tuple (j,r) of integers
    """
    h = murmur3_32(key,seed)
    #print(f'{h:08x}')
    j = ~(0xffffffff << log2m) & h
    r = rho(h)
    return j, r

def get_files(path):
    """
    A generator function: Iterates through all .txt files in the path and
    returns the content of the files

    Parameters:
    - path : string, path to walk through

    Yields:
    The content of the files as strings
    """
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith('.txt'):
                path = f'{root}/{file}'
                with open(path,'r') as f:
                    yield f.read()

def alpha(m):
    """Auxiliary function: bias correction"""
    return 0.7213/(1+(1.079/m))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Using HyperLogLog, computes the approximate number of '
            'distinct words in all .txt files under the given path.'
    )
    parser.add_argument('path',help='path to walk',type=str)
    parser.add_argument('-s','--seed',type=auto_int,default=0,help='seed value')
    parser.add_argument('-m','--num-registers',type=int,required=True,
                            help=('number of registers (must be a power of two)'))
    parser.add_argument('-w','--num-workers',type=int,default=1,
                        help='number of Spark workers')
    args = parser.parse_args()

    seed = args.seed
    m = args.num_registers
    if m <= 0 or (m&(m-1)) != 0:
        sys.stderr.write(f'{sys.argv[0]}: m must be a positive power of 2\n')
        quit(1)
    log2m = dlog2(m)

    num_workers = args.num_workers
    if num_workers < 1:
        sys.stderr.write(f'{sys.argv[0]}: must have a positive number of '
                         'workers\n')
        quit(1)

    path = args.path
    if not os.path.isdir(path):
        sys.stderr.write(f"{sys.argv[0]}: `{path}' is not a valid directory\n")
        quit(1)

    start = time.time()
    conf = SparkConf()
    conf.setMaster(f'local[{num_workers}]')
    conf.set('spark.driver.memory', '16g')
    sc = SparkContext(conf=conf)

    data = sc.parallelize(get_files(path))
    # Implement HyperLogLog here

    # Initialise array to 0
    M = [0]*m

    # Split data into words
    data = data.flatMap(lambda line: line.strip().split())
    # Compute (j,r) for each word and reduce to only max values of r for each j
    data = data.map(lambda key: compute_jr(key.encode('utf-8'), seed, log2m)).reduceByKey(lambda x,y: max(x,y))
    
    for j, r in data.collect():
        M[j] = r

    # Compute harmonic mean and estimate cardinality
    Z = sum(2**(-r) for r in M)
    n = alpha(m) * m * m / Z
    V = sum(1 for x in M if x == 0)

    if n <= (5/2)*m and V > 0:
        E = m * math.log(m/V)
    elif n > ((1/30) * 2**32):
        E = -(2**32) * math.log(1 - n/(2**32))
    else:
        E = n
    
    end = time.time()

    print(f'Cardinality estimate: {E:0.1f}')
    print(f'Number of workers: {num_workers}')
    print(f'Took {end-start} s')

    
    

    