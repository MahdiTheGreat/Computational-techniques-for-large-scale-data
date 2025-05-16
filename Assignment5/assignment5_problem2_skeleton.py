#!/usr/bin/env python3

import argparse
import sys
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
    """Auxiliary function to compute discrete base2 logarithm"""
    return n.bit_length() - 1

def rho(n):
    """Given a 32-bit number n, return the 1-based position of the first
    1-bit"""
    return 32-n.bit_length() + 1
    

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
    print(f'{h:08x}')
    j = ~(0xffffffff << log2m) & h
    r = rho(h)
    return j, r


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Computes (j,r) pairs for input integers.'
    )
    parser.add_argument('key',nargs='*',help='key(s) to be hashed',type=str)
    parser.add_argument('-s','--seed',type=auto_int,default=0,help='seed value')
    parser.add_argument('-m','--num-registers',type=int,required=True,
                            help=('Number of registers (must be a power of two)'))
    args = parser.parse_args()

    seed = args.seed
    m = args.num_registers
    if m <= 0 or (m&(m-1)) != 0:
        sys.stderr.write(f'{sys.argv[0]}: m must be a positive power of 2\n')
        quit(1)

    log2m = dlog2(m)

    for key in args.key:
        h = murmur3_32(key.encode('utf-8'),seed)

        j, r = compute_jr(key.encode('utf-8'),seed,log2m)

        print(f'{key}\t{j}\t{r}')
        