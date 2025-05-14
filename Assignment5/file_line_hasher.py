from assignment5_problem1_skeleton.py import murmur3_32

if __name__ == '__main__':
    import sys
    import os
    import struct
    import hashlib

    # Open the file and read its lines
    with open("/data/courses/2025_dat470_dit066/words ", 'rb') as f:
        lines = f.readlines()

    # Hash each line and print the result
    
    for line in lines:
        line = line.strip()  # Remove leading/trailing whitespace
        hash_value = murmur3_32(line)
        print(f"Line: {line.decode('utf-8')}, Hash: {hash_value}")