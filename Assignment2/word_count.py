import os
import argparse
import sys
import time
import multiprocessing as mp

def get_filenames(path):
    """
    A generator function: Iterates through all .txt files in the path and
    returns the full names of the files

    Parameters:
    - path : string, path to walk through

    Yields:
    The full filenames of all files ending in .txt
    """
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith('.txt'):
                yield f'{root}/{file}'

def get_file(path):
    """
    Reads the content of the file and returns it as a string.

    Parameters:
    - path : string, path to a file

    Return value:
    The content of the file in a string.
    """
    with open(path,'r') as f:
        return f.read()

def count_words_in_file(file):
    """
    Counts the number of occurrences of words in the file
    Whitespace is ignored

    Parameters:
    - file, string : the content of a file

    Returns: Dictionary that maps words (strings) to counts (ints)
    """
    counts = dict()
    for word in file.split():
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    return counts



def get_top10(counts):
    """
    Determines the 10 words with the most occurrences.
    Ties can be solved arbitrarily.

    Parameters:
    - counts, dictionary : a mapping from words (str) to counts (int)
    
    Return value:
    A list of (count, word) pairs (int, str)
    """
    top10 = []
    
    for word, count in counts.items():
        if len(top10) < 10:
            top10.append((count, word))
        else:
            min_count = min(top10)
            if count > min_count[0]:
                top10.remove(min_count)
                top10.append((count, word))
    
    # Sort the top10 list in descending order of counts
    top10.sort(reverse=True)
    
    return top10



def merge_counts(dict_to, dict_from):
    """
    Merges the word counts from dict_from into dict_to, such that
    if the word exists in dict_to, then the count is added to it,
    otherwise a new entry is created with count from dict_from

    Parameters:
    - dict_to, dictionary : dictionary to merge to
    - dict_from, dictionary : dictionary to merge from

    Return value: None
    """
    for (k,v) in dict_from.items():
        if k not in dict_to:
            dict_to[k] = v
        else:
            dict_to[k] += v

def compute_checksum(counts):
    """
    Computes the checksum for the counts as follows:
    The checksum is the sum of products of the length of the word and its count

    Parameters:
    - counts, dictionary : word to count dictionary

    Return value:
    The checksum (int)
    """
    checksum = 0
    for word, count in counts.items():
        checksum = checksum + (len(word) * count)

    print(checksum)
    return checksum
    """raise NotImplementedError"""

def amdahl_speedup(P, n):
    return 1 / ((1 - P) + (P / n))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Counts words of all the text files in the given directory')
    parser.add_argument('-w', '--num-workers', help = 'Number of workers', default=1, type=int)
    parser.add_argument('-b', '--batch-size', help = 'Batch size', default=1, type=int)
    parser.add_argument('path', help = 'Path that contains text files')
    args = parser.parse_args()

    # construct workers and queues
    # construct a special merger process
    # put filenames into the input queue
    # workers then put dictionaries for the merger
    # the merger shall return the checksum and top 10 through the out queue

    path = args.path

    if not os.path.isdir(path):
        sys.stderr.write(f'{sys.argv[0]}: ERROR: `{path}\' is not a valid directory!\n')
        quit(1)

    num_workers = args.num_workers
    if num_workers < 1:
        sys.stderr.write(f'{sys.argv[0]}: ERROR: Number of workers must be positive (got {num_workers})!\n')
        quit(1)

    batch_size = args.batch_size
    if batch_size < 1:
        sys.stderr.write(f'{sys.argv[0]}: ERROR: Batch size must be positive (got {batch_size})!\n')
        quit(1)
    
    t1 = time.time()
    files = [get_file(fn) for fn in get_filenames(path)]
    #file_counts = list()
    t2 = time.time()

    with mp.Pool(num_workers) as pool:
      file_counts = pool.map(count_words_in_file, files)
    t3 = time.time()
    
    global_counts = dict()

    for counts in file_counts:
        merge_counts(global_counts,counts)
    t4 = time.time()
    
    checksum = compute_checksum(global_counts)
    top10 = get_top10(global_counts)
    t5 = time.time()
    time_tot=t5 - t1
    time_p=t3 - t2
    time_seq= time_tot-time_p
    
    print("Top 10 words:", top10)

    print(f"\n--- Timing Report ---\n")
    print(f"File loading time:      {t2 - t1:.4f} seconds\n")
    print(f"Multiprocessing time:   {time_p:.4f} seconds\n")
    print(f"Merging time:           {t4 - t3:.4f} seconds\n")
    print(f"Checksum & Top10 time:  {t5 - t4:.4f} seconds\n")
    print(f"Total time:             {time_tot:.4f} seconds\n")
    print(f"Sequential time:        {time_seq:.4f} seconds\n")
    print(f"Theoretical speedup: {amdahl_speedup(time_p, num_workers):.4f}")
    

