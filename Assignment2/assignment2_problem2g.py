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


def count_words_in_file(filename_queue, wordcount_queue, batch_size):
    """
    Counts the number of occurrences of words in the file
    Performs counting until a None is encountered in the queue
    Counts are stored in wordcount_queue
    Whitespace is ignored
    Parameters:
    - filename_queue, multiprocessing queue : will contain filenames and None as a
sentinel to indicate end of input
    - wordcount_queue, multiprocessing queue : (word,count) dictionaries are put in
the queue, and end of input is indicated with a None
    - batch_size, int : size of batches to process
    
    Returns: None
    """
    keep_counting = True

    while keep_counting:
        counts = dict()
        files_processed = 0

        while (files_processed < batch_size):
            filename = filename_queue.get()
            if filename is None: # End of input
                keep_counting = False
                break

            file = get_file(filename)
            for word in file.split():
                if word in counts:
                    counts[word] += 1
                else:
                    counts[word] = 1
            files_processed += 1

        if files_processed > 0:
            wordcount_queue.put(counts)

    wordcount_queue.put(None)

def get_top10(counts):
    """
    Determines the 10 words with the most occurrences.
    Ties can be solved arbitrarily.
    
    Parameters:
    - counts, dictionary : a mapping from words (str) to counts (int)
    
    Return value:
    A list of (count,word) pairs (int,str)
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


def merge_counts(out_queue,wordcount_queue,num_workers):
    """
    Merges the counts from the queue into the shared dict global_counts.
    Quits when num_workers Nones have been encountered.
    
    Parameters:
    - global_counts, manager dict : global dictionary where to store the counts
    - wordcount_queue, manager queue : queue that contains (word,count) pairs and
Nones to signal end of input from a worker
    - num_workers, int : number of workers (i.e., how many Nones to expect)
    
    Return value: None
    """
    none_count = 0
    global_counts = dict()
    while none_count < num_workers:
        wordcounts = wordcount_queue.get()
        if wordcounts is None:
            none_count += 1
        else:
            for word, value in wordcounts.items():
                if word in global_counts:
                    global_counts[word] += value
                else:
                    global_counts[word] = value
    
    out_queue.put(compute_checksum(global_counts))
    out_queue.put(get_top10(global_counts))
            


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

    return checksum


if __name__ == '__main__':
    start = time.time()
    parser = argparse.ArgumentParser(description='Counts words of all the text files in the given directory')
    parser.add_argument('-w', '--num-workers', help = 'Number of workers', default=1, type=int)
    parser.add_argument('-b', '--batch-size', help = 'Batch size', default=1, type=int)
    parser.add_argument('path', help = 'Path that contains text files')
    args = parser.parse_args()

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
        sys.stderr.write(f'{sys.argv[0]}: ERROR: Batch size must be positive (got{batch_size})!\n')
        quit(1)

    # construct workers and queues
    filename_queue = mp.Queue()
    wordcount_queue = mp.Queue()
    out_queue = mp.Queue()

    workers = [mp.Process(target = count_words_in_file, args=(filename_queue, wordcount_queue, batch_size)) for _ in range(num_workers)]

    for w in workers:
        w.start()
    # construct a special merger process
    merger = mp.Process(target= merge_counts, args= (out_queue, wordcount_queue, num_workers))
    merger.start()

    # put filenames into the input queue
    for filename in get_filenames(path):
        filename_queue.put(filename)

    for _ in range(num_workers):
        filename_queue.put(None)
    # workers then put dictionaries for the merger    

    for w in workers:
        w.join()
        

    # the merger shall return the checksum and top 10 through the out queue
    merger.join()
    print("Checksum:", end=" ")
    print(out_queue.get())
    print("Top10:", end=" ")
    print(out_queue.get())
    end = time.time() - start
    print("Execution time: ", end=" ")
    print(end)
