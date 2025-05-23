#!/usr/bin/env python3

import numpy as np
import pandas as pd
import csv
import argparse
import time

def load_glove(filename):
    """
    Loads the glove dataset. Returns three things:
    A dictionary that contains a map from words to rows in the dataset.
    A reverse dictionary that maps rows to words.
    The embeddings dataset as a NumPy array.
    """
    df = pd.read_table(filename, sep=' ', index_col=0, header=None,
                           quoting=csv.QUOTE_NONE)
    word_to_idx = dict()
    idx_to_word = dict()
    for (i,word) in enumerate(df.index):
        word_to_idx[word] = i
        idx_to_word[i] = word
    return (word_to_idx, idx_to_word, df.to_numpy())

def normalize(X):
    """
    Reads an n*d matrix and normalizes all rows to have unit-length (L2 norm)
    
    Implement this function using array operations! No loops allowed.
    """
    return X/np.linalg.norm(X, axis=1, keepdims=True)

def construct_queries(queries_fn, word_to_idx, X):
    """
    Reads queries (one string per line) and returns:
    - The query vectors as a matrix Q (one query per row)
    - Query labels as a list of strings
    """
    with open(queries_fn, 'r') as f:
        queries = f.read().splitlines()
    Q = np.zeros((len(queries), X.shape[1]))
    for i in range(len(queries)):
        Q[i,:] = X[word_to_idx[queries[i]],:]
    return (Q,queries)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dataset', help='Glove dataset filename',
                            type=str)
    parser.add_argument('queries', help='Queries filename', type=str)
    args = parser.parse_args()
    
    (word_to_idx, idx_to_word, X) = load_glove(args.dataset)


    X = normalize(X)

    (Q,queries) = construct_queries(args.queries, word_to_idx, X)

    t1 = time.time()

    # Compute the dot product between Q and X
    dot_product = np.dot(Q, X.T)

    t2 = time.time()

    # Get the indices of the top k nearest neighbors for each query
    I = np.argsort(dot_product, axis=1)

    # Compute here I such that I[i,:] contains the indices of the nearest
    # neighbors of the word i in ascending order.
    # Naturally, I[i,-1] should then be the index of the word itself.

    t3 = time.time()
    
    for i in range(I.shape[0]):
        neighbors = [idx_to_word[i] for i in I[i,-2:-5:-1]]
        print(f'{queries[i]}: {" ".join(neighbors)}')

    print('matrix multiplication took', t2-t1)
    print('sorting took', t3-t2)
    print('total time', t3-t1)
