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

class RandomHyperplanes:
    """
    This class mimics the interface of sklearn:
    - the constructor sets the number of hyperplanes
    - the random hyperplanes are drawn when fit() is called 
      (input dimension is set)
    - transform actually transforms the vectors
    - fit_transform does fit first, followed by transform
    """
    def __init__(self, D, seed = None)->None:
        """
        Sets the number of hyperplanes (D) and the optional random number seed
        """
        self._D = D
        self._seed = seed

    def fit(self, X):
        """
        Draws _D random hyperplanes, that is, by drawing _D Gaussian unit 
        vectors of length determined by the second dimension (number of 
        columns) of X
        """
        rng = np.random.default_rng(self._seed)
        self._hyperplanes = rng.normal(size=(self._D, X.shape[1]))
        self._hyperplanes = normalize(self._hyperplanes)
        print("Hyperplanes shape: ", self._hyperplanes.shape)
        print("Hyperplanes is: ", self._hyperplanes)

    def transform(self, X):
        """
        Project the rows of X into binary vectors
        """
        if not hasattr(self, '_hyperplanes'):
            raise ValueError("fit() must be called before transform()")
        # Compute the dot product and apply the sign function
        crossings=X @ self._hyperplanes.T
        # print("Crossings shape: ", crossings.shape)
        # print("Crossings is: ", crossings)
        # Convert to binary values (0 and 1)
        crossings = np.where(crossings > 0, 1, 0)
        # Convert to int
        return crossings.astype(int)
        
    def fit_transform(self, X):
        """
        Calls fit() followed by transform()
        """
        self.fit(X)
        return self.transform(X)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-D', help='Random hyperplanes dimension', type=int,
                            required = True)
    parser.add_argument('dataset', help='Glove dataset filename',
                            type=str)
    parser.add_argument('queries', help='Queries filename', type=str)
    args = parser.parse_args()
    
    (word_to_idx, idx_to_word, X) = load_glove(args.dataset)


    X = normalize(X)
    print("Normalized X shape: ", X.shape)

    (Q,queries) = construct_queries(args.queries, word_to_idx, X)

    start = time.time()

    rh = RandomHyperplanes(args.D, 1234)
    X2 = rh.fit_transform(X)
    Q2 = rh.transform(Q)
    print("X2 shape: ", X2.shape)
    print("X2 is: ", X2)
    print("Q2 shape: ", Q2.shape)
    print("Q2 is: ", Q2)

    end = time.time()

    print(end-start)
