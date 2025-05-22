#!/usr/bin/env python3

import numpy as np
import pandas as pd
from numpy import typing as npt
import csv
import argparse
import time
from operator import itemgetter

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

    def transform(self, X):
        """
        Project the rows of X into binary vectors
        """
        if not hasattr(self, '_hyperplanes'):
            raise ValueError("fit() must be called before transform()")
        # Compute the dot product
        crossings= X @ self._hyperplanes.T
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

class LocalitySensitiveHashing:
    """
    Performs locality-sensitive hashing by projecting unit vectors to binary vectors
    """

    # intended members
    # _D: int number of random hyperplanes
    # _k: int hash function length
    # _L: int number of hash functions (tables)
    # _hash_functions numpy integer array, the actual hash functions
    # _random_hyperplanes: RandomHyperplanes random hyperplanes object
    # _H: list of dicts from binary vectors to sets of integers, hash tables
    # _X: numpy array, the original data
    
    def __init__(self, D, k, L, seed = None):
        """
        Sets the parameters
        - D internal dimensionality (used with random hyperplanes)
        - k length of hash functions (how many elementary hash functions 
          to concatenate)
        - L number of hash tables
        - seed random number generator seed (used for intializing random 
          hyperplanes; also used to seed the random number generator
          for drawing the hash functions)
        """
        self._D = D
        self._k = k
        self._L = L
        rng = np.random.default_rng(seed)
        # draw the hash functions here
        # (essentially, draw a random matrix of shape L*k with values in
        # 0,1,...,D-1)
        # also initialize the random hyperplanes
        self._hash_functions = rng.integers(low=0, high=D, size=(L,k))
        self._random_hyperplanes = RandomHyperplanes(D, seed)
        self._H = [dict() for _ in range(L)]

    def fit(self, X: npt.NDArray[np.float64])->None:
        """
        Fit random hyperplanes
        Then project the dataset into binary vectors
        Then hash the dataset L times into the L hash tables
        """
        self._X = X
        X = self._random_hyperplanes.fit_transform(X)
        
        for i in range(self._L):
            table = self._H[i]
            hash_function = self._hash_functions[i]
            for j in range(X.shape[0]):
                key = tuple(X[j, hash_function]) #reduce to hash function
                if key not in table:
                    table[key] = set()
                table[key].add(j)

    def query(self, q: npt.NDArray[np.float64])->npt.NDArray[np.int64]:
        """
        Queries one vector
        Returns the *indices* of the nearest neighbors in descending order
        That is, if the returned array is I, then X[I[0]] is the nearest 
        neighbor (if the vector was member of the dataset, then typically 
        this would be itself), X[I[1]] the second nearest etc.
        """

        # Project the query into a binary vector
        # Then hash it L times
        # Collect all indices from the hash buckets
        # Then compute the dot products with those vectors
        # Finally sort results in *descending* order and return the indices
        q_fitted = self._random_hyperplanes.transform(q)

        I = set()
        for i in range(self._L):
            table = self._H[i]
            hash_function = self._hash_functions[i]
            key = tuple(q_fitted[hash_function])
            if key in table:
                I = I.union(table[key])

        results=[]
        for i in I:
            results.append([np.dot(self._X[i], q), i])
        results.sort(reverse=True)
        return [result[1] for result in results]

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-D', help='Random hyperplanes dimension', type=int,
                            required = True)
    parser.add_argument('-k', help='Hash function length', type=int,
                            required = True)
    parser.add_argument('-L', help='Number of hash tables (functions)', type=int,
                            required = True)
    parser.add_argument('dataset', help='Glove dataset filename',
                            type=str)
    parser.add_argument('queries', help='Queries filename', type=str)
    args = parser.parse_args()
    
    (word_to_idx, idx_to_word, X) = load_glove(args.dataset)


    X = normalize(X)

    (Q,queries) = construct_queries(args.queries, word_to_idx, X)

    t1 = time.time()
    lsh = LocalitySensitiveHashing(args.D, args.k, args.L, 1234)
    
    t2 = time.time()    
    lsh.fit(X)
    
    t3 = time.time()
    neighbors = list()
    for i in range(Q.shape[0]):
        q = Q[i,:]
        I = lsh.query(q)
        neighbors.append([idx_to_word[i] for i in I][1:4])
    t4 = time.time()

    print('init took',t2-t1)
    print('fit took', t3-t2)
    print('query took', t4-t3)
    print('total',t4-t1)

    for i in range(Q.shape[0]):
        print(f'{queries[i]}: {" ".join(neighbors[i])}') 

