import numpy as np
import cupy as cp
import argparse
import pandas as pd
import csv
import sys
import time

def efficient_norm(Q, X):
    """
    Computes the squared Euclidean distance between each row of Q and each row of X.
    Returns an m*n matrix where the (i,j) entry is ||X[j,:] - Q[i,:]||^2.
    """

    X_norms = cp.sum(X**2, axis=1).reshape(-1, 1) 
    Q_norms = cp.sum(Q**2, axis=1).reshape(1, -1) 
    
    dot = X @ Q.T 
    D = X_norms + Q_norms - 2 * dot 

    return D

def linear_scan(X, Q, b = None):
    """
    Perform linear scan for querying nearest neighbor.
    X: n*d dataset
    Q: m*d queries
    b: optional batch size (ignored in this implementation)
    Returns an m-vector of indices I; the value i reports the row in X such 
    that the Euclidean norm of ||X[I[i],:]-Q[i]|| is minimal
    """
    I = cp.zeros(Q.shape[0], dtype=cp.int64)

    X_gpu = cp.asarray(X, blocking=True)

    if b is None:
        Q_gpu = cp.asarray(Q, blocking=True)
        distances = efficient_norm(Q_gpu, X_gpu)
        I = cp.argmin(distances, axis=0)
    else:
        for i in range(0, Q.shape[0], b):
            Q_gpu_batch = cp.asarray(Q[i:i+b], blocking=True)
            distances = efficient_norm(Q_gpu_batch, X_gpu)
            if i+b > Q.shape[0]:
                I[i:]  = cp.argmin(distances, axis=0)
            else:
                I[i:i+b] = cp.argmin(distances, axis=0)

    cp.cuda.Stream.null.synchronize()
    I_cpu = cp.asnumpy(I)
    return I_cpu

def load_glove(fn):
    """
    Loads the glove dataset from the file
    Returns (X,L) where X is the dataset vectors and L is the words associated
    with the respective rows.
    """
    df = pd.read_table(fn, sep = ' ', index_col = 0, header = None,
                           quoting = csv.QUOTE_NONE, keep_default_na = False)
    X = np.ascontiguousarray(df, dtype = np.float32)
    L = df.index.tolist()
    return (X, L)

def load_pubs(fn):
    """
    Loads the pubs dataset from the file
    Returns (X,L) where X is the dataset vectors (easting,northing) and 
    L is the list of names of pubs, associated with each row
    """
    df = pd.read_csv(fn)
    L = df['name'].tolist()
    X = np.ascontiguousarray(df[['easting','northing']], dtype = np.float32)
    return (X, L)

def load_queries(fn):
    """
    Loads the m*d array of query vectors from the file
    """
    return np.loadtxt(fn, delimiter = ' ', dtype = np.float32)

def load_query_labels(fn):
    """
    Loads the m-long list of correct query labels from a file
    """
    with open(fn,'r') as f:
        return f.read().splitlines()

if __name__ == '__main__':
    parser = argparse.ArgumentParser( \
          description = 'Perform nearest neighbor queries under the '
          'Euclidean metric using linear scan, measure the time '
          'and optionally verify the correctness of the results')
    parser.add_argument(
        '-d', '--dataset', type=str, required=True,
        help = 'Dataset file (must be pubs or glove)')
    parser.add_argument(
        '-q', '--queries', type=str, required=True,
        help = 'Queries file (must be compatible with the dataset)')
    parser.add_argument(
        '-l', '--labels', type=str, required=False,
        help = 'Optional correct query labels; if provided, the correctness '
        'of returned results is checked')
    parser.add_argument(
        '-b', '--batch-size', type=int, required=False,
        help = 'Size of batches')
    args = parser.parse_args()

    t1 = time.time()
    if 'pubs' in args.dataset:
        (X,L) = load_pubs(args.dataset)
    elif 'glove' in args.dataset:
        (X,L) = load_glove(args.dataset)
    else:
        sys.stderr.write(f'{sys.argv[0]}: error: Only glove/pubs supported\n')
        exit(1)
    t2 = time.time()

    (n,d) = X.shape
    assert len(L) == n

    t3 = time.time()
    Q = load_queries(args.queries)
    t4 = time.time()

    assert X.flags['C_CONTIGUOUS']
    assert Q.flags['C_CONTIGUOUS']
    assert X.dtype == np.float32
    assert Q.dtype == np.float32
    
    m = Q.shape[0]
    assert Q.shape[1] == d

    t5 = time.time()
    QL = None
    if args.labels is not None:
        QL = load_query_labels(args.labels)
        assert len(QL) == m
    t6 = time.time()
    I = linear_scan(X,Q,args.batch_size)
    t7 = time.time()
    assert I.shape == (m,)

    num_erroneous = 0
    if QL is not None:
        for (i,j) in enumerate(I):
            if QL[i] != L[j]:
                sys.stderr.write(f'{i}th query was erroneous: got "{L[j]}", '
                                     f'but expected "{QL[i]}"\n')
                num_erroneous += 1

    print(f'Loading dataset ({n} vectors of length {d}) took', t2-t1)
    print(f'Performing {m} NN queries took', t7-t6)
    print(f'Number of erroneous queries: {num_erroneous}')
