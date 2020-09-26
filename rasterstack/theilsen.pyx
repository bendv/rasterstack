cimport cython
from cython.view cimport array as cvarray
from cython.parallel cimport prange, parallel
from libc.stdlib cimport qsort
from openmp cimport omp_get_thread_num
import numpy as np
cimport numpy as np

# http://nicolas-hug.com/blog/cython_notes

## sort array 
# https://stackoverflow.com/questions/38254146/sort-memoryview-in-cython
cdef int cmp_func(const void* a, const void* b) nogil:
    cdef double a_v = (<double*>a)[0]
    cdef double b_v = (<double*>b)[0]
    if a_v < b_v:
        return -1
    elif a_v == b_v:
        return 0
    else:
        return 1

@cython.boundscheck(False)
cdef void sort_c(double[:] a, Py_ssize_t size) nogil:
    qsort(&a[0], size, sizeof(char), &cmp_func)
    
@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cdef double median(double [:] arr) nogil:
    cdef Py_ssize_t n = arr.shape[0]
    cdef int i, j
    cdef double arr_med

    sort_c(arr, n)
    
    if n % 2 == 0:
        i = <int> (n/2)
        j = i + 1
        arr_med = (<double> arr[i] + <double> arr[j]) / 2
    else:
        i = <int> (n/2) + 1
        arr_med = <double> arr[i]
    
    return arr_med


### T-S slope
@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef double[:] _theilsen(double[:,:] Xview, int nthreads):
    cdef:
        double[:,:] Xc = Xview.copy()
        Py_ssize_t n = Xc.shape[0]
        Py_ssize_t nx = Xc.shape[1]
        int x, i, j
        int tid # https://stackoverflow.com/questions/42281886/cython-make-prange-parallelization-thread-safe
    
    # independent variable: simple index along z-dimension of input array
    # TODO: allow user to input custom independent variable array
        # especially for cases where it is irregular (gaps in time series)
    cdef double[:] idx = cvarray(shape = (nx,), itemsize = sizeof(double), format = "d")
    for i in range(n):
        idx[i] = <double> i
    
    # number of possible comparisons
    cdef int ncomps = 0
    for i in range(n):
        ncomps += i+1
    
    # arrays
    cdef:
        double[:] local_slopes = cvarray(shape = (ncomps * nthreads,), itemsize = sizeof(double), format = "d")
        int[:] k = cvarray(shape = (nx,), itemsize = sizeof(int), format = "i")
        double[:] medslope = cvarray(shape = (nx,), itemsize = sizeof(double), format = "d")
    
    with nogil, parallel(num_threads = nthreads):
        tid = omp_get_thread_num()
        for x in prange(nx, schedule = 'static', chunksize = 1):
            k[x] = 0
            for i in range(n-1):
                for j in range(i+1, n):
                    local_slopes[ncomps * tid + k[x]] = (Xc[j,x] - Xc[i,x]) / (idx[j] - idx[i])
                    k[x] += 1
            medslope[x] = median(local_slopes[ ncomps*tid : ncomps*tid+k[x] ])

    return medslope


#def theilsen(np.ndarray[ndim=3, dtype = np.float64_t] X, int nthreads):
def theilsen(X, int nthreads = 1):
    '''
    Returns the Theil-Sen slope along axis 0 of input axis. Independent variable is assumed to be range(X.shape[0]) (for now...)

    Args:
    =====
    X:          Input 3-D array
    nthreads:   Number of threads to use (Default: 1)

    Returns:    A 2-D numpy array with the Theil-Sen slope from X
    '''
    if not X.dtype == np.float64:
        X = X.astype(np.float64)

    cdef Py_ssize_t h = X.shape[1]
    cdef Py_ssize_t w = X.shape[2]

    cdef double[:,:] Xview = X.reshape((X.shape[0], X.shape[1] * X.shape[2]))
    cdef np.ndarray[ndim = 2, dtype = np.float64_t] res = np.array( _theilsen(Xview, nthreads) ).reshape((h, w))

    return res

