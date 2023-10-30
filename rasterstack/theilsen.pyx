cimport cython
from cython.view cimport array as cvarray
from cython.parallel cimport prange, parallel
from libc.stdlib cimport qsort
from libc.math cimport sqrt
from openmp cimport omp_get_thread_num, omp_get_max_threads
import numpy as np
cimport numpy as np

# new in Cython 3.0 --> `noexcept` is no longer implicit with nogil functions and must be declared for pure cython functions
# noexcept should only be used if certain there is no interaction with python objects in the function (it's pure cython)
# see: https://github.com/scikit-learn/scikit-learn/issues/25609

# http://nicolas-hug.com/blog/cython_notes

## sort array 
# https://stackoverflow.com/questions/38254146/sort-memoryview-in-cython
cdef int cmp_func(const void* a, const void* b) noexcept nogil:
    cdef double a_v = (<double*>a)[0]
    cdef double b_v = (<double*>b)[0]
    if a_v < b_v:
        return -1
    elif a_v == b_v:
        return 0
    else:
        return 1

@cython.boundscheck(False)
cdef void sort_c(double[:] a, Py_ssize_t size) noexcept nogil:
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
    
    
    
### T-S slope, M-K sign index, M-K Var(S)
@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef double[:,:] _theilsen(double[:,:] arr_view, double[:] x_view, int nthreads) noexcept:
    cdef:
        double[:,:] arr_c = arr_view.copy()
        Py_ssize_t n = arr_c.shape[0]
        Py_ssize_t nx = arr_c.shape[1]
        int x, i, j
        int tid # https://stackoverflow.com/questions/42281886/cython-make-prange-parallelization-thread-safe

    # number of possible comparisons
    cdef int ncomps = 0
    for i in range(n):
        ncomps += i+1
    
    # arrays
    cdef:
        double[:] local_slopes = cvarray(shape = (ncomps * nthreads,), itemsize = sizeof(double), format = "d")
        int[:] k = cvarray(shape = (nx,), itemsize = sizeof(int), format = "i") # counter
        int[:] comp = cvarray(shape = (nx,), itemsize = sizeof(int), format = "i") #sign index
        int[:] S = cvarray(shape = (nx,), itemsize = sizeof(int), format = "i") # cumulative sign index
        int[:] g = cvarray(shape = (nx,), itemsize = sizeof(int), format = "i") # # of ties
        double[:] varS = cvarray(shape = (nx,), itemsize = sizeof(double), format = "d") # var(S)
        double[:] Z = cvarray(shape = (nx,), itemsize = sizeof(double), format = "d") # test statistic
        double[:] group = cvarray(shape = (ncomps * nthreads,), itemsize = sizeof(double), format = "d")
        double[:,:] res = cvarray(shape = (3,nx), itemsize = sizeof(double), format = "d")
        double[:,:] v_c  = cvarray(shape = (n,nx), itemsize = sizeof(double), format = "d") ## saved for sorted array, if needed (ties)
        int[:] tp = cvarray(shape = (nx,), itemsize = sizeof(int), format = "i") # for counting ties
        double[:] tied = cvarray(shape = (nx,), itemsize = sizeof(double), format = "d") # for counting ties
    
    with nogil, parallel(num_threads = nthreads):
        tid = omp_get_thread_num()
        for x in prange(nx, schedule = 'static', chunksize = 1):
            k[x] = 0
            S[x] = 0
            g[x] = 0
            for i in range(n-1):
                for j in range(i+1, n):
                    local_slopes[ncomps * tid + k[x]] = (arr_c[j,x] - arr_c[i,x]) / (x_view[j] - x_view[i])
                    comp[x] = cmp_func(&arr_c[j,x], &arr_c[i,x])
                    if comp[x] == 0:
                        g[x] += 1
                    S[x] += comp[x]
                    k[x] += 1
            res[0,x] = median(local_slopes[ ncomps*tid:ncomps*tid+k[x] ])
            res[1,x] = <double> S[x]
            
            ## Var(S)
            varS[x] = 0
            if g[x] > 0:
                v_c[:,x] = arr_c[:,x]
                sort_c(v_c[:,x], n)
                tp[x] = 0
                tied[x] = v_c[0,x]
                for i in range(1, n):
                    if v_c[i,x] == tied[x]:
                        tp[x] += 1
                    else:
                        tied[x] = v_c[i,x]
                        varS[x] += tp[x] * (tp[x] - 1) * (2*tp[x] + 5)
                        tp[x] = 0
                
            varS[x] = (<double>n * (<double>n - 1) * (2*<double>n + 5) - varS[x]) / 18.
            if S[x] > 0:
                Z[x] = (S[x] - 1) / sqrt(varS[x])
            elif S[x] < 0:
                Z[x] = (S[x] + 1) / sqrt(varS[x])
            else:
                Z[x] = 0
            res[2,x] = Z[x]
            
    return res


def theilsen(arr, x = None, int nthreads = -1):
    '''
    Returns the Theil-Sen slope along axis 0 of input axis.

    Args:
    =====
    arr:        Input 3-D array
    x:          Independent variable array. Default: array indexed along axis 0 of arr.
    nthreads:   Number of threads to use (Default: 1)

    Returns:    A 2-D numpy array with the Theil-Sen slope from X
    '''
    if not arr.dtype == np.float64:
        arr = arr.astype(np.float64)
        
    if x is None:
        x = np.arange(arr.shape[0]).astype(np.float64)
    else:
        if x.shape[0] != arr.shape[0]:
            raise ValueError("Independent variable array must have the same shape as axis 0 of dependent array")
        x = x.astype(np.float64)
    
    if nthreads == -1:
        nthreads = omp_get_max_threads()

    cdef Py_ssize_t h = arr.shape[1]
    cdef Py_ssize_t w = arr.shape[2]

    cdef double[:] x_view = x
    cdef double[:,:] arr_view = arr.reshape((arr.shape[0], arr.shape[1] * arr.shape[2]))
    
    cdef np.ndarray[ndim = 2, dtype = np.float64_t] res = np.array(_theilsen(arr_view, x_view, nthreads))
    cdef np.ndarray[ndim = 2, dtype = np.float64_t] ts_slope = res[0].reshape((h, w))
    cdef np.ndarray[ndim = 2, dtype = np.int16_t] mk_sign = res[1].reshape((h, w)).astype(np.int16)
    cdef np.ndarray[ndim = 2, dtype = np.float64_t] mk_Z = res[2].reshape((h, w))

    return ts_slope, mk_sign, mk_Z
