cimport cython
from cython.view cimport array as cvarray
from cython.parallel cimport prange, parallel
from libc.stdlib cimport qsort
from libc.math cimport sqrt, exp
from openmp cimport omp_get_thread_num, omp_get_max_threads
import numpy as np
cimport numpy as np

cdef const double PI = 3.14159

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cdef double[:,:] _RBF(double[:,:] arr_view, double[:] t_view, double[:,:] new_arr, double[:] new_t, double[:,:] w, double sigma int nthreads) noexcept:
'''
arr_view: 2D array [time, space.flatten()]
t_view: time index (in days from start or DOY's)
new_arr: pre-allocated array to be filled in with interpolated values
new_t: new time index for interpolated series
w: matrix of weights (nt, ntnew)
sigma: free parameter used to compute weight (Gaussian)
'''
    cdef:
        double[:,:] arr_c = arr_view.copy()
        Py_ssize_t nt = arr_c.shape[0]
        Py_ssize_t nx = arr_c.shape[1]
        Py_ssize_t ntnew = new_t.shape[0]
        int x, i, j
        int tid # https://stackoverflow.com/questions/42281886/cython-make-prange-parallelization-thread-safe
        double dsq, k
        double wsum

    # vector of temporal weights
    wsum = 0
    for i in range(nt):
        dsq = (t_view[i] - t_view[j])**2
        w[i] = exp(-1 * dsq / (2 * sigma**2))
        wsum += w[i]
    for i in range(nt):
        w[i] = w[i] / wsum
       
    with nogil, parallel(num_threads = nthreads):
        tid = omp_get_thread_num()
        for x in prange(nx, schedule = 'static', chunksize = 1):
            ## temporal interpolation ##
            for j in range(ntnew):
                for i in range(nt):
                    pass

        
    return res