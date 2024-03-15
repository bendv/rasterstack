cimport cython
from cython.view cimport array as cvarray
from cython.parallel cimport prange, parallel
from libc.stdlib cimport qsort
from libc.math cimport sqrt, exp, log, abs
from openmp cimport omp_get_thread_num, omp_get_max_threads
import numpy as np
cimport numpy as np

@cython.wraparound(False)
@cython.cdivision(True)
cdef double _tps(double r) noexcept nogil:
    if r == 0:
        return 0
    else:
        return r**2*log(r)

@cython.wraparound(False)
@cython.cdivision(True)
cdef double _gaussian(double r, double epsilon) noexcept nogil:
    return exp(-1*((epsilon*r)**2))


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cdef double[:,:] _RBF(double[:,:] arr_view, double[:] t_view, double[:] new_t, double epsilon, int nthreads) noexcept:
    '''
    arr_view: 2D array [time, space.flatten()]
    t_view: time index (in days from start or DOY's)
    new_arr: pre-allocated array to be filled in with interpolated values [new time, space]
    new_t: new time index for interpolated series
    w: array of weights (nt, ntnew)
    nthreads: # of omp threads
    '''
    cdef:
        double[:,:] arr_c = arr_view.copy()
        Py_ssize_t nt = arr_c.shape[0]
        Py_ssize_t nx = arr_c.shape[1]
        Py_ssize_t ntnew = new_t.shape[0]
        int x, i, j
        int tid # https://stackoverflow.com/questions/42281886/cython-make-prange-parallelization-thread-safe
        double r, k
        
    cdef double[:,:] w = cvarray(shape = (nt,nx), itemsize = sizeof(double), format = "d")
    cdef double[:,:] new_arr = cvarray(shape = (ntnew,nx), itemsize = sizeof(double), format = "d")
    cdef double[:] wsum = cvarray(shape = (nthreads,), itemsize = sizeof(double), format = "d")
       
    with nogil, parallel(num_threads = nthreads):
        tid = omp_get_thread_num()
        for x in prange(nx, schedule = 'static', chunksize = 1):
            for i in range(ntnew):
                wsum[tid] = 0
                for j in range(nt):
                    r = abs(new_t[i] - t_view[j])
                    w[j,x] = _gaussian(r, epsilon)
                    wsum[tid] += w[j,x]
                new_arr[i,x] = 0
                if wsum[tid] > 0:
                    for j in range(nt):
                        w[j,x] = w[j,x] / wsum[tid]
                        new_arr[i,x] += w[j,x] * arr_view[j,x]
                else:
                    pass ###
    
    return new_arr


def RBF():
    pass