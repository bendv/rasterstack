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
cdef double[:,:] _RBF(double[:,:] arr_view, double[:] t_view, double[:] new_t, double epsilon, double nodatavalue, int nthreads) noexcept:
    '''
    arr_view: 2D array [time, space.flatten()]
    t_view: time index (in days from start or DOY's)
    new_arr: pre-allocated array to be filled in with interpolated values [new time, space]
    new_t: new time index for interpolated series
    w: array of weights (nt, ntnew)
    nthreads: # of omp threads
    '''
    cdef:
        Py_ssize_t nt = arr_view.shape[0]
        Py_ssize_t nx = arr_view.shape[1]
        Py_ssize_t ntnew = new_t.shape[0]
        int x, i, j
        int tid # https://stackoverflow.com/questions/42281886/cython-make-prange-parallelization-thread-safe
        double r
        
    cdef double[:,:] w = cvarray(shape = (nt,nthreads), itemsize = sizeof(double), format = "d")
    cdef double[:,:] new_arr = cvarray(shape = (ntnew,nx), itemsize = sizeof(double), format = "d")
    cdef double[:] wsum = cvarray(shape = (nthreads,), itemsize = sizeof(double), format = "d")
       
    with nogil, parallel(num_threads = nthreads):
        tid = omp_get_thread_num()
        for x in prange(nx, schedule = 'static', chunksize = 1):
            for i in range(ntnew):
                wsum[tid] = 0
                for j in range(nt):
                    r = abs(new_t[i] - t_view[j])               
                    if arr_view[j,x] == nodatavalue:
                        w[j,tid] = 0
                    else:
                        w[j,tid] = _gaussian(r, epsilon)
                    wsum[tid] += w[j,tid]
                if wsum[tid] > 0:
                    new_arr[i,x] = 0
                    for j in range(nt):
                        w[j,tid] = w[j,tid] / wsum[tid]
                        new_arr[i,x] += w[j,tid] * arr_view[j,x]
                else:
                    new_arr[i,x] = nodatavalue
    
    return new_arr


def RBF(arr, t, new_t = None, t_interval = None, double epsilon = 0.1, double nodatavalue = -32768, int nthreads = -1):
    '''
    Interpolates along the z (time) axis using a Gaussian RBF kernel

    Args:
    ====
    arr: Input 3-D array
    t: 1-D time index (e.g., DOY's) associated with arr
    new_t: new time index to be used in interpolation
    t_interval: time interval for interpolated series (may be specified instead of new_t)
    epsilon: Gaussian kernel smoothing factor
    nthreads: number of parallel threads
    '''

    if t.shape[0] != arr.shape[0]:
        raise ValueError("Axis 0 of arr and t must be the same length.")

    dtype = arr.dtype
    if not dtype == np.float64:
        arr = arr.astype(np.float64)
    if not t.dtype == np.float64:
        t = t.astype(np.float64)

    if new_t is None:
        if t_interval is None:
            raise ValueError("Either new_t or t_interval must be specified.")
        new_t = np.arange(t.min(), t.max() + t_interval, t_interval).astype(np.float64)
    
    if nthreads == -1:
        nthreads = omp_get_max_threads()

    cdef Py_ssize_t l = arr.shape[0]
    cdef Py_ssize_t h = arr.shape[1]
    cdef Py_ssize_t w = arr.shape[2]
    cdef Py_ssize_t newl = new_t.shape[0]

    cdef double[:,:] arr_view = arr.reshape((l, h*w))
    cdef double[:] t_view = t
    cdef double[:] new_t_view = new_t

    cdef np.ndarray[ndim = 2, dtype = np.float64_t] res = np.array(_RBF(arr_view, t_view, new_t_view, epsilon, nodatavalue, nthreads))
    cdef np.ndarray[ndim = 3, dtype = np.float64_t] int_stack = res.reshape((newl,h,w)).astype(dtype)

    return int_stack