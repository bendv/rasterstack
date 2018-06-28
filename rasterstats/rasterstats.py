import numpy as np
import rasterio
from affine import Affine
from rasterio.warp import reproject
from joblib import Parallel, delayed
from datetime import date, datetime, timedelta
from pandas import DataFrame, Series
from collections import OrderedDict
from functools import partial
import os


def _linestats(fl, rchunk, w, h, nodatavalue, l):
    if (l + rchunk) >= h:
        chunk = h - l
    else:
        chunk = rchunk
    
    win = ((l, l + chunk), (None, None))
    x = np.zeros((len(fl), chunk, w), dtype = np.float32)
    for i, f in enumerate(fl):
        with rasterio.open(f) as src:
            x[i,:,:] = src.read(1, window = win).astype(np.float32)
            profile = src.profile

    x[np.where(x == nodatavalue)] = np.nan
    x[np.where(np.isinf(x))] = np.nan
          
    # nobs
    xco = np.isfinite(x).sum(axis = 0)
    xco = xco.reshape((chunk, w)).astype(np.int16)
    
    # mean
    xme = np.nanmean(x, axis = 0)
    xme[np.isnan(xme)] = nodatavalue
    xme = xme.reshape((chunk, w)).astype(profile['dtype'])
    
    # median
    xmd = np.nanmedian(x, axis = 0)
    xmd[np.isnan(xmd)] = nodatavalue
    xmd = xmd.reshape((chunk, w)).astype(profile['dtype'])

    # std
    xst = np.nanstd(x, axis = 0)
    xst[np.isnan(xst)] = nodatavalue
    xst = xst.reshape((chunk, w)).astype(profile['dtype'])

    return xco, xme, xmd, xst
    
def compute_stats(fl, outfile = None, rchunk = 100, njobs = 1, verbose = 0):
    
    if not equalExtents(fl):
        raise ValueError("Rasters do not have aligned extents.")

    with rasterio.open(fl[0]) as src:
        profile = src.profile
    w = profile['width']
    h = profile['height']
    nodatavalue = profile['nodata']
       
    fn = partial(_linestats, fl, rchunk, w, h, nodatavalue)
    if njobs > 1:
        Z = Parallel(n_jobs = njobs, verbose = verbose)(delayed(fn)(i) for i in range(0, h, rchunk))
    else:
        Z = [fn(i) for i in range(0, h, rchunk)]
    
    if profile['dtype'] in [np.float32, np.float64]:
        dtypeout = profile['dtype']
    else:
        dtypeout = np.int16
    
    zmn = np.concatenate([z[1] for z in Z], axis = 0).astype(dtypeout)
    zmd = np.concatenate([z[2] for z in Z], axis = 0).astype(dtypeout)
    zst = np.concatenate([z[3] for z in Z], axis = 0).astype(dtypeout)
    zco = np.concatenate([z[0] for z in Z], axis = 0).astype(dtypeout)
    
    if outfile:
        z = np.stack([zmn, zmd, zst, zco])
        profile.update(count = 4, dtype = dtypeout, compress = 'lzw')
        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(z)
    else:
        return zmn, zmd, zst, zco
     
    
def imageExtent(f):
    '''
    f: single raster filename
    
    returns: image extent as (xmin, ymin, xmax, ymax)
    '''
    with rasterio.open(f) as src:
        aff = src.profile['affine']
        w = src.profile['width']
        h = src.profile['height']
    xmin = aff[2]
    ymax = aff[5]
    xmax = aff[2] + (h * aff[0])
    ymin = aff[5] + (w * aff[4])
    
    return xmin, ymin, xmax, ymax

def equalExtents(fl):
    '''
    Returns True if all extents of all rasters are aligned, False otherwise
    '''
    E = [imageExtent(f) for f in fl]
    test = [
        len(set([e[0] for e in E])),
        len(set([e[1] for e in E])),
        len(set([e[2] for e in E])),
        len(set([e[3] for e in E]))
        ]
    return all([t == 1 for t in test])    
    
    
def unionExtent(fl, njobs = 1, verbose = 0):
    '''
    fl: list of raster filenames
    njobs: # cores (for parallel processing)
    verbose: verbosity (only if njobs > 1)
    
    returns: union extent as (xmin, ymin, xmax, ymax)
    '''
    if njobs > 1:
        E = Parallel(n_jobs = njobs, verbose = verbose)(delayed(imageExtent)(f) for f in fl)
    else:
        E = [imageExtent(f) for f in fl]
    
    xmins = [e[0] for e in E]
    ymins = [e[1] for e in E]
    xmaxs = [e[2] for e in E]
    ymaxs = [e[3] for e in E]
        
    xmin = np.array(xmins).min()
    ymin = np.array(ymins).min()
    xmax = np.array(xmaxs).max()
    ymax = np.array(ymaxs).max()
    
    return xmin, ymin, xmax, ymax
    

def cropToExtent(f, targ_e, res = 30, outdir = None, suffix = 'crop'):
    '''
    f: raster filename
    targ_e: target extent as [xmin, ymin, xmax, ymax]; assumed to be in same srs)
    res: resolution (default = 30m as in Landsat)
    '''
    with rasterio.open(f) as src:
        src_profile = src.profile
        src_aff = src.profile['affine']
        src_srs = src.profile['crs']
        targ_srs = src.profile['crs']
        x = src.read()
    
    targ_h = (targ_e[3] - targ_e[1])/res
    targ_w = (targ_e[2] - targ_e[0])/res
    if (targ_h % 1 != 0) or (targ_w % 1 != 0):
        raise ValueError("Extent and resolution do not produce integer width and/or height.")
    else:
        targ_h = int(targ_h)
        targ_w = int(targ_w)
    targ_shape = (src_profile['count'], targ_h, targ_w)
    targ = np.zeros(targ_shape, dtype = src_profile['dtype'])
    
    targ_aff = Affine(
        res, 0, targ_e[0],
        0, -1*res, targ_e[3]
    )
    
    reproject(x, targ, src_transform = src_aff, dst_transform = targ_aff, src_nodata = src_profile['nodata'], dst_nodata = src_profile['nodata'])
    
    if outdir:
        outfile = "{0}/{1}_{2}.tif".format(outdir, os.path.splitext(os.path.basename(f))[0], suffix)
        targ_profile = {
            'affine': targ_aff,
            'width': targ_w,
            'height': targ_h,
            'crs': src_srs,
            'blockxsize': targ_w,
            'blockysize': 1,
            'compress': 'lzw',
            'nodata': src_profile['nodata'],
            'count': src_profile['count'],
            'dtype': src_profile['dtype'],
            'driver': 'GTiff'
        }

        with rasterio.open(outfile, 'w', **targ_profile) as dst:
            dst.write(targ)
            
    return targ

def _cropToExtent(targ_e, res, outdir, suffix, f):
    return cropToExtent(f, targ_e, res, outdir, suffix)

def batchCropToExtent(fl, targ_e, outdir = None, suffix = 'crop', res = 30, njobs = 1, verbose = 0):
    '''
    Crops a list of rasters
    '''
    
    if not os.path.exists(outdir):
        raise ValueError("%s does not exist" % outdir)
    
    if njobs == 1:
        Z = [cropToExtent(f, targ_e, res) for f in fl]
    else:
        fn = partial(_cropToExtent, targ_e, res, outdir, suffix)
        Z = Parallel(n_jobs = njobs, verbose = verbose)(delayed(fn)(f) for f in fl)
    
    if not outdir:
        return np.concatenate(Z, axis = 0)
    else:
        Z = None
        return ["{0}/{1}_{2}.tif".format(outdir, os.path.splitext(os.path.basename(f))[0], suffix) for f in fl]
        
    
def tileExtent(e, dx, dy, res = 30):
    '''
    e: list of [xmin, ymin, xmax, ymax]
    dx, dy: nominal tile extent (in crs units)
    
    returns: a DataFrame with tile indices and associated extents in crs coordinates
    '''
    w = (e[2] - e[0])
    h = (e[3] - e[1])
    if ( (w/res) % 1 ) * ( (h/res) % 1 ) != 0:
        raise ValueError("Extent and resolution do not produce integer width and/or height.")
       
    xmins = np.arange(e[0], e[2], dx)
    ymins = np.arange(e[1], e[3], dy)
   
    xmin = []
    ymin = []
    xmax = []
    ymax = []
    tileid = []

    j = 1
    for y in ymins:
        i = 1
        for x in xmins:
            xmin.append(x)
            ymin.append(y)
            if x + dx > e[2]:
                xmax.append(e[2])
            else:
                xmax.append(x + dx)
            if y + dy > e[3]:
                ymax.append(e[3])
            else:
                ymax.append(y + dy)
            
            tileid.append("{0:02d}-{1:02d}".format(j, i))
            i += 1
        j += 1
       
    tiles = DataFrame(OrderedDict({
        'tile': tileid,
        'xmin': xmin,
        'ymin': ymin,
        'xmax': xmax,
        'ymax': ymax
    }))
    
    tiles['extent'] = [ list(tiles.loc[i, ['xmin', 'ymin', 'xmax', 'ymax']]) for i in range(len(tiles)) ]
            
    return tiles


class RasterTimeSeries(object):
    '''
    Arguments
    ---------
    fl:    List of filenames pointing to rasters
    dates: List of datetime.datetime objects corresponding to each files in fl
    '''
    def __init__(self, fl, dates, sort = True):
        
        if not equalExtents(fl):
            raise ValueError("Input rasters should have the same extent.")
        
        self.data = DataFrame({'filename': fl, 'date': dates})
        self.length = len(fl)
        
        if len(dates) != self.length:
            raise ValueError("Dates should be the same length as self.data")
              
        self.data = self.data.assign(
            year = [ int(datetime.strftime(d, "%Y")) for d in self.data['date'] ],
            month = [ int(datetime.strftime(d, "%m")) for d in self.data['date'] ],
            doy = [ int(datetime.strftime(d, "%j")) for d in self.data['date'] ]
        )
        
        if sort:
            self.data.sort_values('date', inplace = True)
            self.data.reset_index(drop = True, inplace = True)
        
        self.extent = imageExtent(fl[0])
        
        
    def compute_overall_stats(self, **kwargs):
        '''
        Compute overall stats

        Arguments
        ---------
        outfile:    output filename [None]
        rchunk:     number of rows to process at a time [100]
        njobs:      number of jobs (for parallel processing) [1]
        verbose:    verbosity (0-100) [0]
        '''
        zmn, zmd, zst, zco = compute_stats(self.data['filename'], **kwargs)
        return zmn, zmd, zst, zco
        
    def compute_annual_stats(self):
        pass
        
    def compute_seasonal_stats(self):
        pass
        
        
