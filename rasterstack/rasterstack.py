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


def _linestats(fl, stats, band, rchunk, w, h, nodatavalue, l):
    if (l + rchunk) >= h:
        chunk = h - l
    else:
        chunk = rchunk
    
    win = ((l, l + chunk), (None, None))
    x = np.zeros((len(fl), chunk, w), dtype = np.float32)
    for i, f in enumerate(fl):
        with rasterio.open(f) as src:
            x[i,:,:] = src.read(band, window = win).astype(np.float32)
            profile = src.profile

    x[np.where(x == nodatavalue)] = np.nan
    x[np.where(np.isinf(x))] = np.nan
    
    xco = None
    xme = None
    xmd = None
    xst = None
    
    # nobs
    if 'nobs' in stats:
        xco = np.isfinite(x).sum(axis = 0).reshape((chunk, w)).astype(np.int16)
    
    # mean
    if 'mean' in stats:
        xme = np.nanmean(x, axis = 0)
        xme[np.isnan(xme)] = nodatavalue
        xme = xme.reshape((chunk, w)).astype(profile['dtype'])
    
    # median
    if 'median' in stats:
        xmd = np.nanmedian(x, axis = 0)
        xmd[np.isnan(xmd)] = nodatavalue
        xmd = xmd.reshape((chunk, w)).astype(profile['dtype'])

    # std
    if 'std' in stats:
        xst = np.nanstd(x, axis = 0)
        xst[np.isnan(xst)] = nodatavalue
        xst = xst.reshape((chunk, w)).astype(profile['dtype'])

    return xco, xme, xmd, xst
    
def _compute_stats(fl, stats = ['nobs', 'mean', 'median', 'std'], band = 1, outfile = None, rchunk = 100, njobs = 1, verbose = 0):
    
    if not equalExtents(fl):
        raise ValueError("Rasters do not have aligned extents.")
    if not isinstance(stats, list):
        stats = [stats]

    with rasterio.open(fl[0]) as src:
        profile = src.profile
    w = profile['width']
    h = profile['height']
    nodatavalue = profile['nodata']
       
    fn = partial(_linestats, fl, stats, band, rchunk, w, h, nodatavalue)
    if njobs > 1:
        Z = Parallel(n_jobs = njobs, verbose = verbose)(delayed(fn)(i) for i in range(0, h, rchunk))
    else:
        Z = [fn(i) for i in range(0, h, rchunk)]
    
    if profile['dtype'] in [np.float32, np.float64]:
        dtypeout = profile['dtype']
    else:
        dtypeout = np.int16
    
    # returned stats in order requested
    out = []
    for s in stats:
        if s == 'nobs':
            out.append(np.concatenate([z[0] for z in Z], axis = 0).astype(dtypeout))
        elif s == 'mean':
            out.append(np.concatenate([z[1] for z in Z], axis = 0).astype(dtypeout))
        elif s == 'median':
            out.append(np.concatenate([z[2] for z in Z], axis = 0).astype(dtypeout))
        elif s == 'std':
            out.append(np.concatenate([z[3] for z in Z], axis = 0).astype(dtypeout))
    
    if outfile:
        if len(stats) == 1:
            z = out.reshape((1, out.shape[0], out.shape[1]))
        else:
            z = np.stack(out)
        profile.update(count = len(stats), dtype = dtypeout, compress = 'lzw')
        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(z)

    return out
     
    
def imageExtent(f):
    '''
    f: single raster filename
    
    returns: image extent as (xmin, ymin, xmax, ymax)
    '''
    with rasterio.open(f) as src:
        aff = src.profile['transform']
        w = src.profile['width']
        h = src.profile['height']
    xmin = aff[2]
    ymax = aff[5]
    xmax = aff[2] + (h * aff[0])
    ymin = aff[5] + (w * aff[4])
    
    return xmin, ymin, xmax, ymax
    

def imageCRS(f):
    '''
    f: single raster filename
    returns a crs string
    '''
    with rasterio.open(f) as src:
        crs = src.profile['crs']['init']
    
    return crs
    

def equalExtents(fl, check_crs = True):
    '''
    Returns True if all extents of all rasters are aligned, False otherwise
    '''
    
    if check_crs:
        crs = [imageCRS(f) for f in fl]
        if len(list(set(crs))) > 1:
            raise ValueError("More than one unique CRS found in file list.")
    
    E = [imageExtent(f) for f in fl]
    test = [
        len(set([e[0] for e in E])),
        len(set([e[1] for e in E])),
        len(set([e[2] for e in E])),
        len(set([e[3] for e in E]))
        ]
    return all([t == 1 for t in test])    
    
def checkProjections(fl): ## TODO
    pass
    
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
    

def cropToExtent(f, targ_e, res = 30, outdir = None, suffix = 'crop', check_if_empty = False):
    '''
    Arguments
    ---------
    f:              raster filename
    targ_e:         target extent as [xmin, ymin, xmax, ymax]; assumed to be in same srs)
    res:            resolution (default = 30m as in Landsat)
    outdir:         ouput directory if writing to file [None]
    suffix:         filename suffix if writing to file ['crop']
    check_if_empty: avoid writing if no valid data [False]
    '''
    with rasterio.open(f) as src:
        src_profile = src.profile
        src_aff = src.profile['transform']
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
            'transform': targ_aff,
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
        
        write = True
        
        if check_if_empty:
            if np.all(targ == src_profile['nodata']):
                write = False
        if write:
            with rasterio.open(outfile, 'w', **targ_profile) as dst:
                dst.write(targ)
            
    return targ

def _cropToExtent(targ_e, res, outdir, suffix, check_if_empty, f):
    return cropToExtent(f, targ_e, res, outdir, suffix, check_if_empty)

def batchCropToExtent(fl, targ_e, outdir = None, suffix = 'crop', res = 30, njobs = 1, verbose = 0, check_if_empty = False):
    '''
    Crops a list of rasters
    '''
    
    if not os.path.exists(outdir):
        raise ValueError("%s does not exist" % outdir)
    
    if njobs == 1:
        Z = [cropToExtent(f, targ_e, res, check_if_empty = check_if_empty) for f in fl]
    else:
        fn = partial(_cropToExtent, targ_e, res, outdir, suffix, check_if_empty)
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

def count_nobs(f):
    '''
    Count # non-NA observations in a raster
    Arguments
    ---------
    f:  raster filename
    '''
    with rasterio.open(f) as src:
        nodata = src.profile['nodata']
        count = src.profile['count']
        nobs = []
        for i in range(count):
            x = src.read(i+1)
            nobs.append((x == nodata).sum())

    if count == 1:
        return nobs[0]
    else:
        return nobs
 
def _get_season(doy):
    if doy >= 355 or doy < 81:
        return 'winter'
    elif doy >= 265:
        return 'autumn'
    elif doy >= 173:
        return 'summer'
    else:
        return 'spring'
    
class RasterTimeSeries(object):
    '''
    Arguments
    ---------
    fl:    List of filenames pointing to rasters
    dates: List of datetime.datetime objects corresponding to each files in fl
    '''
    def __init__(self, fl, dates):
        
        if len(dates) != len(fl):
            raise ValueError("Dates should be the same length as self.data")
        
        if not equalExtents(fl):
            raise ValueError("Input rasters should have the same extent.")
        
        self.data = DataFrame({'filename': fl, 'date': dates})
              
        self.data = self.data.assign(
            year = [ int(datetime.strftime(d, "%Y")) for d in self.data['date'] ],
            month = [ int(datetime.strftime(d, "%m")) for d in self.data['date'] ],
            doy = [ int(datetime.strftime(d, "%j")) for d in self.data['date'] ],
            nobs = [None] * len(fl)
        )
        
        self.data = self.data.assign(
            season = [_get_season(d) for d in self.data['doy']],
            quarter = [int(d / 92) + 1 for d in self.data['doy']]
        )
        
        self.data.sort_values('date', inplace = True)
        self.data.reset_index(drop = True, inplace = True)  
        self.extent = imageExtent(fl[0])
        self.profile = rasterio.open(fl[0]).profile
        
        
    def compute_stats(self, band = 1, months = None, years = None, doys = None, seasons = None, quarters = None, stats = ['nobs', 'mean', 'median', 'std'], outfile = None, **kwargs):
        '''
        Compute overall stats

        Arguments
        ---------
        band:       band to open when computing stats
        months:     list of months (integer 1-12) for monthly/seasonal subset [None]. See details for restrictions.
        years:      list of years for annual subset [None]
        doys:       list of days (1-366) for DOY subset [None]. See details for restrictions.
        seasons:    one of 'winter', 'spring', 'summer' or 'autumn' (defined for the Northern Hemisphere). See details for restrictions.
        quarters:   list of quarters between 1 and 4. See details for restrictions.
        stats:      stats to be computed (must be one or more of ['nobs', 'mean', 'median', 'std']
        outfile:    (optional) output filename (multi-band raster where number of bands = len(stats))
        
        Keyword arguments (kwargs)
        --------------------------
        rchunk:     number of rows to process at a time [100]
        njobs:      number of jobs (for parallel processing) [1]
        verbose:    verbosity (0-100) [0]
        
        Details:
        --------
        The 'years' argument can be combined with other subsetting arguments to get (e.g.) all 1st quarter imagery for a given range of years. However, other sub-annual subsetting arguments cannot be used together (e.g., passing arguments to both 'months' and 'quarters' will return an error).
        '''
        if not isinstance(stats, list):
            stats = [stats]
        if not all(s in ['nobs', 'mean', 'median', 'std'] for s in stats):
            raise ValueError("'stats' must be one or more of ['nobs', 'mean', 'median', 'std']")
        
        if sum([months != None, doys != None, quarters != None, seasons != None]) > 1:
            raise ValueError("Only one of months, doys, quarters or seasons can be set.")
        
        df = self.data.assign(subset = [True] * len(self.data))
        
        if seasons != None:
            if not isinstance(seasons, list):
                seasons = [seasons]
            if not all(s in ['winter', 'spring', 'summer', 'autumn'] for s in seasons):
                raise ValueError("'seasons' must be 1 or more of ['winter', 'spring', 'summer', 'autumn']")
            for i in range(len(df)):
                if not df.loc[i, 'season'] in seasons:
                    df.loc[i, 'subset'] = False
        
        if months != None:
            if not isinstance(months, list):
                months = [months]
            if not all(m < 13 for m in months):
                raise ValueError("Months must be between 1 and 12 inclusive")
            for i in range(len(df)):
                if not df.loc[i, 'month'] in months:
                    df.loc[i, 'subset'] = False
        
        if years != None:
            if not isinstance(years, list):
                years = [years]
            for i in range(len(df)):
                if not df.loc[i, 'year'] in years:
                    df.loc[i, 'subset'] = False
                    
        if doys != None:
            if not all(d < 367 for d in doys):
                raise ValueError("DOYs must be between 1 and 366")
            if not isinstance(doys, list):
                raise ValueError("doys must be a list of DOYs")
            for i in range(len(df)):
                if not df.loc[i, 'doy'] in doys:
                    df.loc[i, 'subset'] = False
        
        df = df[df['subset']]
        if len(df) == 0:
            raise ValueError("No data left after subsetting.")
        
        df.sort_values('date', inplace = True)
        df.reset_index(inplace = True, drop = True)
        
        return _compute_stats(df['filename'], band = band, stats = stats, outfile = outfile, **kwargs)
        
    def subset_by_date(self, date, inplace = False):
        pass
    
    def count_obs(self, njobs = 1, verbose = 0):
        ## TODO: fix for multi-band rasters ##
        self.data['nobs'] = Parallel(n_jobs = njobs, verbose = verbose)(delayed(count_nobs)(f) for f in self.data['filename'])
        
        
        