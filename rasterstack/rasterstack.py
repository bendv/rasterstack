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


class RasterStack(object):
    def __init__(self, fl):
        if not equalExtents(fl):
            raise ValueError("Input rasters should have the same extent.")

        self.data = DataFrame({'filename': fl, 'nobs': [None] * len(fl)})
        self.extent = imageExtent(fl[0])
        self.profile = rasterio.open(fl[0]).profile

    def compute_stats(self, band = 1, stats = ['nobs', 'mean', 'median', 'std'], outfile = None, maskband = None, maskvalue = 1, **kwargs):
        '''
        Compute pixel-based descriptive stats

        Arguments
        ---------
        band:       band to open when computing stats
        stats:      stats to be computed (must be one or more of ['nobs', 'mean', 'median', 'std']
        outfile:    (optional) output filename (multi-band raster where number of bands = len(stats))
        maskband:   (optional) integer band number for mask band
        maskvalue:  (optional) value in mask band to be masked (Default: 1)

        Keyword arguments (kwargs)
        --------------------------
        rchunk:     number of rows to process at a time [100]
        njobs:      number of jobs (for parallel processing) [1]
        verbose:    verbosity (0-100) [0]
        '''
        if maskband == band:
            raise ValueError("band number and maskband number should not be the same.")

        return _compute_stats(self.data['filename'], band = band, stats = stats, outfile = outfile, maskband = maskband, maskvalue = maskvalue, **kwargs)

    def count_obs(self, njobs = 1, verbose = 0):
        '''
        Assigns the number of non-nodata pixels to the 'nobs' column of the (meta)dataframe
        '''
        self.data['nobs'] = Parallel(n_jobs = njobs, verbose = verbose)(delayed(count_nobs)(f) for f in self.data['filename'])


class SingleFileRasterStack(object):
    '''
    This is a placeholder until I figure out how to best handle multi-file/single-file stacks as one object...
    '''
    def __init__(self, infile):
        self.filename = infile
        #self.extent = imageExtent(infile)
        self.profile = rasterio.open(infile).profile
    
    def compute_stats(self, stats = ['nobs', 'mean', 'median', 'std'], outfile = None, **kwargs):
        '''
        Compute pixel-based descriptive stats

        Arguments
        ---------
        stats:      stats to be computed (must be one or more of ['nobs', 'mean', 'median', 'std']
        outfile:    (optional) output filename (multi-band raster where number of bands = len(stats))

        Keyword arguments (kwargs)
        --------------------------
        rchunk:     number of rows to process at a time [100]
        njobs:      number of jobs (for parallel processing) [1]
        verbose:    verbosity (0-100) [0]
        '''
        return _compute_stats_single(self.filename, stats = stats, outfile = outfile, **kwargs)   
        
        
        
        
class RasterTimeSeries(RasterStack):
    '''
    Arguments
    ---------
    fl:    List of filenames pointing to rasters
    dates: List of datetime.datetime objects corresponding to each file in fl
    
    TODO: allow for single file (e.g., NETCDF4, GRD) to be read as multi-band time series raster
    '''
    def __init__(self, fl, dates):
        
        if len(dates) != len(fl):
            raise ValueError("dates should be the same length as fl")

        RasterStack.__init__(self, fl)
                      
        self.data = self.data.assign(
            date = dates,
            year = [ int(datetime.strftime(d, "%Y")) for d in dates ],
            month = [ int(datetime.strftime(d, "%m")) for d in dates ],
            doy = [ int(datetime.strftime(d, "%j")) for d in dates ],
        )
        
        self.data = self.data.assign(
            season = [_get_season(d) for d in self.data['doy']],
            quarter = [int(d / 92) + 1 for d in self.data['doy']]
        )
        
        self.data.sort_values('date', inplace = True)
        self.data.reset_index(drop = True, inplace = True)
        
        
    def compute_stats(self, band = 1, months = None, years = None, doys = None, seasons = None, quarters = None, stats = ['nobs', 'mean', 'median', 'std'], outfile = None, maskband = None, maskvalue = 1, **kwargs):
        '''
        Compute pixel-based descriptive stats

        Arguments
        ---------
        band:       band to open when computing stats
        months:     list of months (integer 1-12) for monthly/seasonal subset [None]. See details for restrictions.
        years:      list of years for annual subset [None]
        doys:       list of days (1-366) for DOY subset [None]. See details for restrictions.
        seasons:    one of 'winter', 'spring', 'summer' or 'autumn' (defined for the Northern Hemisphere). See details for restrictions.
        quarters:   list of quarters between 1 and 4. See details for restrictions.
        stats:      stats to be computed (must be one or more of ['nobs', 'mean', 'median', 'std']
        maskband:   (optional) integer band number to be used for masking
        maskvalue:  (optional) value in mask band to be masked (Default: 1)
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
            for i in df.index:
                if not df.loc[i, 'season'] in seasons:
                    df.loc[i, 'subset'] = False
        
        if months != None:
            if not isinstance(months, list):
                months = [months]
            if not all(m < 13 for m in months):
                raise ValueError("Months must be between 1 and 12 inclusive")
            for i in df.index:
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
            for i in df.index:
                if not df.loc[i, 'doy'] in doys:
                    df.loc[i, 'subset'] = False

        if quarters != None:
            if not isinstance(quarters, list):
                quarters = [quarters]
            if not all(q in [1,2,3,4] for q in quarters):
                raise ValueError("quarters must be a list containing one or more of [1,2,3,4]")
            for i in df.index:
                if not df.loc[i, 'quarter'] in quarters:
                    df.loc[i, 'subset'] = False
        
        df = df[df['subset']]
        if len(df) == 0:
            raise ValueError("No data left after subsetting.")
        
        df.sort_values('date', inplace = True)
        df.reset_index(inplace = True, drop = True)
        
        return _compute_stats(df['filename'], band = band, stats = stats, outfile = outfile, maskband = maskband, maskvalue = maskvalue, **kwargs)
        
    def subset_by_date(self, date, inplace = False):
        pass
        
    def update_metadata(self):
        '''
        Use this in methods where data.frame changes
        '''
        pass
        
        
        
## helper functions
    
def _linestats(fl, stats, band, maskband, maskvalue, rchunk, w, h, nodatavalue, l):
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
            if maskband:
                mask = src.read(maskband, window = win)
                x[i][np.where(mask == maskvalue)] = np.nan


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
    
def _compute_stats(fl, stats = ['nobs', 'mean', 'median', 'std'], band = 1, maskband = None, maskvalue = None, outfile = None, rchunk = 100, njobs = 1, verbose = 0):
    
    if not equalExtents(fl):
        raise ValueError("Rasters do not have aligned extents.")
    if not isinstance(stats, list):
        stats = [stats]

    with rasterio.open(fl[0]) as src:
        profile = src.profile
    w = profile['width']
    h = profile['height']
    nodatavalue = profile['nodata']
       
    fn = partial(_linestats, fl, stats, band, maskband, maskvalue, rchunk, w, h, nodatavalue)
    if njobs > 1:
        Z = Parallel(n_jobs = njobs, verbose = verbose)(delayed(fn)(i) for i in range(0, h, rchunk))
    else:
        Z = [fn(i) for i in range(0, h, rchunk)]
    
    if profile['dtype'] in [np.uint8, rasterio.uint8]:
        dtypeout = np.int16
    else:
        dtypeout = profile['dtype']
    
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
 
def _get_season(doy):
    if doy >= 355 or doy < 81:
        return 'winter'
    elif doy >= 265:
        return 'autumn'
    elif doy >= 173:
        return 'summer'
    else:
        return 'spring'



## temp

def _linestats_single(infile, stats, rchunk, w, h, nodatavalue, l):
    if (l + rchunk) >= h:
        chunk = h - l
    else:
        chunk = rchunk
    
    with rasterio.open(infile) as src:
        profile = src.profile
        win = ((l, l + chunk), (None, None))
        x = src.read(window = win).astype(np.float32)

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

def _compute_stats_single(infile, stats = ['nobs', 'mean', 'median', 'std'], outfile = None, rchunk = 100, njobs = 1, verbose = 0):
    
    if not isinstance(stats, list):
        stats = [stats]

    profile = rasterio.open(infile).profile
    w = profile['width']
    h = profile['height']
    nodatavalue = profile['nodata']
       
    fn = partial(_linestats_single, infile, stats, rchunk, w, h, nodatavalue)
    if njobs > 1:
        Z = Parallel(n_jobs = njobs, verbose = verbose)(delayed(fn)(i) for i in range(0, h, rchunk))
    else:
        Z = [fn(i) for i in range(0, h, rchunk)]
    
    if profile['dtype'] in [np.uint8, rasterio.uint8]:
        dtypeout = np.int16
    else:
        dtypeout = profile['dtype']
    
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