'''
Functions for tiling rasters
'''
import rasterio
import numpy as np
from joblib import Parallel, delayed
from functools import partial
from collections import OrderedDict
from pandas import DataFrame

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
    

def cropToExtent(f, targ_e, res = 30, outdir = None, suffix = 'crop', check_if_empty = False, crs = None):
    '''
    Arguments
    ---------
    f:              raster filename
    targ_e:         target extent as [xmin, ymin, xmax, ymax]; assumed to be in same srs)
    res:            resolution (default = 30m as in Landsat)
    outdir:         ouput directory if writing to file [None]
    suffix:         filename suffix if writing to file ['crop']
    check_if_empty: avoid writing if no valid data [False]
    crs:            coordinate reference system; If None, this will be read using rasterio
    '''
    with rasterio.open(f) as src:
        src_profile = src.profile
        src_aff = src.profile['transform']
        src_srs = src.profile['crs']['init']
        targ_srs = src.profile['crs']['init']
        x = src.read()

    if crs is not None:
        src_srs = crs
        targ_srs = crs
    
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

def _cropToExtent(targ_e, res, outdir, suffix, check_if_empty, crs, f):
    return cropToExtent(f, targ_e, res, outdir, suffix, check_if_empty, crs)

def batchCropToExtent(fl, targ_e, outdir = None, suffix = 'crop', res = 30, njobs = 1, verbose = 0, check_if_empty = False, crs = None):
    '''
    Crops a list of rasters
    '''
    
    if not os.path.exists(outdir):
        raise ValueError("%s does not exist" % outdir)
    
    if njobs == 1:
        Z = [cropToExtent(f, targ_e, res, check_if_empty = check_if_empty, crs = crs) for f in fl]
    else:
        fn = partial(_cropToExtent, targ_e, res, outdir, suffix, check_if_empty, crs)
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