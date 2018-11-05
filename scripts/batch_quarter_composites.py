#!/usr/bin/env python

from rasterstack import RasterTimeSeries
import rasterio
import numpy as np
import os, sys, glob, warnings
import pandas as pd
from datetime import datetime


def get_files(indir, string):
    fl = []
    for path, dirs, files in os.walk(indir):
        for f in files:
            if string in f and f.endswith('tif'):
                    fl.append(os.path.join(path, f))
    return fl

def get_landsat_dates(fl, precollection=False):
    if precollection:
        dates = [datetime.strptime(os.path.basename(f)[9:16], "%Y%j") for f in fl]
    else:
        dates = [datetime.strptime(os.path.basename(f).split('_')[3], "%Y%m%d") for f in fl]
    return dates

def main(indir, outdir, precollection):
    dirs = sorted(glob.glob("{0}/SWF*".format(indir)))
    tiles = [os.path.basename(d).split('_')[1] for d in dirs]
    
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    for ind, d in enumerate(dirs):
        
        tile = tiles[ind]
        fl = get_files(d, "pass01")

        if len(fl) > 10:
            print(d)
            dates = get_landsat_dates(fl, precollection=precollection)
            r = RasterTimeSeries(fl, dates)
            profile = r.profile.copy()
            nobs_profile = r.profile.copy()

            profile.update(count = 3, nodata = 255, compress = 'lzw')
            nobs_profile.update(count = 1, dtype = np.int16, nodata = -9999, compress = 'lzw')

            tiledir = "{0}/{1}".format(outdir, tile)
            if not os.path.exists(tiledir):
                os.makedirs(tiledir)   
            
            # quarter composites
            years = list(range(1984, 2018))
            quarters = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]
                
            for y in years:
                for i, q in enumerate(quarters):
                    try:
                        zco, zmn, zmd, zst = r.compute_stats(njobs = 15, months = q, years = y)
                        if zco.sum() == 0:
                            continue
                        zstats = np.stack([zmn, zmd, zst])                     
                        outfl = ["{0}/{1}_{2}Q{3}.tif".format(tiledir, j, y, i+1) for j in ['stats', 'nobs']]
                        with rasterio.open(outfl[0], 'w', **profile) as dst:
                            dst.write(zstats.astype(np.uint8))
                        with rasterio.open(outfl[1], 'w', **nobs_profile) as dst:
                            dst.write(zco.astype(np.int16).reshape((1, zco.shape[0], zco.shape[1])))
                        print("{0}Q{1} ".format(y, i+1), end = "")
                    except:
                        pass

if __name__ == '__main__':

    precollection = False

    try:
        indir = sys.argv[1]
        outdir = sys.argv[2]
    except IndexError:
        print("python {0} indir outdir [precollection=False]".format(os.path.basename(sys.argv[0])))
        sys.exit(1)

    try:
        precollection = sys.argv[3].upper() == "TRUE"
    except IndexError:
        precollection = False

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        main(indir, outdir, precollection)

