#!/usr/bin/env python

from rasterstats import RasterTimeSeries
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



def main(indir, precollection):
    dirs = sorted(glob.glob("{0}/SWF*".format(indir)))
   
    for d in dirs:
        fl = get_files(d, "crop")
       
        if len(fl) > 10:
            print(d)
            dates = get_landsat_dates(fl, precollection=precollection)
            r = RasterTimeSeries(fl, dates)
            profile = r.profile.copy()
            nobs_profile = r.profile.copy()
            
            profile.update(compress = 'lzw')
            nobs_profile.update(dtype = np.int16, compress = 'lzw')

            # overall stats
            print("Computing overall stats...", end = "")
            outdir = "{0}/overall_stats".format(d)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            try:
                zco, zmn, zmd, zst = r.compute_stats(njobs = 14)
                outfl = ["{0}/{1}_overall.tif".format(outdir, j) for j in ['mean', 'median', 'std', 'nobs']]
                with rasterio.open(outfl[0], 'w', **profile) as dst:
                    dst.write(zmn.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                with rasterio.open(outfl[1], 'w', **profile) as dst:
                    dst.write(zmd.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                with rasterio.open(outfl[2], 'w', **profile) as dst:
                    dst.write(zst.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                with rasterio.open(outfl[3], 'w', **nobs_profile) as dst:
                    dst.write(zco.astype(np.int16).reshape((1, zmn.shape[0], zmn.shape[1])))
                print("done.")
            except:
                print("encountered error, skipping...")
                pass

            # annual stats
            years = list(range(1984, 2018))
            print("Computing annual stats...", end = "")
            outdir = "{0}/annual_stats".format(d)
            if not os.path.exists(outdir):
                os.makedirs(outdir)            
            for y in years:
                print(y, " ", end = "")
                try:
                    zco, zmn, zmd, zst = r.compute_stats(njobs = 14, years = y)
                    outfl = ["{0}/{1}_{2}.tif".format(outdir, j, y) for j in ['mean', 'median', 'std', 'nobs']]
                    with rasterio.open(outfl[0], 'w', **profile) as dst:
                        dst.write(zmn.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                    with rasterio.open(outfl[1], 'w', **profile) as dst:
                        dst.write(zmd.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                    with rasterio.open(outfl[2], 'w', **profile) as dst:
                        dst.write(zst.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                    with rasterio.open(outfl[3], 'w', **nobs_profile) as dst:
                        dst.write(zco.astype(np.int16).reshape((1, zmn.shape[0], zmn.shape[1])))
                except:
                    pass
            print("done.")
            
            # seasonal stats
            seasons = [[12, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]
            seasons_str = ['winter', 'spring', 'summer', 'autumn']
            print("Computing annual stats...", end = "")
            outdir = "{0}/seasonal_stats".format(d)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            for i, s in enumerate(seasons):
                print(seasons_str[i], " ", end = "")
                try:
                    zco, zmn, zmd, zst = r.compute_stats(njobs = 14, months = s)
                    outfl = ["{0}/{1}_{2}.tif".format(outdir, j, seasons_str[i]) for j in ['mean', 'median', 'std', 'nobs']]
                    with rasterio.open(outfl[0], 'w', **profile) as dst:
                        dst.write(zmn.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                    with rasterio.open(outfl[1], 'w', **profile) as dst:
                        dst.write(zmd.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                    with rasterio.open(outfl[2], 'w', **profile) as dst:
                        dst.write(zst.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                    with rasterio.open(outfl[3], 'w', **nobs_profile) as dst:
                        dst.write(zco.astype(np.int16).reshape((1, zmn.shape[0], zmn.shape[1])))
                except:
                    pass
            print("done.")
            
            # 10-day composites
            print("Computing 10-day composites")
            outdir = "{0}/composite_10day".format(d)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            years = list(range(1984, 2018))
            doys = list(range(1, 366, 10))
            for y in years:
                for d in doys:
                    print("{0}-{1}".format(y, d), " ", end = "")
                    try:
                        if d < 357:
                            DOY = list(range(d, d+10))
                        else:
                            DOY = list(range(d, 367))
                        zco, zmn, zmd, zst = r.compute_stats(njobs = 14, years = y, doys = DOY)
                        outfl = ["{0}/{1}_{2}{3:03d}.tif".format(outdir, j, y, d) for j in ['mean', 'median', 'std', 'nobs']]
                        with rasterio.open(outfl[0], 'w', **profile) as dst:
                            dst.write(zmn.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                        with rasterio.open(outfl[1], 'w', **profile) as dst:
                            dst.write(zmd.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                        with rasterio.open(outfl[2], 'w', **profile) as dst:
                            dst.write(zst.astype(np.uint8).reshape((1, zmn.shape[0], zmn.shape[1])))
                        with rasterio.open(outfl[3], 'w', **nobs_profile) as dst:
                            dst.write(zco.astype(np.int16).reshape((1, zmn.shape[0], zmn.shape[1])))
            

if __name__ == '__main__':
    
    precollection = False
    
    try:
        indir = sys.argv[1]
    except IndexError:
        print("python {0} indir [precollection=False]".format(os.path.basename(sys.argv[0])))
        sys.exit(1)

    try:
        precollection = sys.argv[2].upper() == "TRUE"
    except IndexError:
        precollection = False
    
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        main(indir, precollection)
