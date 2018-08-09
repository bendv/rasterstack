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

            # quarter composites
            years = list(range(1984, 2018))
            quarters = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]
            print("Computing quarter composites...", end = "")
            outdir = "{0}/composite_quarter".format(d)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            for y in years:
                for i, q in enumerate(quarters):
                    print("{0}Q{1} ".format(y, i+1), end = "")
                    try:
                        zco, zmn, zmd, zst = r.compute_stats(njobs = 14, months = q, years = y)
                        outfl = ["{0}/{1}_{2}Q{3}.tif".format(outdir, j, y, i+1) for j in ['mean', 'median', 'std', 'nobs']]
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

