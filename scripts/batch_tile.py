#!/usr/bin/env python

import rasterio
from rasterstats import unionExtent, tileExtent, batchCropToExtent
import numpy as np
import os, sys, argparse
import pandas as pd


def parse_args(): ### TODO
    pass



def get_files(indir, string):
    fl = []
    for path, dirs, files in os.walk(indir):
        for f in files:
            if string in f and f.endswith('tif'):
                    fl.append(os.path.join(path, f))
    return fl


if __name__ == '__main__':
    
    try:
        indir = sys.argv[1]
        string = sys.argv[2]
        outdir = sys.argv[3]
    except IndexError:
        print("python {0} indir string outdir [njobs (10)]".format(os.path.basename(sys.argv[0])))
        sys.exit(1)
        
    try:
        njobs = int(sys.argv[4])
    except IndexError:
        njobs = 10
    
    fl = get_files(indir, string)
    
    # determine union extent and tiling system
    e = unionExtent(fl, njobs = 16)
    tiles = tileExtent(e, 60000, 60000)
    
    # batch crop to extent
    for i in range(len(tiles)):
        print("Cropping to ({0}, {1}, {2}, {3})".format(tiles.loc[i, 'xmin'], tiles.loc[i, 'ymin'], tiles.loc[i, 'xmax'], tiles.loc[i, 'ymax']))
        tiledir = "{0}/SWF_{1}".format(outdir, tiles.loc[i, 'tile'])
        if not os.path.exists(tiledir):
            os.makedirs(tiledir)
        outfl = batchCropToExtent(fl, tiles.loc[i, 'extent'], outdir = tiledir, njobs = njobs, check_if_empty = True)
        
