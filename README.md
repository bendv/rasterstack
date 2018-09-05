rasterstack
===========

Tools for computing statistics from raster time series stacks.

## Pre-processing raster stacks

### Tiling raster time series

This example assumes you are in a directory with a series of raster tif files that you want to stack and compute temporal stats on.

Compute the union extent of all images:

```python
%matplotlib
import matplotlib.pyplot as plt
from rasterstack import unionExtent, tileExtent, batchCropToExtent, RasterTimeSeries
import glob, os
from datetime import datetime

fl = sorted(glob.glob("*.tif"))
e = unionExtent(fl, njobs = 6)
```

Determine tile extents for 2000x2000 pixel tiles (= 60000m x 60000m tiles)

```python
tiles = tileExtent(e, 60000, 60000)
```

Crop files to a given tile (03-03 in this example) and compute stats for that tile:

```python
outdir = "tile_03-03"
e = tiles.query("tile == \"03-03\"")['extent']
os.makedirs(outdir)
cropfl = batchCropToExtent(fl, e, outdir = outdir, suffix = '03-03', res = 30, njobs = 8, verbose = 0)
dates = [ datetime.strptime(os.path.basename(f).split('_')[3], "%Y%m%d") for f in cropfl ]
ts = RasterTimeSeries(cropfl, dates)
nobs, xmean, xmedian, xstd = ts.compute_stats(njobs = 10)

fig, ax = plt.subplots(1, 2, figsize = [15, 10])
ax[0].imshow(xmedian, vmin = 0, vmax = 100, cmap = plt.cm.YlGnBu)
ax[1].imshow(nobs, vmin = 0)
```
