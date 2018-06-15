rasterstats
===========

Tools for computing statistics from raster time series stacks.

## Pre-processing raster stacks

1. Tiling raster time series

Compute the union extent of all images:

```python
import rasterstats
import glob, os

fl = sorted(glob.glob("*.tif"))
e = unionExtent(fl, njobs = 6)
```

Determine tile extents for 2000x2000 pixel tiles (= 60000mx60000m tiles)

```python
tiles = tileExtent(e, 60000, 60000)
```

Crop all files to all tiles using parallel processing.

```python
for i in range(len(tiles)):
    outdir = "tile_{0}".format(tiles.loc[i, 'tile'])
    e = tiles.loc[i, 'extent']
    os.makedirs(outdir)
    print(outdir, "...")
    _ = batchCropToExtent(fl, e, outdir = outdir, suffix = 'crop', res = 30, njobs = 8, verbose = 0)
```

