rasterstack
===========

A set of miscellaneous tools for working with raster stacks.

## Installation

```bash
git clone https://github.com/bendv/rasterstack
cd rasterstack
pip install .
```

Check the installed version in python:

```python
from rasterstack import __version__
print(__version__)
```

## Creating a RasterTimeSeries instance

Suppose we have a list of annual raster composites:

```python
fl = [
    'composite_2000.tif',
    'composite_2001.tif',
    'composite_2002.tif',
    'composite_2003.tif',
    ...
    'composite_2020.tif'
]
```

We will also need a list of ```datetime.datetime```'s that correspond with each of these rasters:

```python
from datetime import datetime
def getDate(f):
    date = datetime.strptime(f.split("_")[1].replace(".tif", ""), "%Y")
    return date

dates = [getDate(f) for f in fl]
```

Now we can combine the filenames and corresponding dates into a ```RasterTimeSeries``` instance:
```python
from rasterstack import RasterTimeSeries
rts = RasterTimeSeries(fl, dates)
print(rts.data)
```

Compute some basic cell-wise statistics from this object:

```python
nobs, xmean, xmedian, xstd = rts.compute_stats(njobs = 10)
```

## Tiling large rasters

Suppose we have a list of Landsat-8 SWIR1 images from a single path/row:

```python
fl = [
    'LC08_L1TP_037028_20170528_20170615_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20170613_20170628_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20170629_20170714_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20170715_20170727_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20170731_20170811_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20170816_20170825_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20170901_20170916_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20170917_20170929_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20171003_20171014_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20171019_20171025_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20171120_20171206_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20171206_20171223_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180123_20180206_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180312_20180320_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180328_20180405_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180413_20180417_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180429_20180502_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180515_20180604_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180531_20180614_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180702_20180717_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180718_20180731_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180803_20180814_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180819_20180829_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180904_20180912_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20180920_20180928_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20181006_20181010_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20181022_20181031_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20181123_20181210_01_T1_sr_band6.tif',
    'LC08_L1TP_037028_20181209_20181226_01_T1_sr_band6.tif'
]
```

Print the extents of one of the images:

```python
from rasterstack import imageExtent

print(imageExtent(fl[0]))
```

```
> (582285.0, 4986585.0, 816015.0, 5216715.0)
```

The extents of all files are equal:

```python
from rasterstack import equalExtents

print(equalExtents(fl))
```

```
> False
```

Compute the union extent of all images:

```python
from rasterstack import unionExtent

e = unionExtent(fl)
print(e)
```

```
> (578685.0, 4986285.0, 816915.0, 5216715.0)
```

A grid of tiles can be made from this extent using the ```tileExtent``` function. For example, to make a grid of tiles of 60000m X 60000m (= 2000 X 2000 Landsat pixels):

```python
from rasterstack import tileExtent

tiles = tileExtent(e, 60000, 60000)
```

Note that ```dx``` and ```dy``` are assumed to be in the same units as ```e``` (metres in this case).

```python
print(tiles)
```

```
     tile      xmin       ymin      xmax       ymax                                      extent
0   01-01  578685.0  4986285.0  638685.0  5046285.0  [578685.0, 4986285.0, 638685.0, 5046285.0]
1   01-02  638685.0  4986285.0  698685.0  5046285.0  [638685.0, 4986285.0, 698685.0, 5046285.0]
2   01-03  698685.0  4986285.0  758685.0  5046285.0  [698685.0, 4986285.0, 758685.0, 5046285.0]
3   01-04  758685.0  4986285.0  816915.0  5046285.0  [758685.0, 4986285.0, 816915.0, 5046285.0]
4   02-01  578685.0  5046285.0  638685.0  5106285.0  [578685.0, 5046285.0, 638685.0, 5106285.0]
5   02-02  638685.0  5046285.0  698685.0  5106285.0  [638685.0, 5046285.0, 698685.0, 5106285.0]
6   02-03  698685.0  5046285.0  758685.0  5106285.0  [698685.0, 5046285.0, 758685.0, 5106285.0]
7   02-04  758685.0  5046285.0  816915.0  5106285.0  [758685.0, 5046285.0, 816915.0, 5106285.0]
8   03-01  578685.0  5106285.0  638685.0  5166285.0  [578685.0, 5106285.0, 638685.0, 5166285.0]
9   03-02  638685.0  5106285.0  698685.0  5166285.0  [638685.0, 5106285.0, 698685.0, 5166285.0]
10  03-03  698685.0  5106285.0  758685.0  5166285.0  [698685.0, 5106285.0, 758685.0, 5166285.0]
11  03-04  758685.0  5106285.0  816915.0  5166285.0  [758685.0, 5106285.0, 816915.0, 5166285.0]
12  04-01  578685.0  5166285.0  638685.0  5216715.0  [578685.0, 5166285.0, 638685.0, 5216715.0]
13  04-02  638685.0  5166285.0  698685.0  5216715.0  [638685.0, 5166285.0, 698685.0, 5216715.0]
14  04-03  698685.0  5166285.0  758685.0  5216715.0  [698685.0, 5166285.0, 758685.0, 5216715.0]
15  04-04  758685.0  5166285.0  816915.0  5216715.0  [758685.0, 5166285.0, 816915.0, 5216715.0]
```

This has been designed to work with the `gdalwarp` command-line utility. For example, to crop the first raster in our list of files to tile "03-03":

```python
import subprocess
import rasterio

with rasterio.open(fl[0]) as src:
    crs = src.profile['crs']['init']
    res = src.profile['transform'][0]

xmin = tiles.loc[10,'xmin']
ymin = tiles.loc[10,'ymin']
xmax = tiles.loc[10,'xmax']
ymax = tiles.loc[10,'ymax']

command = [
    'gdalwarp',
    '-te', str(xmin), str(ymin), str(xmax), str(ymax),
    '-te_srs', crs,
    '-t_srs', crs,
    '-tr', str(res), str(res),
    '-tap',
    '-r', 'BILINEAR',
    fl[0], fl[0].replace(".tif", "_03-03.tif")
]

print(' '.join(command))
subprocess.call(command)
```

## Theil-Sen / Mann-Kendall trend tests

The `theilsen` submodule contains a tool to carry out a pixelwise Theil-Sen/Mann-Kendall test on a stack of rasters, given an independent variable array (usually time).

The function expects a 3-D array, as it is designed for rasters stack. Therefore, to run it on a dependent variable array, reshape the array into a 3-D array:

```python
import numpy as np
from rasterstack.theilsen import theilsen

rng = np.random.default_rng(seed = 12345)
t = np.arange(10)
X = 0.2*t + rng.random()*1000

X = X[:,np.newaxis,np.newaxis]
ts, mk, Z = theilsen(X, t)
print(ts, mk, Z)
```

To use the Z-statistic in a 2-tailed significance:

```python
from scipy.stats import norm
pval = 2 * norm.cdf(-np.abs(Z))
print(pval)
```

Suppose we have a list of rasters representing annual composites:

```python
fl = [
    'annual_composite_2000.tif',
    'annual_composite_2001.tif',
    'annual_composite_2002.tif',
    'annual_composite_2003.tif',
    'annual_composite_2004.tif',
    'annual_composite_2005.tif',
    'annual_composite_2006.tif',
    'annual_composite_2007.tif',
    'annual_composite_2008.tif',
    'annual_composite_2009.tif',
    'annual_composite_2010.tif',
]
```
 
 Load them sequentially, extract the corresponding years, add them to a `numpy` stack, and run the Theil-Sen/Mann-Kendall function:

 ```python
import rasterio
import numpy as np
import re
from rasterstack.theilsen import theilsen
from scipy.stats import norm

years = np.array( [int(re.findall("[0-9]+", f)[0]) for f in fl] )

stack = []
for f in fl:
    with rasterio.open(f) as src:
        stack.append(src.read(1))
stack = np.stack(stack)

ts, mk, z = theilsen(stack, years)
pval = 2 * norm.cdf(-np.abs(z))
```
