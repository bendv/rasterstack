rasterstack
===========

Tools for computing statistics from raster time series stacks.

## Installation

...TODO: push to github and u/l to PyPI...

## Tiling raster time series

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

To crop files to a given tile ("03-03" in this example) and compute stats for that tile, use ```batchCropToExtent```, which returns a list of output filenames:

```python
from rasterstack import batchCropToExtent

outdir = "tile_03-03"
e = list(tiles.query("tile == \"03-03\"")['extent'])[0]
os.makedirs(outdir)
cropfl = batchCropToExtent(fl, e, outdir = outdir, suffix = '03-03', res = 30, njobs = 8, verbose = 0)
## TODO: debug this step ("Missing src_crs") ##
```

```RasterTimeSeries``` is a class for parsing multi-temporal data. To create a ```RasterTimeSeries``` object, you first need to parse the dates from the filenames (or get them some other way):

```python
from rasterstack import RasterTimeSeries

dates = [ datetime.strptime(f.split('_')[3], "%Y%m%d") for f in fl ]
ts = RasterTimeSeries(cropfl, dates)
print(ts.data)
```


```python
nobs, xmean, xmedian, xstd = ts.compute_stats(njobs = 10)
```


## Theil-Sen Regression sub-module

Load this as a separate module for now. For a 3-D numpy array `z`

```python
from rasterstack.theilsen import theilsen
ts = theilsen(z)
```



