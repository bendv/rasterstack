TODO
====

- add a spatial subset property to RasterTimeSeries
    - this will indicate that all stats, etc., are only computed on that spatial subset
    - use windowed reading https://rasterio.readthedocs.io/en/latest/topics/windowed-rw.html
        - determine row/col offsets by first reading image metadata, and transforming user-input extent coordinates
        - if user-defined extent is not within geotransform bounds, then do not read the file (with a warning)
    
- add a custom opening method to RasterTimeSeries
    - this will (e.g.) follow instructions related to pre-defined spatial subset (see above)
    - this should also allow for reading of rasters with unequal extents (adjust reading window to each raster according to given extent; if no extent is given, then the equalExtents check is run)
    
- add a custom temporal subsetting method to RasterTimeSeries (update metadata accordingly each time)

- add a method to apply a custom pixel-wise function to a RasterTimeSeries (including a compositing feature)
    - this function should accept a 1-D numpy array and return a single value
    - the function object must be in namespace, and can be appended to "stats" list (including 'mean', 'nobs', 'median', and 'sd' as pre-defined keywords)

- allow specification of type of RasterTimeSeries (continuous vs. categorical)
