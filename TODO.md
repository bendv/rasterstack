TODO
====

- allow creation of RasterTimeSeries without date info to allow for basic stats
    - class attribute indicates whether time stamp is present or not...
    - or create new RasterStack class
    - 

- add a spatial subset property to RasterTimeSeries
    - this will indicate that all stats, etc., are only computed on that spatial subset
    
- add a custom opening method to RasterTimeSeries
    - this will (e.g.) follow instructions related to pre-defined spatial subset (see above)
    - this should also allow for reading of rasters with unequal extents (adjust reading window to each raster according to given extent; if no extent is given, then the equalExtents check is run)
    
- add a custom temporal subsetting method to RasterTimeSeries (update metadata accordingly each time)

- add a method to apply a custom pixel-wise function to a RasterTimeSeries (including a compositing feature)

- allow specification of type of RasterTimeSeries (continuous vs. categorical)

- write RasterTimeSeries to a BIP multiband raster?
