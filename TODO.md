TODO
====

- add a spatial subset property to RasterTimeSeries
    - this will indicate that all stats, etc., are only computed on that spatial subset
    
- add a custom opening method to RasterTimeSeries
    - this will (e.g.) follow instructions related to pre-defined spatial subset (see above)
    
- add a custom temporal subsetting method to RasterTimeSeries (update metadata accordingly each time)

- add a method to apply a custom pixel-wise function to a RasterTimeSeries (including a compositing feature)

- revise Bayesian model to include an arbitrary number of input bands:
    - likelihood rasters are computed separately for each band
    - the per-pixel likelihood is then the product of each individual band-specific likelihood
    - this should not affect performance very much, as the likelihood cython function is executed in sequence on 2-D arrays
    - test this using HLS bands + indices and compare to SWF
    