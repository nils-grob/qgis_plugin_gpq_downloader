# qgis_plugin_gpq_downloader

An attempt at a qgis plugin to make it easier to stream data from large GeoParquet 1.1 datasets with BBOX.

This is a work in progress - but it seems to be working to download from places & roads Overture data into GeoParquet.

Next steps:

 * Fix .toml so it'll actually pip install (and check to make sure dependencies are right)
 * Automatically open the file after downloading it.
 * Better selection of download file location (ideally more in line with how qgis works)
 * Distribute as a QGIS plugin, figure out how dependencies work
 * Check if geopackage is working, add other formats
 * Add a progress bar
 * Add a cancel button
 * Filter files by attributes (maybe just sql? Or cooler could be to introspect fields and make a dropdown list of attributes to filter by - but that's likely a lot of work)
 * Add a button to open the data in a new QGIS window
