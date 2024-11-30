# qgis_plugin_gpq_downloader

An attempt at a qgis plugin to make it easier to stream data from large GeoParquet 1.1 datasets with BBOX.

This is a work in progress - but it seems to be working to download from places & roads Overture data into GeoParquet.

Next steps:

 * Fix .toml so it'll actually pip install (and check to make sure dependencies are right)
* Distribute as a QGIS plugin, figure out how dependencies work
 * settings panel to configure default download location, default download format, etc
 * Overture divisions - make sure it'll get one if it's fully contained in the area of a divison.
 * Azure and GCP support (need to have a way to set credentials for duckdb)
 * overture - be able to select multiple themes with check and then download each.
 * Panel for overture, source and add your own
 * Customize which release of overture to use
 * Better way to organize what data goes in each area. To start just define it all in one place, with what validation it could use. Eventually investigate things like building the area of interest into it for faster bounds checks, and auto-populating it based on source coop / overture releases.
 * Better default file names based on file names / themes
 * Make use of admin-partitioned files like vida by figuring out the admin boundaries to request to speed up the download. I think I did this in the open buildings code (though that might have just been user supplied).
 * And better selection of area to download
 * Add other output formats
 * More accurate progress bar? Seems like duckdb may be able to emit percentages, but they don't seem to be that accurate. Could do a two step process of downloading and then writing format.
 * Filter files by attributes (maybe just sql? Or cooler could be to introspect fields and make a dropdown list of attributes to filter by - but that's likely a lot of work)
 * Add a button to open the data in a new QGIS window
