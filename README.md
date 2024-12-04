# GeoParquet Downloader for QGIS

This repo contains a QGIS plugin for downloading GeoParquet data from cloud sources, including Overture Maps, Source Cooperative, and the ability to enter the location of any online GeoParquet file or partition. Just the user's current viewport then gets downloaded, as GeoParquet, DuckDB or GeoPackage.

![gpq-download-demo5](https://github.com/user-attachments/assets/dc862317-1eb6-4ed4-b910-44ae22a60d71)


The core idea is that GeoParquet can act more like a 'server', letting users download only the data they need, if you add a bit more smarts to the client. So this plugin uses [DuckDB](https://duckdb.org/) but abstracts all the details of forming the right queries to external sources, so users can just pick the data they want and pull it down with ease. And with GeoPackage output users don't even need to know anything about GeoParquet.

## Installation

The easiest way to install the plugin file is to use the QGIS plugin manager. Just go to `Plugins > Manage and Install Plugins` and go to 
the 'install' tab and search for 'GeoParquet Downloader'. Click on 'Install Plugin' and you should get it. Right now you also need to install DuckDB
to get things to work. 

On Windows you can use the [QDuckDB plugin](https://oslandia.gitlab.io/qgis/qduckdb/) which includes a precompiled binary. They also document how to install it on [Linux](https://oslandia.gitlab.io/qgis/qduckdb/usage/installation.html#linux) and [Mac OS/X](https://oslandia.gitlab.io/qgis/qduckdb/usage/installation.html#macos).

See [metadata.txt](./metadata.txt) for more OS-specific installation notes.

## Usage

The plugin will install 3 buttons on the QGIS toolbar:



All three open the same dialog box, but the Overture and Source Cooperative
buttons take you straight to the relevant sections. The default button
lets you enter the URL of any GeoParquet file or partition.



To use it move to an area where you'd like to download data and then select which layer you'd like to download. From there you can choose the output format (GeoParquet, GeoPackage or DuckDB) and the location to download the data to.

Downloads can sometimes take awhile, especially if the data provider hasn't optimized their GeoParquet files very well, or if you're downloading an area with a lot of data. Overture generally works the fastest for now, others may take a minute or two. But it should most always be faster than trying to figure out exactly which files you need and downloading them manually.

For now we only support downloading into the current viewport, but hope to [improve that](https://github.com/cholmes/qgis_plugin_gpq_downloader/issues/10). 

If your QGIS doesn't have GeoParquet support you'll get a warning dialog after the data downloads completes. The GeoParquet will be there, but it won't automatically open on the map. We definitely recommend getting your QGIS working with GeoParquet, as the format is faster and handles nested attributes better. See [Installing GeoParquet Support in QGIS](https://github.com/cholmes/qgis_plugin_gpq_downloader/wiki/Installing-GeoParquet-Support-in-QGIS) for more details.


## Contributing

This plugin has been made entirely with AI coding tools (primarily Cursor with claude-3.5-sonnet). Contributions are very welcome, both from more experienced python developers who can help clean up the code and add missing features, and from anyone who wants a place to do AI-assisted coding that (hopefully) actually gets widely used.

I'm interested in exploring open source collaboration in the age of AI coding tools, especially working with less experienced developers who'd like to contribute, so don't hesitate to jump in with AI-assisted pull requests.

And any help on ideas/feedback, documentation, testing, promoting, etc. is very welcome!


 
