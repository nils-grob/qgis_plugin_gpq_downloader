# GeoParquet Downloader for QGIS

This repo contains a QGIS plugin for downloading GeoParquet data from cloud sources, including Overture Maps, Source Cooperative, and the ability to enter the location of any online GeoParquet file or partition. Just the user's current viewport then gets downloaded, as GeoParquet, DuckDB or GeoPackage.

![gpq-download-demo5](https://github.com/user-attachments/assets/dc862317-1eb6-4ed4-b910-44ae22a60d71)


The core idea is that GeoParquet can act more like a 'server', letting users download only the data they need, if you add a bit more smarts to the client. So this plugin uses [DuckDB](https://duckdb.org/) but abstracts all the details of forming the right queries to external sources, so users can just pick the data they want and pull it down with ease. And with GeoPackage output users don't even need to know anything about GeoParquet.

## Installation

The goal will be to distribute this through the QGIS plugin repository, but for now you can install it manually by downloading a zip file from the [releases page](https://github.com/cholmes/qgis_plugin_gpq_downloader/releases) and using 'install from zip' in QGIS. (Hoping to soon get the plugin working with the new QGIS plugin manager so this won't be necessary in the future.)

## Usage

TODO:

## Contributing

This plugin has been made entirely with AI coding tools (primarily Cursor with claude-3.5-sonnet). Contributions are very welcome, both from more experienced python developers who can help clean up the code and add missing features, and from anyone who wants a place to do AI-assisted coding that (hopefully) actually gets widely used.

I'm interested in exploring open source collaboration in the age of AI coding tools, especially working with less experienced developers who'd like to contribute, so don't hesitate to jump in with AI-assisted pull requests!


 
