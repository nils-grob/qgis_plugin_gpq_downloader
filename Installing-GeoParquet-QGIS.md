# Installing GeoParquet Support in QGIS

GeoParquet is starting to be more widely supported in QGIS, but it's not yet supported on all platforms. This guide will help you install the necessary dependencies to view GeoParquet files in QGIS.

## Windows

Most distributions of QGIS for Windows have support for GeoParquet. If you're using the QGIS installer for Windows, you *should* be all set. If it's not working then try to get the latest version of QGIS from [here](https://qgis.org/en/site/for/windows.html). 

## Conda

The clearest way to install QGIS with Parquet support for Linux and OS/X is via the conda package manager.

From the terminal, run:

```
conda config --add channels conda-forge
conda install qgis libgdal-arrow-parquet
```

Then you can run `qgis` from the terminal.

## OS/X

Unfortunately the default QGIS install for OS/X does not have support for Parquet. If you want to have the native installers support it then make your voice heard on https://github.com/qgis/QGIS-Mac-Packager/issues/156

It seems like support has gotten held up with a bigger shift to [move the QGIS Mac builds to conda](https://github.com/qgis/QGIS-Enhancement-Proposals/issues/270).

## Linux

Conda is the easiest way to install QGIS with Parquet support on Linux, as detailed above. 

Additionally, there is a Flatpak QGIS package that includes support for Parquet:

```
flatpak install --user https://dl.flathub.org/build-repo/94031/org.qgis.qgis.flatpakref
```

If you are interested in GeoParquet support in other Linux distributions then advocate to your package maintainers to add the libgdal-arrow-parquet library to the distribution.