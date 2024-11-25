from .qgis_plugin_gpq_downloader import QgisPluginGeoParquet

def classFactory(iface):
    return QgisPluginGeoParquet(iface)