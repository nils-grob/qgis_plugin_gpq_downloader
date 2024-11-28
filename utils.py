from qgis.core import QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject

def transform_bbox_to_4326(extent, source_crs):
    """
    Transform a bounding box to EPSG:4326 (WGS84)
    
    Args:
        extent (QgsRectangle): The input extent to transform
        source_crs (QgsCoordinateReferenceSystem): The source CRS of the extent
        
    Returns:
        QgsRectangle: The transformed extent in EPSG:4326
    """
    dest_crs = QgsCoordinateReferenceSystem("EPSG:4326")

    if source_crs != dest_crs:
        transform = QgsCoordinateTransform(source_crs, dest_crs, QgsProject.instance())
        return transform.transformBoundingBox(extent)
    return extent