from qgis.PyQt.QtWidgets import QAction, QFileDialog, QMessageBox, QInputDialog
from qgis.PyQt.QtGui import QIcon  # Import QIcon to set an icon for the action
from qgis.core import QgsProject, QgsRectangle, QgsVectorLayer, QgsCoordinateReferenceSystem, QgsCoordinateTransform
from PyQt5.QtCore import pyqtSignal, QObject
import duckdb
import os
import threading
import resources_rc
from pathlib import Path

class Worker(QObject):
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, dataset_url, extent, output_file, iface):
        super().__init__()
        self.dataset_url = dataset_url
        self.extent = extent
        self.output_file = output_file
        self.iface = iface

    def run(self):
        # Create source and destination CRS objects
        source_crs = self.iface.mapCanvas().mapSettings().destinationCrs()
        dest_crs = QgsCoordinateReferenceSystem("EPSG:4326")

        # Create transform if needed
        if source_crs != dest_crs:
            transform = QgsCoordinateTransform(source_crs, dest_crs, QgsProject.instance())
            bbox = transform.transformBoundingBox(self.extent)
        else:
            bbox = self.extent

        conn = duckdb.connect()
        try:
            # Install and load the spatial extension
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")

            select_query = "SELECT *"
            if not self.output_file.endswith(".parquet"):

                # Get the schema of the dataset to identify column types
                schema_query = f"DESCRIBE SELECT * FROM read_parquet('{self.dataset_url}')"
                schema_result = conn.execute(schema_query).fetchall()

                # Construct the SELECT clause with array conversion to strings
                columns = []
                for row in schema_result:
                    col_name = row[0]
                    col_type = row[1]
                    
                    
                    if 'STRUCT' in col_type.upper() or 'MAP' in col_type.upper():
                        columns.append(f"TO_JSON({col_name}) AS {col_name}")
                    elif '[]' in col_type:  # Check for array types like VARCHAR[]
                        columns.append(f"array_to_string({col_name}, ', ') AS {col_name}")
                    else:
                        columns.append(col_name)

                   # When we support more than overture just select the primary name when it's o

                    select_query = f"SELECT names.primary as name,{', '.join(columns)}"
            # Base query
            base_query = f"""
            COPY (
                {select_query} FROM read_parquet('{self.dataset_url}')
                WHERE bbox.xmin BETWEEN {bbox.xMinimum()} AND {bbox.xMaximum()}
                AND bbox.ymin BETWEEN {bbox.yMinimum()} AND {bbox.yMaximum()}
            ) TO '{self.output_file}' 
            """

            # Format-specific options
            if self.output_file.endswith(".parquet"):
                format_options = "(FORMAT 'parquet', COMPRESSION 'zstd');"
            elif self.output_file.endswith(".gpkg"):
                format_options = "(FORMAT GDAL, DRIVER 'GPKG');"
            else:
                self.error.emit("Unsupported file format.")
                return

            # Complete query
            copy_query = base_query + format_options

            # Print the SQL query
            print("Executing SQL query:")
            print(copy_query)

            conn.execute(copy_query)
            # Emit the output file path instead of a message
            self.finished.emit(self.output_file)
        except Exception as e:
            self.error.emit(str(e))
        finally:
            conn.close()

class QgisPluginGeoParquet:
    def __init__(self, iface):
        self.iface = iface
        self.action = None
        # Create a default downloads directory in user's home directory
        self.download_dir = Path.home() / "Downloads" 
        # Create the directory if it doesn't exist
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def initGui(self):
        # Create the action with the new icon and tooltip
        self.action = QAction(QIcon(':/qgis_plugin_gpq_downloader/icons/download.svg'), "Download GeoParquet Data", self.iface.mainWindow())
        self.action.setToolTip("Download GeoParquet Data")
        self.action.triggered.connect(self.run)
        # Add the action to the toolbar
        self.iface.addToolBarIcon(self.action)
        # Optionally, add the action to a custom toolbar
        # self.toolbar = self.iface.addToolBar("GeoParquet")
        # self.toolbar.addAction(self.action)
        # Remove the menu-related code
        # self.iface.addPluginToMenu("GeoParquet Plugin", self.action)

    def unload(self):
        # Remove the action from the toolbar
        self.iface.removeToolBarIcon(self.action)
        # Remove the custom toolbar if used
        # del self.toolbar
        # Remove the menu-related code
        # self.iface.removePluginMenu("GeoParquet Plugin", self.action)

    def run(self):
        options = ["Overture Places", "Overture Roads"]
        url_map = {
            "Overture Places": "s3://overturemaps-us-west-2/release/2024-11-13.0/theme=places/type=place/*",
            "Overture Roads": "s3://overturemaps-us-west-2/release/2024-11-13.0/theme=transportation/type=segment/*",
        }

        selected_option, ok = QInputDialog.getItem(
            self.iface.mainWindow(), "Select Dataset", "Choose a dataset:", options, 0, False
        )

        if not ok or not selected_option:
            return

        dataset_url = url_map[selected_option]
        extent = self.iface.mapCanvas().extent()

        if not extent:
            QMessageBox.critical(self.iface.mainWindow(), "Error", "No extent found.")
            return

        # Get the current canvas extent
        extent = self.iface.mapCanvas().extent()
        
        # Generate a default filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"geoparquet_download_{timestamp}.parquet"
        default_save_path = str(self.download_dir / default_filename)

        # Show file dialog with GeoParquet as the default format
        output_file, _ = QFileDialog.getSaveFileName(
            self.iface.mainWindow(),
            "Save GeoParquet Data",
            default_save_path,
            "GeoParquet (*.parquet);;GeoPackage (*.gpkg)"  # Switched order to make GeoParquet the default
        )

        if not output_file:
            return

        self.download_and_save(dataset_url, extent, output_file)

    def download_and_save(self, dataset_url, extent: QgsRectangle, output_file: str):
        worker = Worker(dataset_url, extent, output_file, self.iface)
        worker_thread = threading.Thread(target=worker.run)

        # Connect signals to slots
        worker.finished.connect(self.on_finished)
        worker.error.connect(lambda message: QMessageBox.critical(self.iface.mainWindow(), "Error", message))

        # Start the worker thread
        worker_thread.start()

    def on_finished(self, output_file):
        # Load the output_file into QGIS
        if output_file.endswith('.parquet') or output_file.endswith('.gpkg'):
            # Create a new vector layer
            layer = QgsVectorLayer(output_file, os.path.basename(output_file), "ogr")
            if not layer.isValid():
                QMessageBox.critical(self.iface.mainWindow(), "Error", "Failed to load the layer.")
                return
            # Add the layer to the QGIS project
            QgsProject.instance().addMapLayer(layer)
        else:
            QMessageBox.critical(self.iface.mainWindow(), "Error", "Unsupported file format.")

def classFactory(iface):
    return QgisPluginGeoParquet(iface)
