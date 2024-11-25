from qgis.PyQt.QtWidgets import QAction, QFileDialog, QMessageBox, QInputDialog
from qgis.core import QgsProject, QgsRectangle
import duckdb
import requests
import os
import threading
from PyQt5.QtCore import pyqtSignal, QObject

class Worker(QObject):
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, dataset_url, extent, output_file):
        super().__init__()
        self.dataset_url = dataset_url
        self.extent = extent
        self.output_file = output_file

    def run(self):
        bbox = f"{self.extent.xMinimum()},{self.extent.yMinimum()},{self.extent.xMaximum()},{self.extent.yMaximum()}"

        conn = duckdb.connect()
        try:
            # Install and load the spatial extension
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")

            # Base query
            base_query = f"""
            COPY (
                SELECT * FROM read_parquet('{self.dataset_url}')
                WHERE bbox.xmin BETWEEN {self.extent.xMinimum()} AND {self.extent.xMaximum()}
                AND bbox.ymin BETWEEN {self.extent.yMinimum()} AND {self.extent.yMaximum()}
            ) TO '{self.output_file}' 
            """

            # Format-specific options
            if self.output_file.endswith(".parquet"):
                format_options = "(FORMAT 'parquet', COMPRESSION 'BROTLI');"
            elif self.output_file.endswith(".gpkg"):
                format_options = "(FORMAT 'GPKG');"
            else:
                self.error.emit("Unsupported file format.")
                return

            # Complete query
            copy_query = base_query + format_options

            # Print the SQL query
            print("Executing SQL query:")
            print(copy_query)

            conn.execute(copy_query)
            self.finished.emit(f"Data saved to {self.output_file}.")
        except Exception as e:
            self.error.emit(str(e))
        finally:
            conn.close()

class QgisPluginGeoParquet:
    def __init__(self, iface):
        self.iface = iface
        self.action = None

    def initGui(self):
        self.action = QAction("Download GeoParquet Data", self.iface.mainWindow())
        self.action.triggered.connect(self.run)
        self.iface.addPluginToMenu("GeoParquet Plugin", self.action)

    def unload(self):
        self.iface.removePluginMenu("GeoParquet Plugin", self.action)
        self.iface.removeToolBarIcon(self.action)

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

        output_file, _ = QFileDialog.getSaveFileName(
            self.iface.mainWindow(), "Save As", "", "GeoParquet (*.parquet);;GeoPackage (*.gpkg)"
        )

        if not output_file:
            return

        self.download_and_save(dataset_url, extent, output_file)

    def download_and_save(self, dataset_url, extent: QgsRectangle, output_file: str):
        worker = Worker(dataset_url, extent, output_file)
        worker_thread = threading.Thread(target=worker.run)

        # Connect signals to slots
        worker.finished.connect(lambda message: QMessageBox.information(self.iface.mainWindow(), "Success", message))
        worker.error.connect(lambda message: QMessageBox.critical(self.iface.mainWindow(), "Error", message))

        # Start the worker thread
        worker_thread.start()

def classFactory(iface):
    return QgisPluginGeoParquet(iface)
