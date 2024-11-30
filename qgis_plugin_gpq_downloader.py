from qgis.PyQt.QtWidgets import (
    QAction, QFileDialog, QMessageBox, QDialog, 
    QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
    QPushButton, QComboBox, QProgressDialog, 
    QRadioButton, QStackedWidget, QWidget
)
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtCore import pyqtSignal, QObject, Qt, QThread
from qgis.core import (
    QgsProject, QgsRectangle, QgsVectorLayer, 
    QgsCoordinateReferenceSystem, QgsCoordinateTransform
)
import duckdb
import os
from pathlib import Path
from .utils import transform_bbox_to_4326
from . import resources_rc

class Worker(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    load_layer = pyqtSignal(str)
    info = pyqtSignal(str)
    progress = pyqtSignal(str)
    percent = pyqtSignal(int)

    def __init__(self, dataset_url, extent, output_file, iface):
        super().__init__()
        self.dataset_url = dataset_url
        self.extent = extent
        self.output_file = output_file
        self.iface = iface
        self.killed = False

    def run(self):
        self.progress.emit("Connecting to database...")
        source_crs = self.iface.mapCanvas().mapSettings().destinationCrs()
        bbox = transform_bbox_to_4326(self.extent, source_crs)

        conn = duckdb.connect()
        try:
            # Install and load the spatial extension
            self.progress.emit("Loading spatial extension...")

            if self.output_file.lower().endswith('.duckdb'):
                conn = duckdb.connect(self.output_file)  # Connect directly to output file
            else:
                conn = duckdb.connect() 

            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")

            table_name = "download_data" # TODO: Better name, in line with user selected name

            self.progress.emit("Preparing query...")
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
            # Construct WHERE clause based on presence of bbox (this code is not called now as validation ensures the bbox is there
            # but leaving it here for now as we may want to support non-bbox / 1.0 queries in the future)
            if (True): #has_bbox:
                where_clause = f"""
                WHERE bbox.xmin BETWEEN {bbox.xMinimum()} AND {bbox.xMaximum()}
                AND bbox.ymin BETWEEN {bbox.yMinimum()} AND {bbox.yMaximum()}
                """
            else:
                # Right now this will only work against epsg:4326 data - if we want to make it more robust
                # then should try to transform. Or else tell users the BBOX in v1.1 is required...
                where_clause = f"""
                WHERE ST_Intersects(
                    geometry,
                    ST_GeomFromText('POLYGON(({bbox.xMinimum()} {bbox.yMinimum()},
                                            {bbox.xMaximum()} {bbox.yMinimum()},
                                            {bbox.xMaximum()} {bbox.yMaximum()},
                                            {bbox.xMinimum()} {bbox.yMaximum()},
                                            {bbox.xMinimum()} {bbox.yMinimum()}))')
                )
                """

            # Base query
            base_query = f"""
            CREATE TABLE {table_name} AS (
                {select_query} FROM read_parquet('{self.dataset_url}')
                {where_clause}
            ) 
            """
            self.progress.emit("Downloading data...")
            print("Executing SQL query:")
            print(base_query)
            conn.execute(base_query)
            
            # Add check for empty results
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if row_count == 0:
                self.error.emit("No data found in the requested area. Check that your map extent overlaps with the data and/or expand your map extent.")
                return

            self.progress.emit("Processing data to requested format...")

            file_extension = self.output_file.lower().split('.')[-1]

            if file_extension == 'duckdb':
                # Commit the transaction to ensure the data is saved
                conn.commit()
                if not self.killed:
                    self.info.emit(
                        "Data has been successfully saved to DuckDB database.\n\n"
                        "Note: QGIS does not currently support loading DuckDB files directly."
                    )
            else:
                copy_query = f"COPY {table_name} TO '{self.output_file}'"

                if file_extension == "parquet":
                    format_options = "(FORMAT 'parquet', COMPRESSION 'ZSTD');"
                elif self.output_file.endswith(".gpkg"):
                    format_options = "(FORMAT GDAL, DRIVER 'GPKG');"
                else:
                    self.error.emit("Unsupported file format.")
                
                print("Executing SQL query:")
                print(copy_query + format_options)
                conn.execute(copy_query + format_options)

            
            if self.killed:
                return

            if not self.killed:
                if self.output_file.lower().endswith('.duckdb'):
                    self.info.emit(
                        "Data has been successfully saved to DuckDB database.\n\n"
                        "Note: QGIS does not currently support loading DuckDB files directly."
                    )
                else:
                    self.load_layer.emit(self.output_file)
                self.finished.emit()

        except Exception as e:
            if not self.killed:
                self.error.emit(str(e))
        finally:
            if not self.output_file.lower().endswith('.duckdb'): # Clean up temporary table
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                except:
                    pass
            conn.close()

    def kill(self):
        self.killed = True

class ValidationWorker(QObject):
    finished = pyqtSignal(bool, str)
    progress = pyqtSignal(str)

    def __init__(self, dataset_url, iface, extent):
        super().__init__()
        self.dataset_url = dataset_url
        self.iface = iface
        self.extent = extent
        self.killed = False

    def needs_validation(self):
        #TODO: Change this to be driven by a list that is easier to manage
        # instead of having all this custom logic here.
        """Determine if the dataset needs any validation"""
        # Overture datasets don't need validation
        if "overturemaps-us-west-2" in self.dataset_url:
            return False
        # Source Cooperative datasets don't need validation
        if "data.source.coop" in self.dataset_url:
            return False
        # All other datasets need validation
        return True

    def run(self):
        try:
            self.progress.emit("Connecting to data source...")
            conn = duckdb.connect()
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            
            if not self.needs_validation():
                self.finished.emit(True, "Validation successful")
                return
            
            self.progress.emit("Checking data format...")
            schema_query = f"DESCRIBE SELECT * FROM read_parquet('{self.dataset_url}')"
            schema_result = conn.execute(schema_query).fetchall()
            has_bbox = any(row[0].lower() == 'bbox' for row in schema_result)
            
            if not has_bbox:
                self.finished.emit(False, "This plugin currently only supports GeoParquet 1.1 files with a bbox column.")
                return

            if (False): # TODO: See about checking this faster based on geoparquet metadata - it's too slow now.
                self.progress.emit("Checking data bounds...")
                bounds_query = f"""
                SELECT MIN(bbox.xmin) as min_x, MAX(bbox.xmax) as max_x,
                    MIN(bbox.ymin) as min_y, MAX(bbox.ymax) as max_y
                FROM read_parquet('{self.dataset_url}')
                """
                bounds_result = conn.execute(bounds_query).fetchone()
                
                if bounds_result and not self.killed:
                    min_x, max_x, min_y, max_y = bounds_result
                    source_crs = self.iface.mapCanvas().mapSettings().destinationCrs()
                    bbox = transform_bbox_to_4326(self.extent, source_crs)
                    request_bounds = (bbox.xMinimum(), bbox.xMaximum(), 
                                    bbox.yMinimum(), bbox.yMaximum())
                    
                    # Check for overlap
                    if (max_x < request_bounds[0] or min_x > request_bounds[1] or
                        max_y < request_bounds[2] or min_y > request_bounds[3]):
                        self.finished.emit(False, "The current view extent does not overlap with the data. " +
                                    f"\nData bounds: {min_x:.2f}, {min_y:.2f}, {max_x:.2f}, {max_y:.2f}" +
                                    f"\nRequested bounds: {request_bounds[0]:.2f}, {request_bounds[2]:.2f}, {request_bounds[1]:.2f}, {request_bounds[3]:.2f}")
                        return
                
            self.finished.emit(True, "Validation successful")

        except Exception as e:
            self.finished.emit(False, f"Error validating source: {str(e)}")
        finally:
            conn.close()

class DataSourceDialog(QDialog):
    def __init__(self, parent=None, iface=None):
        super().__init__(parent)
        self.iface = iface
        self.validation_thread = None
        self.validation_worker = None
        self.progress_message = None
        self.requires_validation = True
        self.setWindowTitle("GeoParquet Data Source")
        self.setMinimumWidth(500)
        
        # Create main layout
        layout = QVBoxLayout()
        
        # Create horizontal layout for radio buttons
        radio_layout = QHBoxLayout()
        
        # Create radio buttons
        self.custom_radio = QRadioButton("Custom URL")
        self.overture_radio = QRadioButton("Overture Maps")
        self.sourcecoop_radio = QRadioButton("Source Cooperative")
        self.other_radio = QRadioButton("Other Sources")
        
        # Add radio buttons to horizontal layout
        radio_layout.addWidget(self.custom_radio)
        radio_layout.addWidget(self.overture_radio)
        radio_layout.addWidget(self.sourcecoop_radio)
        radio_layout.addWidget(self.other_radio)
        
        # Add radio button layout to main layout
        layout.addLayout(radio_layout)
        
        # Add some spacing between radio buttons and content
        layout.addSpacing(10)
        
        # Create and setup the stacked widget for different options
        self.stack = QStackedWidget()
        
        # Custom URL page
        custom_page = QWidget()
        custom_layout = QVBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Enter URL to Parquet file or folder (s3:// or https://)")
        custom_layout.addWidget(self.url_input)
        custom_page.setLayout(custom_layout)
        
        # Overture Maps page
        overture_page = QWidget()
        overture_layout = QVBoxLayout()
        self.overture_combo = QComboBox()
        self.overture_combo.addItems([
            "Places", 
            "Buildings", 
            "Transportation",
            "Addresses",
            "Divisions",
            "Base"
        ])
        overture_layout.addWidget(self.overture_combo)
        
        # Add base subtype combo
        self.base_subtype_widget = QWidget()
        base_subtype_layout = QVBoxLayout()
        base_subtype_layout.setContentsMargins(20, 0, 0, 0)  # Add left margin for indentation
        self.base_subtype_label = QLabel("Base Layer Type:")
        self.base_subtype_combo = QComboBox()
        self.base_subtype_combo.addItems([
            "infrastructure",
            "land",
            "land_cover",
            "land_use",
            "water"
        ])
        base_subtype_layout.addWidget(self.base_subtype_label)
        base_subtype_layout.addWidget(self.base_subtype_combo)
        self.base_subtype_widget.setLayout(base_subtype_layout)
        self.base_subtype_widget.hide()  # Initially hidden
        
        overture_layout.addWidget(self.base_subtype_widget)
        overture_page.setLayout(overture_layout)
        
        # Connect the overture combo change signal
        self.overture_combo.currentTextChanged.connect(self.handle_overture_selection)
        
        # Source Cooperative page
        sourcecoop_page = QWidget()
        sourcecoop_layout = QVBoxLayout()
        self.sourcecoop_combo = QComboBox()
        self.sourcecoop_combo.addItems([
            "Planet EU Field Boundaries (2022)",
            "USDA Crop Sequence Boundaries",
            "California Crop Mapping",
            "VIDA Google/Microsoft/OSM Buildings"
        ])
        sourcecoop_layout.addWidget(self.sourcecoop_combo)

        # Add link label
        self.sourcecoop_link = QLabel()
        self.sourcecoop_link.setOpenExternalLinks(True)
        self.sourcecoop_link.setWordWrap(True)
        sourcecoop_layout.addWidget(self.sourcecoop_link)

        # Connect combo box change to update link
        self.sourcecoop_combo.currentTextChanged.connect(self.update_sourcecoop_link)
        sourcecoop_page.setLayout(sourcecoop_layout)
        
        # Other sources page
        other_page = QWidget()
        other_layout = QVBoxLayout()
        self.other_combo = QComboBox()
        self.other_combo.addItems([
            "Foursquare Places"
        ])
        other_layout.addWidget(self.other_combo)
        other_page.setLayout(other_layout)
        
        # Add pages to stack
        self.stack.addWidget(custom_page)
        self.stack.addWidget(overture_page)
        self.stack.addWidget(sourcecoop_page)
        self.stack.addWidget(other_page)
        
        layout.addWidget(self.stack)
        
        # Buttons
        button_layout = QHBoxLayout()
        self.ok_button = QPushButton("OK")
        self.cancel_button = QPushButton("Cancel")
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        
        # Connect signals
        self.custom_radio.toggled.connect(lambda: self.stack.setCurrentIndex(0))
        self.overture_radio.toggled.connect(lambda: self.stack.setCurrentIndex(1))
        self.sourcecoop_radio.toggled.connect(lambda: self.stack.setCurrentIndex(2))
        self.other_radio.toggled.connect(lambda: self.stack.setCurrentIndex(3))
        self.ok_button.clicked.connect(self.validate_and_accept)
        self.cancel_button.clicked.connect(self.reject)
        
        # Set initial state
        self.custom_radio.setChecked(True)
        
        # Add after setting up the sourcecoop_combo
        self.update_sourcecoop_link(self.sourcecoop_combo.currentText())
        
    def handle_overture_selection(self, text):
        """Show/hide base subtype combo based on selection"""
        self.base_subtype_widget.setVisible(text == "Base")

    def validate_and_accept(self):
        """Validate the input and accept the dialog if valid"""
        url = self.get_url()
        if not url:
            QMessageBox.warning(self, "Validation Error", "Please enter a URL or select a dataset")
            return
        
        # For custom URLs, do some basic validation
        if self.custom_radio.isChecked():
            if not (url.startswith('http://') or url.startswith('https://') or 
                   url.startswith('s3://') or url.startswith('file://')):
                QMessageBox.warning(self, "Validation Error", 
                    "URL must start with http://, https://, s3://, or file://")
                return

        # Set requires_validation based on the selected dataset
        self.requires_validation = True
        if self.overture_radio.isChecked() or \
           (self.sourcecoop_radio.isChecked()):
            self.requires_validation = False

        # Create progress dialog
        self.progress_dialog = QProgressDialog("Starting validation...", "Cancel", 0, 0, self)
        self.progress_dialog.setWindowTitle("Validating Data Source")
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.setMinimumDuration(0)
        
        # Get the current canvas extent
        extent = self.iface.mapCanvas().extent()
        
        # Setup validation worker
        self.validation_worker = ValidationWorker(url, self.iface, extent)
        self.validation_thread = QThread()
        self.validation_worker.moveToThread(self.validation_thread)
        
        # Connect signals
        self.validation_thread.started.connect(self.validation_worker.run)
        self.validation_worker.progress.connect(self.update_progress)
        self.validation_worker.finished.connect(self.handle_validation_result)
        self.validation_worker.finished.connect(lambda: self.cleanup_validation(True))
        self.progress_dialog.canceled.connect(lambda: self.cleanup_validation(False))
        
        # Start validation
        self.validation_thread.start()
        self.progress_dialog.show()

    def update_progress(self, message):
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.setLabelText(message)

    def handle_validation_result(self, success, message):
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.close()
        
        if success:
            self.accept()
        else:
            QMessageBox.warning(self, "Validation Error", message)

    def cleanup_validation(self, success):
        if self.validation_worker:
            self.validation_worker.deleteLater()
            self.validation_worker = None
            
        if self.validation_thread:
            self.validation_thread.quit()
            self.validation_thread.wait()
            self.validation_thread.deleteLater()
            self.validation_thread = None

        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None

        if not success:
            self.reject()

    def closeEvent(self, event):
        """Handle dialog closing"""
        self.cleanup_validation(False)
        super().closeEvent(event)

    def get_url(self):
        if self.custom_radio.isChecked():
            return self.url_input.text().strip()
        elif self.overture_radio.isChecked():
            theme = self.overture_combo.currentText().lower()
            if theme == "transportation":
                type_str = "segment"
            elif theme == "divisions":
                type_str = "division_area"
            elif theme == "addresses":
                type_str = "*"
            elif theme == "base":
                type_str = self.base_subtype_combo.currentText()
            else:
                type_str = theme.rstrip('s')  # remove trailing 's' for singular form
            return f"s3://overturemaps-us-west-2/release/2024-11-13.0/theme={theme}/type={type_str}/*"
        elif self.sourcecoop_radio.isChecked():
            selection = self.sourcecoop_combo.currentText()
            if selection == "USDA Crop Sequence Boundaries":
                return "https://source.coop/fiboa/us-usda-cropland/us_usda_cropland.parquet"
            elif selection == "California Crop Mapping":
                return "https://data.source.coop/fiboa/us-ca-scm/us_ca_scm.parquet"
            elif selection == "Planet EU Field Boundaries (2022)":
                return "https://data.source.coop/planet/eu-field-boundaries/field_boundaries.parquet"
            elif selection == "VIDA Google/Microsoft/OSM Buildings":
                return "s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country/*/*.parquet"
        elif self.other_radio.isChecked():
            selection = self.other_combo.currentText()
            if selection == "Foursquare Places":
                QMessageBox.warning(self, "Not Implemented", 
                    "Foursquare Places integration is not yet implemented. Please select another data source.")
                return ""
        return ""

    def update_sourcecoop_link(self, selection):
        """Update the link based on the selected dataset"""
        if selection == "Planet EU Field Boundaries (2022)":
            self.sourcecoop_link.setText('<a href="https://source.coop/repositories/planet/eu-field-boundaries/description">View dataset description</a>')
        elif selection == "USDA Crop Sequence Boundaries":
            self.sourcecoop_link.setText('<a href="https://source.coop/fiboa/us-usda-cropland/description">View dataset description</a>')
        elif selection == "California Crop Mapping":
            self.sourcecoop_link.setText('<a href="https://source.coop/repositories/fiboa/us-ca-scm/description">View dataset description</a>')
        elif selection == "VIDA Google/Microsoft/OSM Buildings":
            self.sourcecoop_link.setText('<a href="https://source.coop/repositories/vida/google-microsoft-osm-open-buildings/description">View dataset description</a>')
        else:
            self.sourcecoop_link.setText('')

class QgisPluginGeoParquet:
    def __init__(self, iface):
        self.iface = iface
        self.worker = None
        self.worker_thread = None
        self.action = None
        # Create a default downloads directory in user's home directory
        self.download_dir = Path.home() / "Downloads" 
        # Create the directory if it doesn't exist
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def initGui(self):
        # Create the action with the icon and tooltip
        icon_path = ':/qgis_plugin_gpq_downloader/icons/download.svg'
        self.action = QAction(
            QIcon(icon_path),
            "Download GeoParquet Data", 
            self.iface.mainWindow()
        )
        self.action.setToolTip("Download GeoParquet Data")
        self.action.triggered.connect(self.run)

        # Create the Overture action
        overture_icon_path = ':/qgis_plugin_gpq_downloader/icons/overture-logo.svg'
        self.overture_action = QAction(
            QIcon(overture_icon_path),
            "Download Overture Data", 
            self.iface.mainWindow()
        )
        self.overture_action.setToolTip("Download Overture Maps Data")
        self.overture_action.triggered.connect(lambda: self.run(default_source='overture'))

        # Create the Source Cooperative action
        sourcecoop_icon_path = ':/qgis_plugin_gpq_downloader/icons/source-coop.svg'
        self.sourcecoop_action = QAction(
            QIcon(sourcecoop_icon_path),
            "Download Source Cooperative Data", 
            self.iface.mainWindow()
        )
        self.sourcecoop_action.setToolTip("Download Source Cooperative Data")
        self.sourcecoop_action.triggered.connect(lambda: self.run(default_source='sourcecoop'))

        # Add the actions to the toolbar
        self.iface.addToolBarIcon(self.action)
        self.iface.addToolBarIcon(self.overture_action)
        self.iface.addToolBarIcon(self.sourcecoop_action)

    def unload(self):
        # Clean up worker and thread when plugin is unloaded
        self.cleanup_thread()
        # Remove all actions from the toolbar
        self.iface.removeToolBarIcon(self.action)
        self.iface.removeToolBarIcon(self.overture_action)
        self.iface.removeToolBarIcon(self.sourcecoop_action)

    def run(self, default_source=None):
        # Show the data source dialog
        dialog = DataSourceDialog(self.iface.mainWindow(), self.iface)
        
        # Set the default radio button based on the source
        if default_source == 'overture':
            dialog.overture_radio.setChecked(True)
        elif default_source == 'sourcecoop':
            dialog.sourcecoop_radio.setChecked(True)
        
        if dialog.exec_() != QDialog.Accepted:
            return
        
        dataset_url = dialog.get_url()
        
        # Get the current canvas extent
        extent = self.iface.mapCanvas().extent()
        
        # Generate default filename
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"geoparquet_download_{timestamp}.parquet"
        default_save_path = str(self.download_dir / default_filename)
        
        # Show save file dialog with DuckDB option
        output_file, selected_filter = QFileDialog.getSaveFileName(
            self.iface.mainWindow(),
            "Save Data",
            default_save_path,
            "GeoParquet (*.parquet);;DuckDB Database (*.duckdb);;GeoPackage (*.gpkg)"
        )
        
        if output_file:
            # Add extension if not present
            if not any(output_file.lower().endswith(ext) for ext in ['.duckdb', '.parquet', '.gpkg']):
                if selected_filter == "DuckDB Database (*.duckdb)":
                    output_file += '.duckdb'
                elif selected_filter == "GeoParquet (*.parquet)":
                    output_file += '.parquet'
                elif selected_filter == "GeoPackage (*.gpkg)":
                    output_file += '.gpkg'
            
            # Remove the validation check since it's already done in DataSourceDialog
            self.download_and_save(dataset_url, extent, output_file)

    def download_and_save(self, dataset_url, extent: QgsRectangle, output_file: str):
        # Clean up any existing worker/thread
        if self.worker_thread is not None:
            self.cleanup_thread()

        # Create progress dialog
        self.progress_dialog = QProgressDialog("Starting download...", "Cancel", 0, 0, self.iface.mainWindow())
        self.progress_dialog.setWindowTitle("Downloading Data")
        self.progress_dialog.setWindowModality(Qt.NonModal)
        self.progress_dialog.setMinimumDuration(0)
        self.progress_dialog.show()
        
        # Create new worker and thread
        self.worker = Worker(dataset_url, extent, output_file, self.iface)
        self.worker_thread = QThread()
        
        # Move worker to thread
        self.worker.moveToThread(self.worker_thread)
        
        # Connect signals
        self.worker_thread.started.connect(self.worker.run)
        self.worker.error.connect(self.handle_error)
        self.worker.load_layer.connect(self.load_layer)
        self.worker.info.connect(self.show_info)
        self.worker.finished.connect(self.cleanup_thread)
        self.worker.progress.connect(self.update_progress)
        self.progress_dialog.canceled.connect(self.cancel_download)
        
        # Start the thread
        self.worker_thread.start()

    def handle_error(self, message):
        self.progress_dialog.close()
        QMessageBox.critical(self.iface.mainWindow(), "Error", message)

    def update_progress(self, message):
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.setLabelText(message)

    def cancel_download(self):
        if self.worker:
            self.worker.kill()
        self.cleanup_thread()

    def cleanup_thread(self):
        if self.worker_thread is not None:
            if self.worker:
                self.worker.kill()
            self.worker_thread.quit()
            self.worker_thread.wait()
            self.worker_thread = None
            self.worker = None
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.close()

    def load_layer(self, output_file):
        """Load the layer into QGIS if GeoParquet is supported"""
        if output_file.lower().endswith('.parquet'):
            # Try to create a test layer to check GeoParquet support
            test_layer = QgsVectorLayer(output_file, "test", "ogr")
            if not test_layer.isValid():
                dialog = QDialog(self.iface.mainWindow())
                dialog.setWindowTitle("GeoParquet Support Not Available")
                dialog.setMinimumWidth(400)
                
                layout = QVBoxLayout()
                
                message = QLabel(
                    "Data has been successfully saved to GeoParquet file.\n\n"
                    "Note: Your current QGIS installation does not support reading GeoParquet files directly.\n\n"
                    "To view GeoParquet files in QGIS, you'll need to install QGIS with GDAL 3.8 "
                    "or higher with 'libgdal-arrow-parquet'. You can find instructions at:"
                )
                message.setWordWrap(True)
                layout.addWidget(message)
                
                link = QLabel()
                link.setText('<a href="https://github.com/cholmes/qgis_plugin_gpq_downloader/Installing-GeoParquet-QGIS.md">Installing GeoParquet Support in QGIS</a>')
                link.setOpenExternalLinks(True)
                layout.addWidget(link)
                
                button_box = QPushButton("OK")
                button_box.clicked.connect(dialog.accept)
                layout.addWidget(button_box)
                
                dialog.setLayout(layout)
                dialog.exec_()
                return

        # If we get here, either it's not a parquet file or GeoParquet is supported
        layer = QgsVectorLayer(output_file, "Downloaded Layer", "ogr")
        if not layer.isValid():
            QMessageBox.critical(self.iface.mainWindow(), "Error", "Failed to load the layer.")
            return

        QgsProject.instance().addMapLayer(layer)

    def show_info(self, message):
        """Show an information message to the user"""
        QMessageBox.information(self.iface.mainWindow(), "Success", message)

def classFactory(iface):
    return QgisPluginGeoParquet(iface)
