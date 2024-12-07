import os
import sys
from pathlib import Path
import subprocess

def ensure_duckdb():
    try:
        import duckdb
        print("DuckDB already installed")
        return True
    except ImportError:
        print("DuckDB not found, attempting to install...")
        try:
            # Get the Python executable that's running QGIS
            python_exe = sys.executable
            
            # Install duckdb using pip
            subprocess.check_call([python_exe, "-m", "pip", "install", "--user", "duckdb"])
            
            # Try importing again
            import duckdb
            print("DuckDB installed successfully")
            return True
        except Exception as e:
            print(f"Failed to install DuckDB: {str(e)}")
            return False

# Try to install/import duckdb
if not ensure_duckdb():
    print("Failed to setup DuckDB")

# Setup the path for duckdb
plugin_dir = os.path.dirname(__file__)
ext_libs_path = os.path.join(plugin_dir, 'ext-libs')
duckdb_path = os.path.join(ext_libs_path, 'duckdb')

# Add paths to sys.path if they're not already there
for path in [ext_libs_path, duckdb_path]:
    if path not in sys.path:
        sys.path.insert(0, path)

def classFactory(iface):
    from .qgis_plugin_gpq_downloader import QgisPluginGeoParquet
    return QgisPluginGeoParquet(iface)
