import os
import platform
import subprocess
import sys
from pathlib import Path

def ensure_duckdb():
    try:
        import duckdb
        version = duckdb.__version__
        from packaging import version as version_parser
        
        if version_parser.parse(version) >= version_parser.parse("1.1.0"):
            print(f"DuckDB {version} already installed")
            # Install required extensions
            try:
                conn = duckdb.connect(':memory:')
                conn.execute("INSTALL httpfs;")
                conn.execute("INSTALL spatial;")
                conn.close()
                print("DuckDB extensions installed successfully")
            except Exception as e:
                print(f"Failed to install DuckDB extensions: {str(e)}")
                return False
            return True
        else:
            print(f"DuckDB {version} found but needs upgrade to 1.1.0+")
            raise ImportError("Version too old")
            
    except ImportError:
        print("DuckDB not found or needs upgrade, attempting to install/upgrade...")
        try:
            if platform.system() == "Windows":
                py_path = os.path.join(os.path.dirname(sys.executable), "python.exe")
            else:
                py_path = sys.executable
            subprocess.check_call([py_path, "-m", "pip", "install", "--user", "duckdb"])
            
            # Force Python to reload all modules to pick up the new installation
            import importlib
            importlib.invalidate_caches()
            
            # Try importing again
            import duckdb
            print(f"DuckDB {duckdb.__version__} installed successfully")
            return True
        except Exception as e:
            print(f"Failed to install/upgrade DuckDB: {str(e)}")
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
