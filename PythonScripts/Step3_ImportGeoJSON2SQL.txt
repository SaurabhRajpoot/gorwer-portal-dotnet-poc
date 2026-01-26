# ---  Step 1: Import all the tools this script needs ---
import os
import geopandas as gpd
import pandas as pd
import json
import sqlalchemy as sal
from sqlalchemy import text
import gc
from datetime import datetime

# === Step 2: User inputs ===
input_folder = r"C:\\ZespriWorkspace\\Data\\ExportedGeoJSON"
output_folder = r"C:\\ZespriWorkspace\\Data\\ExportedGeoJSON\\GeoJSON_Outputs"
schema_file = r"C:\\ZespriWorkspace\\Scripts\\schemaMapper.xlsx"
log_folder = r"C:\\ZespriWorkspace\\logs"
# --- Create output + log folders if missing ---
os.makedirs(output_folder, exist_ok=True)
os.makedirs(log_folder, exist_ok=True)

# --- Create log file ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path = os.path.join(log_folder, f"PythonScript2_log_{timestamp}.txt")

def log(msg):
    print(msg)
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(msg + "\n")

log(f"Log started: {datetime.now()}")
log(f"Input folder: {input_folder}")
log(f"Output folder: {output_folder}")
log(f"Schema file: {schema_file}")

# --- SQL database connection ---
constr = 'mssql+pyodbc://WIN-K01V82HRITO/SpatialDB?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
engine = sal.create_engine(constr)
schema_name = 'dbo'

# --- Find all GeoJSON files in your input folder ---
geojson_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".geojson")]
log(f"Found {len(geojson_files)} GeoJSON file(s): {geojson_files}")

# --- Load the excel "Schema Mapper" ---
try:
    sheet_names = pd.ExcelFile(schema_file).sheet_names
    log(f"Loaded schema mapper with sheets: {sheet_names}")
except Exception as e:
    log(f"Error reading schema mapper file: {e}")
    sheet_names = []

# === Helper function ===
def wkb_hexer(geom):
    return geom.wkb_hex if geom else None

# === Step 3: Main loop ===
for file_name in geojson_files:
    input_path = os.path.join(input_folder, file_name)
    base_name = os.path.splitext(file_name)[0]
    log(f"\nProcessing: {file_name}")

    gdf = None

    try:
        # === Load GeoJSON ===
        gdf = gpd.read_file(input_path)
        log(" - GeoJSON file loaded successfully.")
        log(f" - Detected CRS: {gdf.crs}")

        # === Step 3.1: CRS handling ===
        if gdf.crs is None:
            log(" - Warning: No CRS detected in file.")
        else:
            log(f" - Detected CRS: {gdf.crs}")

        if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
            log(" - Reprojecting to WGS84 (EPSG:4326)...")
            gdf = gdf.to_crs(epsg=4326)
        elif gdf.crs is None:
            log(" - Skipped reprojection (no CRS defined).")
        else:
            log(" - Already in WGS84 (EPSG:4326).")

        # === Step 3.2: Add derived fields ===
        if "blockid" in gdf.columns:
            log(" - Creating new field 'puid' from 'blockid'")
            gdf["puid"] = gdf["blockid"]
        else:
            log(" - Warning: 'blockid' not found. Skipping 'puid' creation.")

        log(" - Adding 'Geometry_Type' field")
        gdf["Geometry_Type"] = gdf.geometry.geom_type

        if "created_date" in gdf.columns and "last_edited_date" in gdf.columns:
            log(" - Adding 'Geometry_Status' field based on date comparison")
            def get_status(row):
                c = str(row["created_date"])
                e = str(row["last_edited_date"])
                if pd.isna(c) or pd.isna(e):
                    return "Unknown"
                return "New" if c == e else "Updated"
            gdf["Geometry_Status"] = gdf.apply(get_status, axis=1)
        else:
            log(" - Missing date fields; setting Geometry_Status = 'Unknown'")
            gdf["Geometry_Status"] = "Unknown"

        # === Step 3.3: Field renaming from Schema Mapper ===
        if base_name in sheet_names:
            log(f" - Found matching schema sheet: {base_name}")
            df_map = pd.read_excel(schema_file, sheet_name=base_name)
            df_map = df_map.dropna(subset=["oldFieldName", "newFieldName"])
            if {"oldFieldName", "newFieldName"}.issubset(df_map.columns):
                rename_dict = dict(zip(df_map["oldFieldName"], df_map["newFieldName"]))
                rename_dict = {old: new for old, new in rename_dict.items() if old in gdf.columns}
                if rename_dict:
                    log(f" - Renaming {len(rename_dict)} fields")
                    gdf = gdf.rename(columns=rename_dict)
                else:
                    log(" - No matching fields to rename.")
            else:
                log(" - Schema mapper missing columns. Skipping renaming.")
        else:
            log(f" - No matching schema sheet for {base_name}. Skipping renaming.")

        # === Step 4: Save output GeoJSON ===
        output_path = os.path.join(output_folder, file_name)
        gdf.columns = gdf.columns.map(str)
        gdf.to_file(output_path, driver="GeoJSON")
        log(f" - Saved updated GeoJSON to: {output_path}")

        # === Step 5: Prepare for SQL Upload ===
        log(" - Preparing data for SQL upload")
        gdf["tmp_geom"] = gdf["geometry"].apply(wkb_hexer)
        gdf.drop("geometry", axis=1, inplace=True)

        # --- Upload to SQL Database ---
        log(f" - Uploading {base_name} to SQL...")
        conn = engine.connect()

        gdf.to_sql(base_name, con=engine, schema=schema_name, if_exists="replace", index=False)

        sqlQRY = f"ALTER TABLE {schema_name}.{base_name} ADD geom GEOGRAPHY"
        conn.execute(text(sqlQRY))

        sqlQRY = f"""
            UPDATE {schema_name}.{base_name}
            SET geom = geography::STGeomFromWKB(CONVERT(VARBINARY(MAX), tmp_geom, 2), 4326)
        """
        conn.execute(text(sqlQRY))

        sqlQRY = f"ALTER TABLE {schema_name}.{base_name} DROP COLUMN tmp_geom"
        conn.execute(text(sqlQRY))

        # === Step 6: Clean up SQL memory and python session memory ===

        conn.commit()
        conn.close()
        log(f" - Successfully loaded {base_name} into SQL Server.")

    except Exception as e:
        log(f"Failed to process {file_name}: {e}")

    finally:
        if gdf is not None:
            del gdf
        gc.collect()

log("\nAll done! All GeoJSON files processed and loaded into SQL successfully.")
log(f"Log finished: {datetime.now()}")
