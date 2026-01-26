using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using NetTopologySuite.Features;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using OfficeOpenXml;

namespace GeoJsonProcessor
{
    public class Program
    {
        // === Step 2: User inputs ===
        private static readonly string InputFolder = @"C:\ZespriWorkspace\Data\ExportedGeoJSON";
        private static readonly string OutputFolder = @"C:\ZespriWorkspace\Data\ExportedGeoJSON\GeoJSON_Outputs";
        private static readonly string SchemaFile = @"C:\ZespriWorkspace\Scripts\schemaMapper.xlsx";
        private static readonly string LogFolder = @"C:\ZespriWorkspace\logs";

        private static readonly string ConnectionString =
            "Server=WIN-K01V82HRITO;Database=SpatialDB;Trusted_Connection=yes;TrustServerCertificate=true;";

        private static readonly string SchemaName = "dbo";

        private static StreamWriter? _logWriter;

        public static void Main(string[] args)
        {
            // Create output + log folders if missing
            Directory.CreateDirectory(OutputFolder);
            Directory.CreateDirectory(LogFolder);

            // Create log file
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var logPath = Path.Combine(LogFolder, $"CSharpProcessor_log_{timestamp}.txt");
            _logWriter = new StreamWriter(logPath, append: true) { AutoFlush = true };

            // EPPlus license context (required for EPPlus 5+)
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            try
            {
                Log($"Log started: {DateTime.Now}");
                Log($"Input folder: {InputFolder}");
                Log($"Output folder: {OutputFolder}");
                Log($"Schema file: {SchemaFile}");

                // Find all GeoJSON files in input folder
                var geojsonFiles = Directory.GetFiles(InputFolder, "*.geojson", SearchOption.TopDirectoryOnly);
                Log($"Found {geojsonFiles.Length} GeoJSON file(s): {string.Join(", ", geojsonFiles.Select(Path.GetFileName))}");

                // Load the Excel schema mapper sheet names
                var sheetNames = GetExcelSheetNames(SchemaFile);
                Log($"Loaded schema mapper with sheets: {string.Join(", ", sheetNames)}");

                // === Step 3: Main loop ===
                foreach (var filePath in geojsonFiles)
                {
                    var fileName = Path.GetFileName(filePath);
                    var baseName = Path.GetFileNameWithoutExtension(fileName);
                    Log($"\nProcessing: {fileName}");

                    try
                    {
                        ProcessGeoJsonFile(filePath, baseName, sheetNames);
                    }
                    catch (Exception ex)
                    {
                        Log($"Failed to process {fileName}: {ex.Message}");
                    }

                    // Force garbage collection (equivalent to Python's gc.collect())
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }

                Log("\nAll done! All GeoJSON files processed and loaded into SQL successfully.");
                Log($"Log finished: {DateTime.Now}");
            }
            finally
            {
                _logWriter?.Dispose();
            }
        }

        private static void ProcessGeoJsonFile(string inputPath, string baseName, List<string> sheetNames)
        {
            // === Load GeoJSON ===
            var geoJsonReader = new GeoJsonReader();
            var featureCollection = ReadGeoJsonFile(inputPath);
            Log(" - GeoJSON file loaded successfully.");

            // Build a list of feature dictionaries for processing
            var features = new List<Dictionary<string, object?>>();

            foreach (var feature in featureCollection)
            {
                var featureDict = new Dictionary<string, object?>();

                // Copy all attributes
                foreach (var attrName in feature.Attributes.GetNames())
                {
                    featureDict[attrName] = feature.Attributes[attrName];
                }

                // Store geometry
                featureDict["geometry"] = feature.Geometry;

                features.Add(featureDict);
            }

            // === Step 3.1: CRS handling ===
            // Note: NetTopologySuite doesn't track CRS in the same way as GeoPandas
            // Assuming input is already WGS84 (EPSG:4326) or handling reprojection separately
            Log(" - Assuming WGS84 (EPSG:4326) coordinate system.");

            // === Step 3.2: Add derived fields ===
            foreach (var feature in features)
            {
                // Create 'puid' from 'blockid'
                if (feature.ContainsKey("blockid"))
                {
                    feature["puid"] = feature["blockid"];
                    if (features.IndexOf(feature) == 0)
                        Log(" - Creating new field 'puid' from 'blockid'");
                }
                else if (features.IndexOf(feature) == 0)
                {
                    Log(" - Warning: 'blockid' not found. Skipping 'puid' creation.");
                }

                // Add Geometry_Type
                var geometry = feature["geometry"] as Geometry;
                feature["Geometry_Type"] = geometry?.GeometryType ?? "Unknown";

                // Add Geometry_Status based on date comparison
                if (feature.ContainsKey("created_date") && feature.ContainsKey("last_edited_date"))
                {
                    var createdDate = feature["created_date"]?.ToString();
                    var editedDate = feature["last_edited_date"]?.ToString();

                    if (string.IsNullOrEmpty(createdDate) || string.IsNullOrEmpty(editedDate))
                    {
                        feature["Geometry_Status"] = "Unknown";
                    }
                    else
                    {
                        feature["Geometry_Status"] = createdDate == editedDate ? "New" : "Updated";
                    }
                }
                else
                {
                    feature["Geometry_Status"] = "Unknown";
                }
            }

            Log(" - Adding 'Geometry_Type' field");
            if (features.Count > 0 && features[0].ContainsKey("created_date") && features[0].ContainsKey("last_edited_date"))
            {
                Log(" - Adding 'Geometry_Status' field based on date comparison");
            }
            else
            {
                Log(" - Missing date fields; setting Geometry_Status = 'Unknown'");
            }

            // === Step 3.3: Field renaming from Schema Mapper ===
            if (sheetNames.Contains(baseName, StringComparer.OrdinalIgnoreCase))
            {
                Log($" - Found matching schema sheet: {baseName}");
                var renameDict = LoadRenameMapping(SchemaFile, baseName);

                if (renameDict.Count > 0)
                {
                    Log($" - Renaming {renameDict.Count} fields");
                    foreach (var feature in features)
                    {
                        var keysToRename = feature.Keys
                            .Where(k => renameDict.ContainsKey(k))
                            .ToList();

                        foreach (var oldKey in keysToRename)
                        {
                            var value = feature[oldKey];
                            feature.Remove(oldKey);
                            feature[renameDict[oldKey]] = value;
                        }
                    }
                }
                else
                {
                    Log(" - No matching fields to rename.");
                }
            }
            else
            {
                Log($" - No matching schema sheet for {baseName}. Skipping renaming.");
            }

            // === Step 4: Save output GeoJSON ===
            var outputPath = Path.Combine(OutputFolder, Path.GetFileName(inputPath));
            SaveGeoJsonFile(features, outputPath);
            Log($" - Saved updated GeoJSON to: {outputPath}");

            // === Step 5: Upload to SQL Database ===
            Log(" - Preparing data for SQL upload");
            Log($" - Uploading {baseName} to SQL...");
            UploadToSqlServer(features, baseName);
            Log($" - Successfully loaded {baseName} into SQL Server.");
        }

        private static FeatureCollection ReadGeoJsonFile(string path)
        {
            var json = File.ReadAllText(path);
            var reader = new GeoJsonReader();
            return reader.Read<FeatureCollection>(json);
        }

        private static void SaveGeoJsonFile(List<Dictionary<string, object?>> features, string outputPath)
        {
            var featureCollection = new FeatureCollection();

            foreach (var featureDict in features)
            {
                var geometry = featureDict["geometry"] as Geometry;
                var attributes = new AttributesTable();

                foreach (var kvp in featureDict)
                {
                    if (kvp.Key != "geometry")
                    {
                        attributes.Add(kvp.Key, kvp.Value);
                    }
                }

                var feature = new Feature(geometry, attributes);
                featureCollection.Add(feature);
            }

            var writer = new GeoJsonWriter();
            var json = writer.Write(featureCollection);
            File.WriteAllText(outputPath, json);
        }

        private static List<string> GetExcelSheetNames(string excelPath)
        {
            var sheetNames = new List<string>();

            try
            {
                using var package = new ExcelPackage(new FileInfo(excelPath));
                sheetNames.AddRange(package.Workbook.Worksheets.Select(ws => ws.Name));
            }
            catch (Exception ex)
            {
                Log($"Error reading schema mapper file: {ex.Message}");
            }

            return sheetNames;
        }

        private static Dictionary<string, string> LoadRenameMapping(string excelPath, string sheetName)
        {
            var renameDict = new Dictionary<string, string>();

            try
            {
                using var package = new ExcelPackage(new FileInfo(excelPath));
                var worksheet = package.Workbook.Worksheets[sheetName];

                if (worksheet == null) return renameDict;

                // Find column indices for oldFieldName and newFieldName
                int oldFieldCol = -1, newFieldCol = -1;
                var headerRow = 1;

                for (int col = 1; col <= worksheet.Dimension?.Columns; col++)
                {
                    var header = worksheet.Cells[headerRow, col].Text;
                    if (header == "oldFieldName") oldFieldCol = col;
                    if (header == "newFieldName") newFieldCol = col;
                }

                if (oldFieldCol == -1 || newFieldCol == -1)
                {
                    Log(" - Schema mapper missing columns. Skipping renaming.");
                    return renameDict;
                }

                // Read mappings
                for (int row = 2; row <= worksheet.Dimension?.Rows; row++)
                {
                    var oldName = worksheet.Cells[row, oldFieldCol].Text;
                    var newName = worksheet.Cells[row, newFieldCol].Text;

                    if (!string.IsNullOrWhiteSpace(oldName) && !string.IsNullOrWhiteSpace(newName))
                    {
                        renameDict[oldName] = newName;
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"Error loading rename mapping: {ex.Message}");
            }

            return renameDict;
        }

        private static void UploadToSqlServer(List<Dictionary<string, object?>> features, string tableName)
        {
            if (features.Count == 0) return;

            using var connection = new SqlConnection(ConnectionString);
            connection.Open();

            // Get all column names (excluding geometry)
            var columns = features[0].Keys
                .Where(k => k != "geometry")
                .ToList();

            // Drop table if exists
            var dropTableSql = $@"
                IF OBJECT_ID('{SchemaName}.{tableName}', 'U') IS NOT NULL
                    DROP TABLE {SchemaName}.{tableName}";

            using (var cmd = new SqlCommand(dropTableSql, connection))
            {
                cmd.ExecuteNonQuery();
            }

            // Create table with dynamic columns + tmp_geom
            var columnDefs = columns.Select(col =>
            {
                // Determine SQL type based on sample data
                var sampleValue = features.FirstOrDefault(f => f[col] != null)?[col];
                var sqlType = GetSqlType(sampleValue);
                return $"[{col}] {sqlType}";
            }).ToList();

            columnDefs.Add("[tmp_geom] NVARCHAR(MAX)");

            var createTableSql = $@"
                CREATE TABLE {SchemaName}.{tableName} (
                    {string.Join(",\n                    ", columnDefs)}
                )";

            using (var cmd = new SqlCommand(createTableSql, connection))
            {
                cmd.ExecuteNonQuery();
            }

            // Insert data
            foreach (var feature in features)
            {
                var geometry = feature["geometry"] as Geometry;
                var wkbHex = geometry != null ? ConvertToWkbHex(geometry) : null;

                var insertColumns = string.Join(", ", columns.Select(c => $"[{c}]")) + ", [tmp_geom]";
                var insertParams = string.Join(", ", columns.Select((c, i) => $"@p{i}")) + ", @pGeom";

                var insertSql = $"INSERT INTO {SchemaName}.{tableName} ({insertColumns}) VALUES ({insertParams})";

                using var cmd = new SqlCommand(insertSql, connection);
                for (int i = 0; i < columns.Count; i++)
                {
                    var value = feature[columns[i]];
                    cmd.Parameters.AddWithValue($"@p{i}", value ?? DBNull.Value);
                }
                cmd.Parameters.AddWithValue("@pGeom", (object?)wkbHex ?? DBNull.Value);
                cmd.ExecuteNonQuery();
            }

            // Add geography column
            var addGeomColSql = $"ALTER TABLE {SchemaName}.{tableName} ADD geom GEOGRAPHY";
            using (var cmd = new SqlCommand(addGeomColSql, connection))
            {
                cmd.ExecuteNonQuery();
            }

            // Convert WKB to geography
            var updateGeomSql = $@"
                UPDATE {SchemaName}.{tableName}
                SET geom = geography::STGeomFromWKB(CONVERT(VARBINARY(MAX), tmp_geom, 2), 4326)";
            using (var cmd = new SqlCommand(updateGeomSql, connection))
            {
                cmd.ExecuteNonQuery();
            }

            // Drop temporary column
            var dropTmpColSql = $"ALTER TABLE {SchemaName}.{tableName} DROP COLUMN tmp_geom";
            using (var cmd = new SqlCommand(dropTmpColSql, connection))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private static string? ConvertToWkbHex(Geometry geometry)
        {
            if (geometry == null) return null;

            var writer = new WKBWriter();
            var wkbBytes = writer.Write(geometry);
            return BitConverter.ToString(wkbBytes).Replace("-", "");
        }

        private static string GetSqlType(object? value)
        {
            return value switch
            {
                int or long => "BIGINT",
                double or float or decimal => "FLOAT",
                bool => "BIT",
                DateTime => "DATETIME2",
                _ => "NVARCHAR(MAX)"
            };
        }

        private static void Log(string message)
        {
            Console.WriteLine(message);
            _logWriter?.WriteLine(message);
        }
    }
}
