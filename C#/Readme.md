# GeoJSON Processor

A C# application that processes GeoJSON files, applies field transformations based on an Excel schema mapper, and uploads the data to SQL Server with geography support.

## Features

- Batch processes all GeoJSON files in a specified input folder
- Adds derived fields (`puid`, `Geometry_Type`, `Geometry_Status`)
- Renames fields based on an Excel schema mapping file
- Outputs transformed GeoJSON files
- Uploads data to SQL Server with native `GEOGRAPHY` column support
- Generates timestamped log files for each run

## Prerequisites

- .NET 8.0 SDK or later
- SQL Server with spatial data support
- ODBC Driver 17 for SQL Server

## Installation

1. Clone or download the project files
2. Restore NuGet packages:

```bash
dotnet restore
```

3. Build the project:

```bash
dotnet build
```

## Configuration

Edit the following constants in `GeoJsonProcessor.cs` to match your environment:

```csharp
private static readonly string InputFolder = @"C:\ZespriWorkspace\Data\ExportedGeoJSON";
private static readonly string OutputFolder = @"C:\ZespriWorkspace\Data\ExportedGeoJSON\GeoJSON_Outputs";
private static readonly string SchemaFile = @"C:\ZespriWorkspace\Scripts\schemaMapper.xlsx";
private static readonly string LogFolder = @"C:\ZespriWorkspace\logs";

private static readonly string ConnectionString =
    "Server=WIN-K01V82HRITO;Database=SpatialDB;Trusted_Connection=yes;TrustServerCertificate=true;";

private static readonly string SchemaName = "dbo";
```

## Schema Mapper Format

The Excel schema mapper file should contain one sheet per GeoJSON file (matched by filename without extension). Each sheet requires two columns:

| oldFieldName | newFieldName |
|--------------|--------------|
| original_col | renamed_col  |
| another_col  | new_name     |

## Usage

Run the application:

```bash
dotnet run
```

The processor will:

1. Scan the input folder for `.geojson` files
2. For each file:
   - Load and parse the GeoJSON
   - Create `puid` field from `blockid` (if present)
   - Add `Geometry_Type` based on each feature's geometry
   - Add `Geometry_Status` ("New", "Updated", or "Unknown") based on date comparison
   - Rename fields according to the schema mapper
   - Save the transformed GeoJSON to the output folder
   - Upload to SQL Server with a `GEOGRAPHY` column
3. Write all activity to a timestamped log file

## Output

- **Transformed GeoJSON files** — saved to the output folder
- **SQL Server tables** — one table per GeoJSON file in the specified schema, with a `geom` column of type `GEOGRAPHY`
- **Log files** — timestamped logs in the log folder (e.g., `CSharpProcessor_log_20250115_143022.txt`)

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| Microsoft.Data.SqlClient | 5.2.0 | SQL Server connectivity |
| NetTopologySuite | 2.5.0 | Geometry operations |
| NetTopologySuite.IO.GeoJSON | 4.0.0 | GeoJSON reading/writing |
| EPPlus | 7.0.0 | Excel file reading |

## EPPlus Licensing

The code uses `LicenseContext.NonCommercial` for EPPlus. For commercial use, you must either:

- Purchase an EPPlus commercial license
- Replace EPPlus with an alternative library such as `ClosedXML`

## Notes

- The application assumes input GeoJSON files use WGS84 (EPSG:4326) coordinates. If your data uses a different CRS, add coordinate transformation using `ProjNet` or `DotSpatial.Projections`.
- Tables are replaced (`DROP` + `CREATE`) on each run. Modify the `UploadToSqlServer` method if you need append or upsert behaviour.
- SQL column types are inferred from sample data. For stricter type control, define an explicit schema mapping.
