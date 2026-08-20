# nectar_dataengineering_project
Developed an end-to-end Microsoft Fabric solution using Medallion architecture, with metadata-driven Bronze ingestion, incremental Silver processing, DQ validation, Gold star schema, and Power BI analytics for energy and asset performance.
# NECTAR Energy & Asset Performance Analytics

## Project Overview

NECTAR Energy & Asset Performance Analytics is an end-to-end Microsoft
Fabric and Power BI data engineering project for ingesting telemetry,
operational events, and asset metadata and converting them into trusted
analytical datasets.

**Architecture:** Source CSVs → Bronze → Silver → Data Quality → Gold →
Power BI

## Technology Stack

-   Microsoft Fabric
-   Fabric Lakehouse / OneLake
-   Fabric Data Pipelines
-   PySpark / Fabric Notebooks
-   Parquet and Delta tables
-   Power BI
-   SQL
-   DAX

## Source Data

The project uses three source CSV datasets:

### telemetry.csv

Contains timestamp, site, building, asset, sensor, temperature,
humidity, pressure, vibration, power consumption, and operating mode
data.

### events.csv

Contains event ID, timestamp, asset ID, event type, severity, and
message data.

### asset_metadata.csv

Contains asset ID, asset name, asset type, manufacturer, installation
date, and site information.

## Repository Structure

``` text
nectar_project/
├── 1.bronze_pipeline/
├── 2.notebook_silver_gold/
├── 3.dq_validation/
├── 4.network_model/
├── 5.master_pipeline_failure_reason/
├── 6.architecture/
├── 7.gold_data/
├── 8.powerbi/
├── 9.sql_challenge/
├── 10_dax/
├── 11_presentation/
└── docs/
```

  ------------------------------------------------------------------------
  Folder                               Purpose
  ------------------------------------ -----------------------------------
  `1.bronze_pipeline`                  Bronze ingestion pipeline and
                                       implementation artifacts

  `2.notebook_silver_gold`             Silver cleansing and Gold
                                       transformation notebooks

  `3.dq_validation`                    Data-quality validation and
                                       quarantine logic

  `4.network_model`                    Graph model(Multi-Asset Hierarchy & Connectivity )                                       

  `5.master_pipeline_failure_reason`   Master orchestration and
                                       failure-handling documentation

  `6.architecture`                     High-level architecture and
                                       data-model and schema definations diagrams

  `7.gold_data`                        Gold-layer datasets 
                                       

  `8.powerbi`                          Power BI reports and analytics

  `9.sql_challenge`                    SQL queries and analytical
                                       exercises

  `10_dax`                             DAX measures and calculations

  `docs`                               Project documentation
  ------------------------------------------------------------------------

# 1. Bronze Layer

The Bronze layer is the raw ingestion layer. Source data is preserved
with no business transformation. The pipeline is metadata-driven and
processes the available source files dynamically.

### Source location

``` text
Files/source/
├── telemetry.csv
├── events.csv
└── asset_metadata.csv
```

### Bronze structure

``` text
Files/bronze/
├── telemetry/YYYY-MM-DD/raw_telemetry.parquet
├── events/YYYY-MM-DD/raw_events.parquet
└── asset_metadata/YYYY-MM-DD/raw_asset_metadata.parquet
```

### Bronze pipeline flow

``` text
Get Metadata
    ↓
ForEach Source File
    ↓
Validate Source File
    ↓
Delete Existing Bronze File
    ↓
Copy Bronze File
```

The pipeline accepts the source location through the `source_path`
parameter instead of hardcoding the source path.

Two audit columns are retained/added:

-   `_source_file` --- original CSV filename
-   `_ingested_at` --- UTC ingestion timestamp

### Re-run behaviour

For the same load date, the existing Bronze output is deleted before the
new Parquet file is written. This prevents duplicate Bronze files for
the same load date.

The dynamic folder expression is:

``` text
@concat(
    'bronze/',
    replace(item().name,'.csv',''),
    '/',
    formatDateTime(utcNow(),'yyyy-MM-dd')
)
```

The dynamic file name is:

``` text
@concat(
    'raw_',
    replace(item().name,'.csv',''),
    '.parquet'
)
```

# 2. Silver Layer

Silver is the cleaned and standardized layer.

The main tables are:

``` text
silver_telemetry
silver_events
silver_asset_metadata
```

Typical transformations include:

-   Timestamp standardization
-   Type conversion
-   String trimming
-   Blank-to-null handling
-   Numeric conversion
-   Default value handling
-   Operating-mode standardization
-   Duplicate handling
-   Audit-column preservation

## Dynamic Bronze Reading

Silver dynamically identifies the relevant Bronze load-date folder
rather than permanently hardcoding a date.

``` text
Files/bronze/telemetry/<load_date>/raw_telemetry.parquet
Files/bronze/events/<load_date>/raw_events.parquet
Files/bronze/asset_metadata/<load_date>/raw_asset_metadata.parquet
```

## Watermark-Based Incremental Loading

Silver uses:

``` text
silver_watermark
```

The table maintains the latest successfully processed `_ingested_at`
timestamp for each Silver dataset.

Example:

``` text
table_name       last_ingested_at
--------------------------------------------
telemetry        <latest successful timestamp>
events           <latest successful timestamp>
asset_metadata   <latest successful timestamp>
```

Incremental flow:

``` text
Read Bronze
    ↓
Read silver_watermark
    ↓
Filter _ingested_at > last_ingested_at
    ↓
Transform new records
    ↓
Append to Silver
    ↓
Get MAX(_ingested_at)
    ↓
Update watermark
```

If new records arrive, only the new records are appended. Previously
processed records are not unnecessarily reloaded.

The watermark is advanced after successful Silver processing.

## Telemetry transformations

The telemetry Silver logic preserves the original timestamp as
`raw_timestamp` and standardizes supported timestamp formats.

Supported formats include:

``` text
dd-MM-yyyy HH:mm
dd-MM-yyyy HH:mm:ss
yyyy-MM-dd HH:mm:ss
yyyy-MM-dd HH:mm:ss.SSS
```

Identifier fields are trimmed and blank values become NULL.

Measurements are converted to DOUBLE:

``` text
temperature
humidity
pressure
vibration
power_consumption
```

Missing `power_consumption` is defaulted to `0.0`.

`operating_mode` is trimmed and upper-cased, with missing values
represented as `UNKNOWN`.

Bronze audit fields are mapped to Silver as:

``` text
_source_file → source_file
_ingested_at → ingestion_timestamp
```

# 3. Data Quality Validation

The DQ layer validates Silver data before downstream analytical
processing.

The implemented validation area includes checks such as:

-   Missing values
-   Duplicate records
-   Invalid timestamps
-   Invalid asset IDs
-   Schema violations
-   Outliers
-   Late-arriving data
-   Referential integrity

Invalid records can be separated into quarantine datasets so that bad
records do not silently enter the analytical Gold layer.

``` text
Silver
  ↓
DQ Validation
  ├── Valid Records → Gold
  └── Invalid Records → Quarantine
```

Late-arriving data is treated as a data-quality/monitoring condition
according to the implemented project rules; late arrival alone does not
automatically mean the record is structurally invalid.

# 4. Gold Layer

Gold is the curated business and analytical layer.

The model follows a dimensional/star-schema approach.

## Dimensions

``` text
gold_dim_site
gold_dim_building
gold_dim_asset
gold_dim_date
```

## Facts

``` text
gold_fact_telemetry
gold_fact_energy
gold_fact_event
```

## Gold dimensions

### gold_dim_site

Site-level master data keyed by `site_id`.

### gold_dim_building

Building information with the site relationship through `site_id`.

### gold_dim_asset

Asset master information including:

-   asset_id
-   asset_name
-   asset_type
-   installation_date
-   manufacturer
-   site_id

### gold_dim_date

Reusable date dimension containing:

-   date_key
-   day
-   day_of_week
-   day_of_year
-   month
-   week_of_year
-   year

## Gold facts

### gold_fact_telemetry

Contains telemetry measurements and analytical keys such as asset,
building, site, date, sensor, temperature, humidity, pressure,
vibration, power consumption, operating mode, timestamp, and audit
information.

### gold_fact_energy

Provides aggregated energy analytics including:

-   asset_id
-   building_id
-   site_id
-   date_key
-   hour
-   avg_power_consumption
-   hourly_energy_consumption
-   reading_count
-   energy_key

### gold_fact_event

Contains event analytics including:

-   asset_id
-   date_key
-   event_id
-   event_key
-   event_type
-   severity
-   message
-   timestamp
-   ingestion_timestamp
-   source_file





# 5. Analytics and Business Use Cases

The Gold layer supports:

-   Hourly energy consumption
-   Average power consumption
-   Site-level energy analysis
-   Building-level energy analysis
-   Asset-level energy analysis
-   Temperature and humidity analysis
-   Pressure and vibration monitoring
-   Operational event analysis
-   Fault/severity analysis
-   Asset performance monitoring
-   Historical trend analysis

# 7. Power BI

Power BI is the reporting and analytics layer.

Power BI consumes curated Gold data rather than raw Bronze data.

The model supports slicing and analysis across:

``` text
Site → Building → Asset → Sensor
```

and across time using `gold_dim_date`.

The reporting layer is intended for operational dashboards, historical
reporting, energy analytics, asset performance, and event/fault
analysis.



# 8. Production-Oriented Design

The project applies these production principles:

-   Parameterized source paths
-   Metadata-driven file processing
-   Dynamic Bronze-to-Silver reading
-   Audit columns for traceability
-   Deterministic Bronze re-runs
-   Watermark-based Silver incremental processing
-   Separate Bronze, Silver, DQ and Gold responsibilities
-   Data-quality validation before analytical consumption
-   Quarantine handling for invalid records
-   Dimensional/star-schema Gold modeling
-   Power BI consumption from curated Gold data

# 9. Assumptions

1.  The source system provides `telemetry.csv`, `events.csv`, and
    `asset_metadata.csv`.
2.  Source files are available under the configured `source_path`.
3.  Business identifiers used by the project are stable for the
    assignment.
4.  Bronze preserves source business data and adds the required
    ingestion audit information.
5.  The Bronze pipeline can be safely re-run for the same load date
    without accumulating duplicate Bronze files.
6.  `_ingested_at` is the watermark used for Silver incremental
    processing.
7.  Records with `_ingested_at` greater than the stored watermark are
    treated as new records for the incremental load.
8.  The watermark is updated after successful Silver processing.
9.  Supported timestamp formats are explicitly defined in the Silver
    transformation.
10. `installation_date` is treated as a DATE attribute in the asset
    model.
11. Power BI is the reporting layer.
12. Gold metrics are derived from curated and validated records
    according to the implemented project logic.

# 10. End-to-End Example

A telemetry record follows this path:

``` text
telemetry.csv
      ↓
Files/source/telemetry.csv
      ↓
Metadata-driven Bronze Pipeline
      ↓
Files/bronze/telemetry/2026-08-20/raw_telemetry.parquet
      ↓
Dynamic Bronze Reader
      ↓
Watermark Check
      ↓
New Records Only
      ↓
Silver Transformation
      ↓
silver_telemetry
      ↓
DQ Validation
      ↓
Valid Records
      ↓
gold_fact_telemetry
      ↓
gold_fact_energy
      ↓
Power BI
```

For a subsequent load:

``` text
New Bronze records
      ↓
_ingested_at > last_ingested_at
      ↓
Only new records selected
      ↓
Append to Silver
      ↓
Update watermark
      ↓
Gold processing
      ↓
Power BI
```

# 11. Project Outcome

The final solution provides an end-to-end Microsoft Fabric data
engineering architecture combining:

**Reliable ingestion + raw data preservation + auditability +
incremental processing + data quality + dimensional modeling + business
analytics**

The result is a structured analytical platform for energy, telemetry,
asset, and operational-event analysis.

