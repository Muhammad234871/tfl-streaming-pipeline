resource "aws_athena_workgroup" "tfl" {
  name          = "tfl"
  description   = "Queries on the TfL data lake"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 1073741824

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name        = "tfl_lake"
  description = "TfL data lake: silver and gold tables (Parquet on S3)"
}

locals {
  lake_tables = {
    gold_line_status_daily = {
      path = "gold/line_status_daily"
      columns = {
        line_id            = "string"
        line_name          = "string"
        mode               = "string"
        status_severity    = "int"
        status_description = "string"
        status_minutes     = "bigint"
        observed_minutes   = "bigint"
        pct_of_observed    = "double"
      }
    }
    gold_station_hourly = {
      path = "gold/station_hourly"
      columns = {
        hour           = "int"
        naptan_id      = "string"
        station_name   = "string"
        train_arrivals = "bigint"
        line_count     = "bigint"
        lines          = "array<string>"
      }
    }
    gold_line_headways = {
      path = "gold/line_headways"
      columns = {
        hour               = "int"
        line_id            = "string"
        naptan_id          = "string"
        station_name       = "string"
        platform_name      = "string"
        trains             = "bigint"
        avg_headway_min    = "double"
        median_headway_min = "double"
        max_headway_min    = "double"
      }
    }
    silver_arrivals = {
      path = "silver/arrivals"
      columns = {
        poll_id               = "string"
        ingested_at           = "timestamp"
        source                = "string"
        mode                  = "string"
        line_id               = "string"
        line_name             = "string"
        naptan_id             = "string"
        station_name          = "string"
        platform_name         = "string"
        direction             = "string"
        destination_naptan_id = "string"
        destination_name      = "string"
        towards               = "string"
        current_location      = "string"
        vehicle_id            = "string"
        prediction_id         = "string"
        prediction_ts         = "timestamp"
        expected_arrival      = "timestamp"
        time_to_station_s     = "int"
      }
    }
    silver_line_status = {
      path = "silver/line_status"
      columns = {
        poll_id             = "string"
        ingested_at         = "timestamp"
        line_id             = "string"
        line_name           = "string"
        mode                = "string"
        status_severity     = "int"
        status_description  = "string"
        reason              = "string"
        disruption_category = "string"
      }
    }
  }
}

resource "aws_glue_catalog_table" "lake" {
  for_each = local.lake_tables

  name          = each.key
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                      = "TRUE"
    classification                = "parquet"
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.range"         = "2026-10-01,NOW"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "storage.location.template"   = "s3://${aws_s3_bucket.lake.bucket}/${each.value.path}/dt=$${dt}"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/${each.value.path}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}


moved {
  from = aws_glue_catalog_table.gold_line_status_daily
  to   = aws_glue_catalog_table.lake["gold_line_status_daily"]
}