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


resource "aws_glue_catalog_table" "gold_line_status_daily" {
  name          = "gold_line_status_daily"
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
    "storage.location.template"   = "s3://${aws_s3_bucket.lake.bucket}/gold/line_status_daily/dt=$${dt}"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/gold/line_status_daily/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "line_id"
      type = "string"
    }
    columns {
      name = "line_name"
      type = "string"
    }
    columns {
      name = "mode"
      type = "string"
    }
    columns {
      name = "status_severity"
      type = "int"
    }
    columns {
      name = "status_description"
      type = "string"
    }
    columns {
      name = "status_minutes"
      type = "bigint"
    }
    columns {
      name = "observed_minutes"
      type = "bigint"
    }
    columns {
      name = "pct_of_observed"
      type = "double"
    }
  }
}