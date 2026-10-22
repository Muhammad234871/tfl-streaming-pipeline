import json

from pyspark.sql import functions as F

from tfl_pipeline.jobs.silver.arrivals import transform


def bronze_row(
    prediction_id,
    naptan_id="940GZZLUOXC",
    vehicle_id="214",
    timestamp="2026-10-19T10:00:00Z",
    expected="2026-10-19T10:02:00Z",
):
    """One bronze arrivals message, as the consumer writes it."""
    return json.dumps(
        {
            "poll_id": "p1",
            "ingested_at": "2026-10-19T10:00:05Z",
            "source": "arrivals/tube",
            "data": {
                "id": prediction_id,
                "lineId": "victoria",
                "lineName": "Victoria",
                "modeName": "tube",
                "naptanId": naptan_id,
                "stationName": "Oxford Circus",
                "platformName": "Northbound - Platform 6",
                "direction": "inbound",
                "destinationNaptanId": "940GZZLUWWL",
                "destinationName": "Walthamstow Central",
                "towards": "Walthamstow Central",
                "currentLocation": "At Warren Street",
                "vehicleId": vehicle_id,
                "timestamp": timestamp,
                "expectedArrival": expected,
            },
        }
    )


def to_bronze_df(spark, rows):
    """Read JSON strings like the real bronze files, plus the dt partition column."""
    df = spark.read.json(spark.sparkContext.parallelize(rows))

    return df.withColumn("dt", F.lit("2026-10-19").cast("date"))


def test_time_to_station_is_derived_from_the_two_timestamps(spark):
    raw = to_bronze_df(spark, [bronze_row("a")])

    row = transform(raw).first()

    assert row.time_to_station_s == 120


def test_placeholder_vehicle_ids_become_null_but_rows_are_kept(spark):
    raw = to_bronze_df(
        spark,
        [
            bronze_row("a", vehicle_id="214"),
            bronze_row("b", vehicle_id="000"),
            bronze_row("c", vehicle_id=""),
        ],
    )

    rows = {r.prediction_id: r.vehicle_id for r in transform(raw).collect()}

    assert rows == {"a": "214", "b": None, "c": None}


def test_rows_without_a_station_are_dropped(spark):
    raw = to_bronze_df(spark, [bronze_row("a"), bronze_row("b", naptan_id=None)])

    assert transform(raw).count() == 1


def test_exact_duplicates_are_removed_but_same_id_different_train_is_kept(spark):
    raw = to_bronze_df(
        spark,
        [
            bronze_row("a"),
            bronze_row("a"),
            bronze_row("a", expected="2026-10-19T10:06:00Z"),
        ],
    )

    assert transform(raw).count() == 2
