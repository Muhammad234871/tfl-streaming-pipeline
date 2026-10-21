"""Turn arrival predictions into arrival events, shared by the arrival gold tables.

A train counts as arriving when it is predicted within ARRIVING_WITHIN_S of the station,
and repeated sightings of the same train at the same platform within SAME_ARRIVAL_GAP_S
count once.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ARRIVING_WITHIN_S = 60  # a prediction this close counts as "arriving now"
SAME_ARRIVAL_GAP_S = 600  # sightings closer than this are the same arrival


def arrival_events(silver: DataFrame) -> DataFrame:
    """One row per train arrival at a station platform."""
    near = silver.where(F.col("time_to_station_s") <= ARRIVING_WITHIN_S)

    with_key = near.withColumn(
        "train_key",
        F.coalesce(
            F.col("vehicle_id"),
            F.concat_ws("|", "destination_naptan_id", "prediction_id"),
        ),
    )
    w = Window.partitionBy(
        "line_id", "naptan_id", "platform_name", "train_key"
    ).orderBy("expected_arrival")
    # Window functions can't be used in a filter directly: store the result as a column first.
    with_prev = with_key.withColumn("prev_arrival", F.lag("expected_arrival").over(w))
    gap_s = F.col("expected_arrival").cast("long") - F.col("prev_arrival").cast("long")

    return with_prev.where(
        F.col("prev_arrival").isNull() | (gap_s > SAME_ARRIVAL_GAP_S)
    ).drop("prev_arrival")
