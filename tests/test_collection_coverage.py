from datetime import datetime

from tfl_pipeline.jobs.gold.collection_coverage import transform


def test_counts_polls_and_includes_empty_hours(spark):
    rows = [
        # hour 7: 3 different polls; poll "a" has two rows, as a real poll has thousands
        ("a", datetime(2026, 11, 4, 7, 0, 5)),
        ("a", datetime(2026, 11, 4, 7, 0, 5)),
        ("b", datetime(2026, 11, 4, 7, 1, 5)),
        ("c", datetime(2026, 11, 4, 7, 2, 5)),
        # hour 9: 1 poll
        ("d", datetime(2026, 11, 4, 9, 30, 0)),
    ]
    silver = spark.createDataFrame(rows, "poll_id string, ingested_at timestamp")

    result = {r["hour"]: r for r in transform(silver, "2026-11-04").collect()}

    assert sorted(result) == list(range(24))
    assert (result[7]["polls"], result[7]["coverage_pct"]) == (3, 5.0)
    assert (result[8]["polls"], result[8]["coverage_pct"]) == (0, 0.0)
    assert result[9]["polls"] == 1
    assert {r["dt"] for r in result.values()} == {"2026-11-04"}


def test_coverage_is_capped_at_100(spark):
    # 61 polls in one hour (a slow poll can push one more into the hour)
    rows = [(f"p{i}", datetime(2026, 11, 4, 10, 0, 0)) for i in range(61)]
    silver = spark.createDataFrame(rows, "poll_id string, ingested_at timestamp")

    hour_10 = transform(silver, "2026-11-04").where("hour = 10").first()
    assert (hour_10["polls"], hour_10["coverage_pct"]) == (61, 100.0)
