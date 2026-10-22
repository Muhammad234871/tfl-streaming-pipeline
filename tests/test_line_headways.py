from tfl_pipeline.jobs.gold.line_headways import transform


def test_headways_are_gaps_between_trains_and_long_gaps_are_dropped(silver_arrivals):
    df = silver_arrivals(
        [
            ("1", "10:00:00", 0),
            ("2", "10:03:00", 0),  # 3 min after train 1
            ("3", "10:07:00", 0),  # 4 min after train 2
            ("4", "10:47:00", 0),  # 40 min gap: a collection gap, dropped
        ]
    )

    rows = transform(df).collect()

    assert len(rows) == 1
    row = rows[0]

    assert row.trains == 2
    assert row.median_headway_min == 3.0
    assert row.max_headway_min == 4.0
    assert row.avg_headway_min == 3.5
