from tfl_pipeline.jobs.gold.arrival_events import arrival_events


def test_same_train_seen_arriving_in_two_polls_counts_once(silver_arrivals):
    df = silver_arrivals(
        [
            ("214", "10:00:00", 60),  # poll 1: 60 s away
            ("214", "10:00:30", 0),  # poll 2: at platform (same arrival)
            (
                "215",
                "10:05:00",
                300,
            ),  # another train, still 5 min away: not arriving yet
        ]
    )

    assert arrival_events(df).count() == 1


def test_trains_without_vehicle_id_are_still_counted(silver_arrivals):
    df = silver_arrivals(
        [
            (None, "10:00:00", 0),  # DLR-style: no vehicle id
            (None, "10:03:00", 0),  # a different DLR train 3 minutes later
        ]
    )

    assert arrival_events(df).count() == 2
