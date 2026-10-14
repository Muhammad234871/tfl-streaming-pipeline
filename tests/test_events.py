from datetime import datetime, timezone

from tfl_pipeline.events import to_iso
from tfl_pipeline.events import lake_partition



def test_to_iso_formats_utc_datetime():
    ts = datetime(2026, 10, 14, 15, 11, 54, tzinfo=timezone.utc)
    assert to_iso(ts) == "2026-10-14T15:11:54Z"
    
    

def test_lake_partition_uses_date_and_hour():
    assert lake_partition("2026-10-14T00:00:00Z") == "dt=2026-10-14/hour=00"
    assert lake_partition("2026-10-14T15:11:54Z") == "dt=2026-10-14/hour=15"