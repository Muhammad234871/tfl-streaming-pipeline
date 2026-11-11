"""Build stations.csv: the latitude and longitude of every station, for the map.

Stations hardly ever move, so this runs by hand once (and again when a station opens),
not in the pipeline:
    python dashboard/build_stations.py
"""

import csv
import json
from pathlib import Path
from urllib.request import Request, urlopen

URL = "https://api.tfl.gov.uk/StopPoint/Mode/tube,dlr,elizabeth-line,overground"
# Whole stations, not their entrances or platforms.
STATION_TYPES = {"NaptanMetroStation", "NaptanRailStation"}
OUT = Path(__file__).with_name("stations.csv")


def main() -> None:
    request = Request(URL, headers={"User-Agent": "tfl-network-dashboard"})
    with urlopen(request, timeout=60) as response:
        stop_points = json.load(response)["stopPoints"]
    rows = sorted(
        (s["naptanId"], s["commonName"], round(s["lat"], 6), round(s["lon"], 6))
        for s in stop_points
        if s["stopType"] in STATION_TYPES
    )
    with OUT.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["naptan_id", "name", "lat", "lon"])
        writer.writerows(rows)
    print(f"{len(rows)} stations written to {OUT}")


if __name__ == "__main__":
    main()
