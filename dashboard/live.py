"""The "Live now" strip: every line's status right now, straight from the TfL API.

This is the only part of the dashboard that doesn't come from the lake. It calls TfL at
most once a minute (cached), and refreshes on its own without reloading the page.
"""

import json
from html import escape
from urllib.request import Request, urlopen

import streamlit as st

from charts import LINE_COLOURS, OTHER_LEVEL

STATUS_URL = (
    "https://api.tfl.gov.uk/Line/Mode/tube,dlr,elizabeth-line,overground/Status"
)
GOOD_SERVICE = 10
CLOSED_FOR_NIGHT = 20

# Tube lines first in TfL's usual order, then the rest alphabetically.
TUBE_ORDER = [
    "Bakerloo", "Central", "Circle", "District", "Hammersmith & City", "Jubilee",
    "Metropolitan", "Northern", "Piccadilly", "Victoria", "Waterloo & City",
]  # fmt: skip

STYLE = """
<style>
.live-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(170px, 1fr));
  gap: 8px;
}
.live-tile {
  border-left: 6px solid var(--line);
  border-radius: 4px;
  padding: 6px 10px;
  background: rgba(128, 128, 128, 0.08);
  font-size: 0.85rem;
  line-height: 1.3;
}
.live-tile .name { font-weight: 600; }
.live-tile .status { opacity: 0.75; }
.live-tile.bad .status { opacity: 1; font-weight: 600; }
</style>
"""


@st.cache_data(ttl=60, show_spinner=False)
def fetch_status() -> list[dict]:
    """One dict per line: name, statuses (list of text), worst severity (lower is worse)."""
    request = Request(STATUS_URL, headers={"User-Agent": "tfl-network-dashboard"})
    with urlopen(request, timeout=5) as response:
        lines = json.load(response)
    result = []
    for line in lines:
        statuses = line.get("lineStatuses", [])
        descriptions = list(
            dict.fromkeys(s["statusSeverityDescription"] for s in statuses)
        )
        result.append(
            {
                "name": line["name"],
                "statuses": descriptions or ["Unknown"],
                "severity": min((s["statusSeverity"] for s in statuses), default=0),
            }
        )
    order = {name: i for i, name in enumerate(TUBE_ORDER)}
    return sorted(result, key=lambda x: (order.get(x["name"], len(order)), x["name"]))


def _tile(line: dict) -> str:
    good = line["severity"] in (GOOD_SERVICE, CLOSED_FOR_NIGHT)
    colour = LINE_COLOURS.get(line["name"], OTHER_LEVEL[1])
    icon = "" if good else "⚠ "
    status = escape(", ".join(line["statuses"]))
    return (
        f'<div class="live-tile{"" if good else " bad"}" style="--line:{colour}">'
        f'<div class="name">{escape(line["name"])}</div>'
        f'<div class="status">{icon}{status}</div></div>'
    )


@st.fragment(run_every="60s")
def live_status() -> None:
    """Draw the strip. Re-runs every minute on its own."""
    st.subheader("Live now")
    try:
        lines = fetch_status()
    # TfL down, slow or sending something odd: the rest of the dashboard still works.
    except (OSError, ValueError, KeyError):
        st.caption(
            "Live status is unavailable right now. The data below is unaffected."
        )
        return
    problems = sum(
        line["severity"] not in (GOOD_SERVICE, CLOSED_FOR_NIGHT) for line in lines
    )
    summary = (
        "Good service on every line right now."
        if problems == 0
        else f"{problems} of {len(lines)} lines have problems right now."
    )
    st.caption(f"{summary} Straight from TfL, refreshed every minute.")
    st.html(STYLE + '<div class="live-grid">' + "".join(map(_tile, lines)) + "</div>")
