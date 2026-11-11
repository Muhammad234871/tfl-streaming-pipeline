"""The "Method and caveats" section shown at the bottom of every page."""

import streamlit as st

METHOD = """
**Where the data comes from.** Every minute, a producer asks the TfL Unified API for
live arrival predictions and line status on the Tube, DLR, Elizabeth line and London
Overground. Each answer goes through Kafka to Amazon S3 (bronze). Every night at 01:00 UTC
a batch job cleans the previous day with PySpark (silver) and builds the tables shown
here (gold), which the dashboard reads through Athena.

**Train arrivals.** TfL publishes predictions, not a record of when trains actually
arrived. A train counts as arriving when it is predicted within 60 seconds of a station.
Repeated sightings of the same train at the same platform within 10 minutes count once.

**Minutes between trains (headway).** The time from one arrival to the next on the same
line and platform (so the same direction). Arrivals less than 60 seconds apart are treated
as one train, and gaps over 30 minutes as a gap in our data, so both are left out. A
line's figure is the median over all platform-hours with at least 5 trains.

**Disruption.** Each poll records every line's status, and each poll counts as one minute.
"Good Service" and "Closed for the night" aren't counted as disruption. The share
disrupted is the disrupted minutes divided by the minutes we observed that line.

**Collected hours.** The producer should make 60 polls an hour. An hour with at least 90%
of them counts as complete; anything less is marked as partly collected.
"""

CAVEATS = """
- **Predictions, not timetables or actual arrivals.** Counts are as good as TfL's
  predictions. A train that is cancelled after it was predicted close by may still count.
- **Times are UTC.** In winter that's London time; from late March to late October
  (British Summer Time) London is one hour ahead. Days are UTC days too, so the Friday
  night Night Tube falls under Saturday's date.
- **Gaps in collection undercount.** If the producer wasn't running, there are fewer
  arrivals, not fewer trains. Partly collected days and hours are shown lighter or grey;
  compare complete ones.
- **The feed has duplicates.** It sometimes lists several "trains" at one platform
  seconds apart. These are merged, but a real train less than 60 seconds behind another
  would be merged too (rare).
- **Disruption is line-wide.** A part closure on one section counts against the whole
  line. A line can have two statuses at once, so totals can pass 100%; the Trends page
  caps the share at 100%.
- **Station names follow TfL.** One place can appear under two names when it has both
  Tube and rail platforms, for example "Abbey Wood" and "Abbey Wood (London) Rail Station".
- **Not covered:** buses, trams, the cable car and National Rail services.
"""


def method_and_caveats() -> None:
    """Draw the section as two collapsed expanders."""
    st.divider()
    with st.expander("Method"):
        st.markdown(METHOD)
    with st.expander("Caveats"):
        st.markdown(CAVEATS)
