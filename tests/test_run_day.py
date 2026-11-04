import re

from tfl_pipeline.jobs import run_day as rd


def fake_steps(monkeypatch, codes):
    """Replace run_step with a fake that records calls and returns codes[step] (default 0)."""
    calls = []

    def fake_run_step(step, lake, dt):
        calls.append(step)
        return codes.get(step, 0)

    monkeypatch.setattr(rd, "run_step", fake_run_step)
    return calls


def test_runs_all_steps_in_order(monkeypatch):
    calls = fake_steps(monkeypatch, {})
    assert rd.run_day("lake", "2026-11-03") == 0
    assert calls == rd.STEPS


def test_skips_when_no_bronze(monkeypatch):
    calls = fake_steps(monkeypatch, {"bronze_exists": rd.NO_DATA_EXIT_CODE})
    assert rd.run_day("lake", "2026-10-16") == rd.NO_DATA_EXIT_CODE
    assert calls == ["bronze_exists"]


def test_stops_at_first_failure(monkeypatch):
    calls = fake_steps(monkeypatch, {"silver.line_status": 1})
    assert rd.run_day("lake", "2026-11-03") == 1
    assert calls == ["bronze_exists", "silver.arrivals", "silver.line_status"]


def test_silver_before_gold_and_quality_last():
    first_gold = min(i for i, s in enumerate(rd.STEPS) if s.startswith("gold."))
    last_silver = max(i for i, s in enumerate(rd.STEPS) if s.startswith("silver."))
    assert last_silver < first_gold
    assert rd.STEPS[-1] == "quality"


def test_yesterday_is_a_date():
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", rd.yesterday())
