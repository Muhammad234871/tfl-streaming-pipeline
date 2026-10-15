from pathlib import Path

from tfl_pipeline.sinks import LocalSink


def test_local_sink_writes_file_and_creates_folders(tmp_path):
    sink = LocalSink(str(tmp_path))

    path = sink.write("bronze/arrivals/dt=2026-10-15/hour=11/part-1.jsonl.gz", b"hello")

    assert Path(path).read_bytes() == b"hello"
    assert path.endswith("bronze/arrivals/dt=2026-10-15/hour=11/part-1.jsonl.gz")
