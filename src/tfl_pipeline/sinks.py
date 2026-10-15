"""Where the consumer writes files: a local folder now, S3 later."""

from pathlib import Path


class LocalSink:
    """A sink is a place where the consumer writes files."""

    def __init__(self, root: str):
        self.root = Path(root)

    def write(self, key: str, data: bytes) -> str:
        """Write content to a file in the sink."""
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path)
