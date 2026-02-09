import json

class RecordCounter:
    """
    A line writer that counts RECORD messages per stream from Singer tap output.
    """

    def __init__(self):
        self.counts: dict[str, int] = {}

    def writelines(self, line: str) -> None:
        """
        Parse a Singer message line and count RECORD messages per stream.

        Args:
            line: A JSON string containing a Singer message.
        """
        try:
            message = json.loads(line)
            if message.get("type") == "RECORD":
                stream = message.get("stream")
                if stream:
                    self.counts[stream] = self.counts.get(stream, 0) + 1
        except json.JSONDecodeError:
            pass

    def reset(self) -> None:
        """Reset all counts to zero."""
        self.counts = {}

