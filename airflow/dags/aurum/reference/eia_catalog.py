from __future__ import annotations


class EIADatasetStub:
    def __init__(self, default_frequency: str = "OTHER"):
        self.default_frequency = default_frequency


def get_dataset(path: str) -> EIADatasetStub:  # pragma: no cover - stub
    return EIADatasetStub()


