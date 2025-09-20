from __future__ import annotations

import pytest

from aurum.reference.eia_catalog import DatasetNotFoundError, get_dataset, iter_datasets


def test_catalog_iterates_datasets() -> None:
    datasets = list(iter_datasets())
    assert datasets, "Expected at least one dataset in the catalog"
    assert any(dataset.path == "natural-gas/stor/wkly" for dataset in datasets)


def test_get_dataset_returns_metadata() -> None:
    dataset = get_dataset("natural-gas/stor/wkly")
    assert dataset.default_frequency == "weekly"
    assert "duoarea" in {facet["id"] for facet in dataset.facets}


def test_get_dataset_raises_for_unknown_path() -> None:
    with pytest.raises(DatasetNotFoundError):
        get_dataset("not-a-real/dataset")
