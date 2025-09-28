from aurum.airflow_utils.datasets import dataset_uri, iso_trigger, iso_ingest, noaa_trigger, noaa_ingest, URIS


def test_dataset_uri_builder():
    assert dataset_uri("ingest", "iso", "miso", "lmp") == "dataset://aurum/ingest/iso/miso/lmp"
    assert dataset_uri("/ingest/", "/iso/", "/isone/", "/load/") == "dataset://aurum/ingest/iso/isone/load"


def test_iso_helpers():
    assert iso_trigger("miso", "lmp_window_ready") == "dataset://aurum/triggers/iso/miso/lmp_window_ready"
    assert iso_ingest("isone", "generation_mix") == "dataset://aurum/ingest/iso/isone/generation_mix"


def test_noaa_helpers():
    assert noaa_trigger("ghcnd_daily_window_ready") == "dataset://aurum/triggers/noaa/ghcnd_daily_window_ready"
    assert noaa_ingest("ghcnd_daily") == "dataset://aurum/ingest/noaa/ghcnd_daily"


def test_uris_constants():
    assert URIS.TRIGGER_PJM_DA_WINDOW == "dataset://aurum/triggers/pjm_da_window_ready"
    assert URIS.INGEST_PJM_DA_LMP == "dataset://aurum/ingest/pjm_da_lmp"
    assert URIS.INGEST_ISO_LMP_RAW == "dataset://aurum/ingest/iso_lmp_raw"
    assert URIS.INGEST_ISO_LMP_TIMESCALE == "dataset://aurum/ingest/iso_lmp_timescale"
    assert URIS.INGEST_EIA_SERIES_RAW == "dataset://aurum/ingest/eia_series_raw"
    assert URIS.INGEST_EIA_SERIES_TIMESCALE == "dataset://aurum/ingest/eia_series_timescale"

