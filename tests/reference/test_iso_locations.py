from aurum.reference.iso_locations import get_location, iter_locations


def test_get_location_found():
    loc = get_location("PJM", "AECO")
    assert loc is not None
    assert loc.location_name == "AECO Zone"
    assert loc.location_type == "ZONE"
    assert loc.zone == "AECO"
    assert loc.hub == "AECO"
    assert loc.timezone == "America/New_York"


def test_get_location_missing_returns_none():
    assert get_location("PJM", "UNKNOWN") is None


def test_iter_locations_filters_iso():
    results = list(iter_locations("CAISO"))
    assert results
    assert all(loc.iso.upper() == "CAISO" for loc in results)
    assert any(loc.hub == "SP15" for loc in results)
