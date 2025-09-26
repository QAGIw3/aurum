import asyncio

import os

import pytest
import schemathesis
from hypothesis import settings

from aurum.api.app import create_app

# Running the full Schemathesis suite requires the entire service stack (Trino,
# Redis, Schema Registry) to be available.  In the unit-test environment we
# disable it by default to avoid hundreds of failing counter examples.
if os.getenv("AURUM_RUN_CONTRACT_TESTS", "0") not in {"1", "true", "TRUE"}:
    pytest.skip("Schemathesis-powered contract tests require the integration stack", allow_module_level=True)

# Generate the ASGI application once per test session
_APP = create_app()
SCHEMA = schemathesis.openapi.from_asgi("/openapi.json", _APP)

# Filter for GET operations only
GET_SCHEMA = SCHEMA.include(method="GET")

@GET_SCHEMA.parametrize()
@settings(max_examples=5, deadline=None)
def test_openapi_contract(case) -> None:
    """Fuzz basic GET endpoints to ensure schema conformance."""
    # Schemathesis 4.x removed the dedicated ``call_asgi`` helper in favour of
    # a unified ``call`` method that honours the transport bound to the schema.
    # Older versions still expose ``call_asgi`` so keep a compatibility bridge
    # for developers that haven't upgraded yet.
    call = getattr(case, "call_asgi", None)
    if call is None:
        response = case.call()
    else:  # pragma: no cover - exercised under legacy Schemathesis
        response = call()
    case.validate_response(response)
