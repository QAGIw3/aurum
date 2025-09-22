import asyncio

import schemathesis
from hypothesis import settings

from aurum.api.app import create_app

# Generate the ASGI application once per test session
_APP = asyncio.run(create_app())
SCHEMA = schemathesis.from_asgi("/openapi.json", _APP)


@SCHEMA.parametrize(method="GET")
@settings(max_examples=5, deadline=None)
def test_openapi_contract(case) -> None:
    """Fuzz basic GET endpoints to ensure schema conformance."""
    response = case.call_asgi()
    case.validate_response(response)
