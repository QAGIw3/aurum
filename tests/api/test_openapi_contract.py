import asyncio

import schemathesis
from hypothesis import settings

from aurum.api.app import create_app

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
