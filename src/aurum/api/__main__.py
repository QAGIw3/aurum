from __future__ import annotations

import logging
import os

from aurum.api.app import create_app


def main() -> None:
    logging.basicConfig(level=os.getenv("AURUM_LOG_LEVEL", "INFO"))
    logger = logging.getLogger(__name__)
    logger.info("Creating Aurum API application...")
    app = create_app()
    import uvicorn

    host = os.getenv("AURUM_API_HOST", "0.0.0.0")
    port = int(os.getenv("AURUM_API_PORT", "8080"))
    logger.info("Starting server on %s:%s...", host, port)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()

