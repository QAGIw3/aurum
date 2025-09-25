#!/usr/bin/env python3
import logging

from aurum.api.app import create_app


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    try:
        logger.info("Creating Aurum API application...")
        app = create_app()
        import uvicorn
        logger.info("Starting server on 0.0.0.0:8080...")
        uvicorn.run(app, host="0.0.0.0", port=8080)
    except Exception as e:
        logger.error(f"Failed to start API: {e}")
        raise


if __name__ == "__main__":
    main()
