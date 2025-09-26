#!/usr/bin/env python3
"""Compatibility launcher for the Aurum API.

Defers to the canonical console entry point under `aurum.api.__main__`.
"""

from aurum.api.__main__ import main


if __name__ == "__main__":
    main()
