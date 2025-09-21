import asyncio
import importlib.util
import pathlib
import sys
import types

import pandas as pd

_SRC_PATH = None
for parent in pathlib.Path(__file__).resolve().parents:
    if parent.name == "src":
        _SRC_PATH = parent
        break

if _SRC_PATH is None:
    raise RuntimeError("Unable to locate project src directory")

if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

if "aurum.api" not in sys.modules:
    api_pkg = types.ModuleType("aurum.api")
    api_pkg.__path__ = [str(_SRC_PATH / "aurum" / "api")]
    sys.modules["aurum.api"] = api_pkg


def _load_api_module(name: str):
    if name in sys.modules:
        return sys.modules[name]
    module_path = _SRC_PATH / "aurum" / "api" / f"{name.split('.')[-1]}.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module {name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_load_api_module("aurum.api.exceptions")

from aurum.parsers.async_parser import ParserResult, ParserValidator  # noqa: E402


def test_parser_validator_flags_missing_columns():
    validator = ParserValidator()
    df = pd.DataFrame({"mid": [10.0]})
    result = ParserResult(data=df, metadata={}, errors=[], warnings=[], processing_time=0.0)

    issues = asyncio.run(validator.validate(result))

    assert any("Missing required columns" in issue for issue in issues)


def test_parser_validator_detects_duplicates_and_negatives():
    validator = ParserValidator()
    df = pd.DataFrame(
        [
            {
                "asof_date": "2024-01-01",
                "iso": "PJM",
                "market": "DA",
                "location": "HUB",
                "mid": 10.0,
                "bid": 9.5,
                "ask": 10.5,
                "tenor_label": "Jan",
                "price_type": "MID",
                "curve_key": "CK1",
            },
            {
                "asof_date": "2024-01-01",
                "iso": "PJM",
                "market": "DA",
                "location": "HUB",
                "mid": -5.0,
                "bid": -5.5,
                "ask": -4.5,
                "tenor_label": "Jan",
                "price_type": "MID",
                "curve_key": "CK1",
            },
            {
                "asof_date": "2024-01-01",
                "iso": "PJM",
                "market": "DA",
                "location": "HUB",
                "mid": 10.0,
                "bid": 9.5,
                "ask": 10.5,
                "tenor_label": "Jan",
                "price_type": "MID",
                "curve_key": "CK1",
            },
        ]
    )

    result = ParserResult(data=df, metadata={}, errors=[], warnings=[], processing_time=0.0)
    issues = asyncio.run(validator.validate(result))

    assert any("duplicate" in issue.lower() for issue in issues)
    assert any("negative" in issue.lower() for issue in issues)
