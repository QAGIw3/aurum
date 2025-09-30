import json
from pathlib import Path


def read_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def main() -> None:
    reports = Path("reports")
    outputs = {
        "pip_audit": read_json(reports / "pip-audit.json"),
        "sbom_present": (reports / "sbom.json").exists(),
        "deadfixtures": (reports / "deadfixtures.txt").exists(),
        "vulture": (reports / "vulture.txt").exists(),
        "coverage_xml": Path("coverage.xml").exists(),
    }
    print(json.dumps(outputs, indent=2))


if __name__ == "__main__":
    main()


