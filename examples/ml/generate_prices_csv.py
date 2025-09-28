from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import math
import random
from pathlib import Path


def main() -> None:
    random.seed(42)
    n = 300
    start = datetime.now(timezone.utc) - timedelta(hours=n)
    rows = [("timestamp", "price")]
    base = 50.0
    for i in range(n):
        ts = start + timedelta(hours=i)
        daily = 3.0 * math.sin(2 * math.pi * (i % 24) / 24.0)
        weekly = 2.0 * math.sin(2 * math.pi * (i % (24 * 7)) / (24 * 7))
        noise = random.gauss(0.0, 0.5)
        price = base + daily + weekly + noise
        # Inject a couple of anomalies
        if i in {220, 260}:
            price += 10.0 if i == 220 else -8.0
        rows.append((ts.isoformat(), round(price, 4)))

    out = Path(__file__).parents[1] / "data" / "prices_sample.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerows(rows)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()

