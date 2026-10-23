"""Synthetic sell-out data with the problems real retail feeds have.

Brands, stores and products are invented. The generator is deterministic
(seeded) and writes plain files, so it works the same on a laptop and on a
Unity Catalog volume (/Volumes/<catalog>/<schema>/<volume>/...).

Problems injected on purpose, each one handled somewhere in the pipeline:
  * codes with stray spaces and lower case          -> silver standardization
  * lines re-sent in a second file                  -> silver dedup / MERGE
  * null SKU, negative price, bad timestamp,
    unknown store, unknown channel                  -> silver quarantine
  * sales arriving two days late                    -> gold lookback window
  * price and category changes                      -> dim_product SCD2
  * a store changing format                         -> dim_store SCD1
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

BRANDS = ["Vale Cacau", "Doce Serra", "Crocante", "Brisa", "Nuvem"]
CATALOG = {
    "Chocolates": ["Tabletes", "Bombons", "Wafers"],
    "Biscoitos": ["Recheados", "Salgados", "Integrais"],
    "Balas": ["Mastigaveis", "Gomas", "Refrescantes"],
    "Bebidas Em Po": ["Refrescos", "Achocolatados"],
}
STORES = [
    ("LJ001", "Mercado Central Pinheiros", "Sao Paulo", "SP", "Sudeste", "supermercado"),
    ("LJ002", "Mercado Central Moema", "Sao Paulo", "SP", "Sudeste", "supermercado"),
    ("LJ003", "Atacado Bom Preco Osasco", "Osasco", "SP", "Sudeste", "atacarejo"),
    ("LJ004", "Mini Box Campinas", "Campinas", "SP", "Sudeste", "proximidade"),
    ("LJ005", "Mercado Central Copacabana", "Rio de Janeiro", "RJ", "Sudeste", "supermercado"),
    ("LJ006", "Atacado Bom Preco Contagem", "Contagem", "MG", "Sudeste", "atacarejo"),
    ("LJ007", "Mercado Central Batel", "Curitiba", "PR", "Sul", "supermercado"),
    ("LJ008", "Mini Box Porto Alegre", "Porto Alegre", "RS", "Sul", "proximidade"),
    ("LJ009", "Atacado Bom Preco Recife", "Recife", "PE", "Nordeste", "atacarejo"),
    ("LJ010", "Mercado Central Salvador", "Salvador", "BA", "Nordeste", "supermercado"),
    ("LJ011", "Mini Box Goiania", "Goiania", "GO", "Centro-Oeste", "proximidade"),
    ("LJ012", "Mercado Central Brasilia", "Brasilia", "DF", "Centro-Oeste", "supermercado"),
]
CHANNELS = ["store"] * 8 + ["ecommerce", "delivery_app"]
TS = "%Y-%m-%d %H:%M:%S"


def _products(rng: random.Random) -> list[dict]:
    out, n = [], 1001
    for category, subs in CATALOG.items():
        for sub in subs:
            for _ in range(3):
                brand = rng.choice(BRANDS)
                size = rng.choice([30, 45, 90, 120, 200, 400])
                out.append(
                    {
                        "sku": f"SKU-{n}",
                        "product_name": f"{brand} {sub[:-1] if sub.endswith('s') else sub} {size}g",
                        "brand": brand,
                        "category": category,
                        "subcategory": sub,
                        "pack_size_g": str(size),
                        "list_price": f"{rng.uniform(2.5, 25):.2f}",
                    }
                )
                n += 1
    return out


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _sales_lines(rng: random.Random, day: date, products: list[dict], n_tx: int, tx_prefix: str) -> list[list]:
    lines = []
    for i in range(n_tx):
        store = rng.choice(STORES)[0]
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(8 * 3600, 22 * 3600))
        channel = rng.choice(CHANNELS)
        for ln, p in enumerate(rng.sample(products, rng.randint(1, 4)), start=1):
            qty = rng.choice([1, 1, 1, 2, 2, 3, 6, 12])
            price = float(p["list_price"])
            discount = round(qty * price * rng.choice([0, 0, 0, 0.05, 0.10]), 2)
            lines.append(
                [
                    f"{tx_prefix}{i:05d}",
                    ln,
                    ts.strftime(TS),
                    store,
                    p["sku"],
                    qty,
                    f"{price:.2f}",
                    f"{discount:.2f}",
                    channel,
                ]
            )
    return lines


def _corrupt(rng: random.Random, lines: list[list]) -> None:
    """Mutate a few lines in place, the way upstream systems actually break."""
    for line in rng.sample(lines, max(1, len(lines) // 40)):
        line[4] = f"  {line[4].lower()} "  # fixable: standardization
    bad = rng.sample(lines, 6)
    bad[0][4] = ""  # null sku
    bad[1][6] = "-3.90"  # negative price
    bad[2][2] = "2026/13/45 25:00"  # unparseable timestamp
    bad[3][3] = "LJ999"  # store not in master data
    bad[4][8] = "telefone"  # channel not supported
    bad[5][5] = "0"  # zero quantity


def generate(root: str | Path, end_date: date, days: int = 7, tx_per_day: int = 300, seed: int = 42) -> dict[str, int]:
    rng = random.Random(seed)
    root = Path(root)
    start = end_date - timedelta(days=days - 1)
    products = _products(rng)
    catalog_ts = datetime.combine(start - timedelta(days=30), datetime.min.time())

    # master data: full extracts first, then incremental changes
    _write_csv(
        root / "stores" / f"stores_{start:%Y%m%d}.csv",
        ["store_code", "store_name", "city", "state", "region", "store_format", "updated_at"],
        [[*s, catalog_ts.strftime(TS)] for s in STORES],
    )
    _write_jsonl(
        root / "products" / f"products_{start:%Y%m%d}.json",
        [{**p, "updated_at": catalog_ts.strftime(TS)} for p in products],
    )

    price_day = start + timedelta(days=2)
    repriced = rng.sample(products, 5)
    for p in repriced:
        p["list_price"] = f"{float(p['list_price']) * 1.08:.2f}"
    _write_jsonl(
        root / "products" / f"products_{price_day:%Y%m%d}.json",
        [{**p, "updated_at": f"{price_day} 06:00:00"} for p in repriced],
    )

    recat_day = start + timedelta(days=4)
    moved = rng.sample([p for p in products if p["category"] == "Biscoitos"], 2)
    for p in moved:
        p["subcategory"] = "Integrais" if p["subcategory"] != "Integrais" else "Salgados"
    _write_jsonl(
        root / "products" / f"products_{recat_day:%Y%m%d}.json",
        [{**p, "updated_at": f"{recat_day} 06:00:00"} for p in moved],
    )

    store_day = start + timedelta(days=3)
    s = list(STORES[3])
    s[5] = "supermercado"
    _write_csv(
        root / "stores" / f"stores_{store_day:%Y%m%d}.csv",
        ["store_code", "store_name", "city", "state", "region", "store_format", "updated_at"],
        [[*s, f"{store_day} 05:00:00"]],
    )

    # sales: one file per day, plus a re-sent file and a late file
    header = [
        "transaction_id",
        "line_number",
        "sale_ts",
        "store_code",
        "sku",
        "quantity",
        "unit_price",
        "discount",
        "channel",
    ]
    total = 0
    late_day = end_date - timedelta(days=2)
    late_lines: list[list] = []
    for d in range(days):
        day = start + timedelta(days=d)
        lines = _sales_lines(rng, day, products, tx_per_day, f"T{day:%Y%m%d}-")
        _corrupt(rng, lines)
        if day == late_day:
            late_lines = lines[-40:]
            lines = lines[:-40]
        _write_csv(root / "sales" / f"sales_{day:%Y%m%d}.csv", header, lines)
        total += len(lines)
        if d == 1:
            resent = rng.sample(lines, 25)
            _write_csv(root / "sales" / f"sales_{day:%Y%m%d}_resend.csv", header, resent)
    _write_csv(root / "sales" / f"sales_{end_date:%Y%m%d}_late.csv", header, late_lines)
    total += len(late_lines)
    return {"products": len(products), "stores": len(STORES), "sales_lines": total}
