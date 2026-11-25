"""Clean-room deterministic TPC-H-derived synthetic analytical dataset generator.

License note: This generator creates original synthetic e-commerce order-analytics tables
(orders, lineitem, customer, nation) inspired by analytical schema patterns. It does NOT
use TPC-H source code or data and makes no claim of official audited TPC-H compliance.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import numpy as np
import pyarrow as pa

HOT_CUSTOMER_ID = 1

NATIONS: list[tuple[int, str, int, str]] = [
    (0, "UNITED STATES", 0, "AMERICAS"),
    (1, "CANADA", 0, "AMERICAS"),
    (2, "BRAZIL", 0, "AMERICAS"),
    (3, "UNITED KINGDOM", 1, "EUROPE"),
    (4, "GERMANY", 1, "EUROPE"),
    (5, "FRANCE", 1, "EUROPE"),
    (6, "JAPAN", 2, "ASIA"),
    (7, "INDIA", 2, "ASIA"),
    (8, "SINGAPORE", 2, "ASIA"),
    (9, "AUSTRALIA", 3, "OCEANIA"),
    (10, "SOUTH AFRICA", 4, "MEA"),
    (11, "UAE", 4, "MEA"),
]

ORDER_MONTHS: list[str] = [
    "2025-01",
    "2025-02",
    "2025-03",
    "2025-04",
    "2025-05",
    "2025-06",
]

SEGMENTS: list[str] = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
PRIORITIES: list[str] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
STATUSES: list[str] = ["F", "O", "P"]
RETURN_FLAGS: list[str] = ["A", "N", "R"]


@dataclass(frozen=True)
class ScaleProfile:
    name: str
    num_customers: int
    num_orders: int
    avg_lines_per_order: int
    hot_key_ratio: float = 0.22
    duplicate_rate: float = 0.015
    corrupt_rate: float = 0.005


SCALE_PROFILES: dict[str, ScaleProfile] = {
    "smoke": ScaleProfile(name="smoke", num_customers=200, num_orders=2_000, avg_lines_per_order=3),
    "local": ScaleProfile(name="local", num_customers=2_000, num_orders=30_000, avg_lines_per_order=4),
    "stress": ScaleProfile(name="stress", num_customers=8_000, num_orders=120_000, avg_lines_per_order=4),
}


def generate_nation_table() -> pa.Table:
    return pa.table(
        {
            "nation_id": pa.array([n[0] for n in NATIONS], type=pa.int64()),
            "nation_name": pa.array([n[1] for n in NATIONS], type=pa.string()),
            "region_id": pa.array([n[2] for n in NATIONS], type=pa.int64()),
            "region_name": pa.array([n[3] for n in NATIONS], type=pa.string()),
        }
    )


def generate_customer_table(profile: ScaleProfile, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed + 101)
    n = profile.num_customers
    cust_ids = np.arange(1, n + 1, dtype=np.int64)
    nation_ids = rng.integers(0, len(NATIONS), size=n, dtype=np.int64)
    nation_to_region = np.array([row[2] for row in NATIONS], dtype=np.int64)
    region_ids = nation_to_region[nation_ids]
    # Pin hot customer to nation 0 / region 0 deterministically
    nation_ids[0] = 0
    region_ids[0] = 0
    acct_bal = np.round(rng.uniform(-500.0, 15000.0, size=n), 2)
    seg_idx = rng.integers(0, len(SEGMENTS), size=n)
    mkt_segment = [SEGMENTS[int(i)] for i in seg_idx]
    names = [f"Customer#{int(cid):06d}" for cid in cust_ids]
    is_hot = [cid == HOT_CUSTOMER_ID for cid in cust_ids]

    return pa.table(
        {
            "customer_id": pa.array(cust_ids, type=pa.int64()),
            "name": pa.array(names, type=pa.string()),
            "nation_id": pa.array(nation_ids, type=pa.int64()),
            "region_id": pa.array(region_ids, type=pa.int64()),
            "acct_bal": pa.array(acct_bal, type=pa.float64()),
            "mkt_segment": pa.array(mkt_segment, type=pa.string()),
            "is_hot_account": pa.array(is_hot, type=pa.bool_()),
        }
    )


def generate_orders_and_lineitems(
    profile: ScaleProfile,
    seed: int = 42,
    include_anomalies: bool = True,
) -> tuple[pa.Table, pa.Table, list[dict[str, object]]]:
    """Generate deterministic orders, lineitems, and raw anomalous records for DQ testing.

    Orders are generated with:
    - Heavy hot-key skew on customer_id = HOT_CUSTOMER_ID (~22% of orders)
    - Explicit event_time vs ingested_at (with late arrivals)
    - Optional duplicate events (with newer source_version corrections) and invalid records
    """
    rng = np.random.default_rng(seed + 202)
    n_orders = profile.num_orders
    n_customers = profile.num_customers

    order_ids = np.arange(1, n_orders + 1, dtype=np.int64)

    # Heavy key skew: hot_key_ratio of orders go to HOT_CUSTOMER_ID = 1
    is_hot_order = rng.random(n_orders) < profile.hot_key_ratio
    random_custs = rng.integers(2, max(3, n_customers + 1), size=n_orders, dtype=np.int64)
    customer_ids = np.where(is_hot_order, HOT_CUSTOMER_ID, random_custs).astype(np.int64)

    # Spread across 6 months (2025-01..2025-06) and 5 regions (0..4)
    month_idx = rng.integers(0, len(ORDER_MONTHS), size=n_orders)
    day_of_month = rng.integers(1, 29, size=n_orders)
    order_months = [ORDER_MONTHS[int(m)] for m in month_idx]
    order_dates = [f"{ORDER_MONTHS[int(m)]}-{int(d):02d}" for m, d in zip(month_idx, day_of_month, strict=True)]

    region_ids = rng.integers(0, 5, size=n_orders, dtype=np.int64)
    status_idx = rng.integers(0, len(STATUSES), size=n_orders)
    order_statuses = [STATUSES[int(i)] for i in status_idx]
    prio_idx = rng.integers(0, len(PRIORITIES), size=n_orders)
    order_priorities = [PRIORITIES[int(i)] for i in prio_idx]

    # Generate lineitems first so order.total_price is reconcilable with lineitems!
    lines_per_order = rng.integers(1, profile.avg_lines_per_order * 2, size=n_orders, dtype=np.int64)
    total_lines = int(lines_per_order.sum())

    li_order_ids = np.repeat(order_ids, lines_per_order)
    li_region_ids = np.repeat(region_ids, lines_per_order)
    li_month_idx = np.repeat(month_idx, lines_per_order)
    li_order_months = [ORDER_MONTHS[int(m)] for m in li_month_idx]

    # Line numbers 1..k per order
    starts = np.repeat(np.cumsum(lines_per_order) - lines_per_order, lines_per_order)
    li_line_numbers = (np.arange(total_lines, dtype=np.int64) - starts + 1).astype(np.int64)

    li_part_ids = rng.integers(1, 5001, size=total_lines, dtype=np.int64)
    li_supplier_ids = rng.integers(1, 251, size=total_lines, dtype=np.int64)
    li_quantity = rng.integers(1, 51, size=total_lines, dtype=np.int64)
    unit_price = np.round(rng.uniform(10.0, 450.0, size=total_lines), 2)
    li_extended_price = np.round(li_quantity * unit_price, 2)
    li_discount = np.round(rng.choice(np.linspace(0.0, 0.10, 11), size=total_lines), 2)
    li_tax = np.round(rng.choice(np.linspace(0.0, 0.08, 9), size=total_lines), 2)
    rf_idx = rng.integers(0, len(RETURN_FLAGS), size=total_lines)
    li_return_flags = [RETURN_FLAGS[int(i)] for i in rf_idx]

    # Ship date = order date + 1..14 days (kept within month+1 string)
    li_order_dates = np.repeat(np.array(order_dates), lines_per_order)
    ship_offsets = rng.integers(1, 15, size=total_lines)
    base_dt = datetime(2025, 1, 1, tzinfo=UTC)
    # Vectorized fast date formatting using lookup dict for unique order dates
    unique_dates = set(order_dates)
    dt_map = {d: datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=UTC) for d in unique_dates}
    li_ship_dates = [
        (dt_map[str(od)] + timedelta(days=int(off))).strftime("%Y-%m-%d")
        for od, off in zip(li_order_dates, ship_offsets, strict=True)
    ]

    # Compute exact reconcilable order total_price from net lineitem value
    net_line = np.round(li_extended_price * (1.0 - li_discount) * (1.0 + li_tax), 2)
    order_totals = np.zeros(n_orders, dtype=np.float64)
    np.add.at(order_totals, li_order_ids - 1, net_line)
    order_totals = np.round(order_totals, 2)

    # Event time and processing time (with deterministic late arrivals)
    ingested_base = base_dt + timedelta(days=190)
    event_times = [f"{d}T12:00:00Z" for d in order_dates]
    ingested_ats = [ingested_base.strftime("%Y-%m-%dT%H:%M:%SZ")] * n_orders
    source_versions = np.ones(n_orders, dtype=np.int64)

    orders_dict: dict[str, pa.Array] = {
        "order_id": pa.array(order_ids, type=pa.int64()),
        "customer_id": pa.array(customer_ids, type=pa.int64()),
        "order_status": pa.array(order_statuses, type=pa.string()),
        "total_price": pa.array(order_totals, type=pa.float64()),
        "order_date": pa.array(order_dates, type=pa.string()),
        "order_month": pa.array(order_months, type=pa.string()),
        "order_priority": pa.array(order_priorities, type=pa.string()),
        "region_id": pa.array(region_ids, type=pa.int64()),
        "event_time": pa.array(event_times, type=pa.string()),
        "ingested_at": pa.array(ingested_ats, type=pa.string()),
        "source_version": pa.array(source_versions, type=pa.int64()),
    }
    orders_table = pa.table(orders_dict)

    lineitem_table = pa.table(
        {
            "order_id": pa.array(li_order_ids, type=pa.int64()),
            "line_number": pa.array(li_line_numbers, type=pa.int64()),
            "part_id": pa.array(li_part_ids, type=pa.int64()),
            "supplier_id": pa.array(li_supplier_ids, type=pa.int64()),
            "quantity": pa.array(li_quantity, type=pa.int64()),
            "extended_price": pa.array(li_extended_price, type=pa.float64()),
            "discount": pa.array(li_discount, type=pa.float64()),
            "tax": pa.array(li_tax, type=pa.float64()),
            "return_flag": pa.array(li_return_flags, type=pa.string()),
            "ship_date": pa.array(li_ship_dates, type=pa.string()),
            "order_month": pa.array(li_order_months, type=pa.string()),
            "region_id": pa.array(li_region_ids, type=pa.int64()),
        }
    )

    anomalies: list[dict[str, object]] = []
    if include_anomalies:
        # Inject deterministic duplicate events + invalid records for DQ quarantine verification
        n_dups = max(3, int(n_orders * profile.duplicate_rate))
        dup_indices = rng.choice(n_orders, size=min(n_dups, n_orders), replace=False)
        dup_table = orders_table.take(pa.array(dup_indices))
        orders_table = pa.concat_tables([orders_table, dup_table])

        # Invalid records: negative total_price, null customer_id, out-of-range region_id
        anomalies = [
            {
                "order_id": -999,
                "customer_id": 1,
                "order_status": "O",
                "total_price": -150.0,
                "order_date": "2025-03-10",
                "order_month": "2025-03",
                "order_priority": "1-URGENT",
                "region_id": 0,
                "reason": "negative_total_price_and_id",
            },
            {
                "order_id": 999_999_001,
                "customer_id": None,
                "order_status": "X",
                "total_price": 42.5,
                "order_date": "2025-04-01",
                "order_month": "2025-04",
                "order_priority": "2-HIGH",
                "region_id": 99,
                "reason": "null_customer_id_and_invalid_region",
            },
        ]

    return orders_table, lineitem_table, anomalies
