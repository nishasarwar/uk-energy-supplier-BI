"""
Brightwatt Energy — synthetic smart-meter & billing data generator.

Produces realistic raw source files for a BI / data-engineering portfolio project,
with deliberately injected data-quality defects. Every defect is recorded in
data/raw/_dq_manifest.json as ground truth, so the silver-layer DQ framework can
be validated against known issues (reconciliation by design).

Usage:
    python src/generate_data.py --config config/generator_config.yaml
"""

from __future__ import annotations

import argparse
import json
import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml

# ----------------------------------------------------------------------------
# Reference data
# ----------------------------------------------------------------------------

REGIONS = ["North West", "South East", "Scotland", "Wales", "Midlands", "London"]

TARIFFS = [
    # tariff_id, name, type, unit_rate_pence, standing_charge_pence_per_day
    ("TAR-FIX12", "Fixed Saver 12m", "fixed", 24.5, 53.0),
    ("TAR-VAR", "Standard Variable", "variable", 27.0, 60.0),
    ("TAR-GRN", "Green Renewable", "green", 26.2, 58.0),
    ("TAR-EV", "EV Smart Night", "variable", 22.0, 55.0),
]

FIRST_NAMES = ["Amara", "James", "Priya", "Oliver", "Fatima", "Sophie", "Noah",
               "Aisha", "Liam", "Mei", "Daniel", "Zara", "Ethan", "Leila", "Omar"]
LAST_NAMES = ["Khan", "Smith", "Patel", "Jones", "Ahmed", "Brown", "Wilson",
              "Begum", "Taylor", "Evans", "Murphy", "Singh", "Clarke", "Hughes"]

# Half-hourly residential load shape (48 slots, midnight -> midnight), normalised.
INTRADAY = np.array([
    0.45, 0.42, 0.40, 0.38, 0.37, 0.36, 0.36, 0.38,  # 00:00-03:30
    0.42, 0.50, 0.62, 0.78, 0.90, 0.85, 0.72, 0.60,  # 04:00-07:30 morning ramp
    0.55, 0.52, 0.50, 0.50, 0.52, 0.55, 0.58, 0.60,  # 08:00-11:30 midday
    0.62, 0.60, 0.58, 0.56, 0.58, 0.62, 0.70, 0.82,  # 12:00-15:30
    0.95, 1.05, 1.15, 1.20, 1.18, 1.05, 0.92, 0.80,  # 16:00-19:30 evening peak
    0.70, 0.62, 0.56, 0.52, 0.50, 0.48, 0.47, 0.46,  # 20:00-23:30 wind-down
])


@dataclass
class Config:
    seed: int = 42
    output_dir: str = "data/raw"
    n_customers: int = 50
    start_date: str = "2024-10-01"
    n_days: int = 30
    reading_interval_minutes: int = 30
    defects: dict = field(default_factory=dict)


def load_config(path: str | None) -> Config:
    if path and os.path.exists(path):
        with open(path) as f:
            raw = yaml.safe_load(f)
        return Config(**raw)
    return Config()


# ----------------------------------------------------------------------------
# Dimension / source-entity generators
# ----------------------------------------------------------------------------

def gen_tariffs() -> pd.DataFrame:
    return pd.DataFrame(
        TARIFFS,
        columns=["tariff_id", "tariff_name", "tariff_type",
                 "unit_rate_pence", "standing_charge_pence_per_day"],
    )


def gen_customers(cfg: Config, rng: random.Random) -> pd.DataFrame:
    d = cfg.defects
    rows = []
    start = datetime.strptime(cfg.start_date, "%Y-%m-%d")
    for i in range(1, cfg.n_customers + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        region = rng.choice(REGIONS)
        # Inject inconsistent capitalisation so silver must standardise it.
        if d.get("region_casing_noise") and rng.random() < 0.25:
            region = rng.choice([region.upper(), region.lower(), region.title()])
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        if rng.random() < d.get("missing_email_rate", 0.0):
            email = None  # GDPR contactability gap
        signup = start - timedelta(days=rng.randint(30, 1500))
        rows.append({
            "account_id": f"ACC-{100000 + i}",
            "first_name": first,
            "last_name": last,
            "email": email,
            "postcode": f"{rng.choice('ABEGLMNSW')}{rng.randint(1,9)} {rng.randint(1,9)}{rng.choice('ABDEFGH')}{rng.choice('JLNPQR')}",
            "region": region,
            "tariff_id": rng.choice([t[0] for t in TARIFFS]),
            "segment": rng.choice(["residential", "residential", "residential", "small_business"]),
            "signup_date": signup.strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)


def gen_meters(customers: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    rows = []
    mid = 0
    for _, c in customers.iterrows():
        n_meters = 1 if rng.random() < 0.7 else 2  # some have elec + gas
        types = rng.sample(["electricity", "gas"], n_meters) if n_meters == 2 else ["electricity"]
        for mt in types:
            mid += 1
            rows.append({
                "meter_id": f"MTR-{200000 + mid}",
                "account_id": c["account_id"],
                "meter_type": mt,
                # MPAN-style for electricity, MPRN-style for gas
                "supply_id": f"{rng.randint(10,19)}{rng.randint(100000000,999999999)}",
                "install_date": c["signup_date"],
                "base_load_kw": round(rng.uniform(0.18, 0.55), 3),  # household size proxy
            })
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# Consumption modelling
# ----------------------------------------------------------------------------

def day_profile(base_load_kw: float, the_date: datetime, rng: np.random.Generator) -> np.ndarray:
    """Return 48 half-hourly kWh values for one meter on one day."""
    doy = the_date.timetuple().tm_yday
    # Winter-peaking seasonality (UK heating): peak ~ early January.
    seasonal = 1.0 + 0.35 * np.cos(2 * np.pi * (doy - 10) / 365)
    weekend = 1.10 if the_date.weekday() >= 5 else 1.0
    noise = rng.normal(1.0, 0.12, size=48).clip(0.4, 1.8)
    # base_load_kw is power; * 0.5h gives kWh per half-hour slot.
    kwh = base_load_kw * 0.5 * INTRADAY * seasonal * weekend * noise
    return kwh.clip(min=0.0)


def gen_readings(cfg: Config, meters: pd.DataFrame):
    """Generate half-hourly readings + inject defects. Returns (df, manifest)."""
    d = cfg.defects
    rng_np = np.random.default_rng(cfg.seed)
    rng = random.Random(cfg.seed + 1)
    start = datetime.strptime(cfg.start_date, "%Y-%m-%d")
    slots_per_day = 24 * 60 // cfg.reading_interval_minutes

    records = []
    rid = 0
    for _, m in meters.iterrows():
        for day_i in range(cfg.n_days):
            the_date = start + timedelta(days=day_i)
            profile = day_profile(m["base_load_kw"], the_date, rng_np)
            for slot in range(slots_per_day):
                ts = the_date + timedelta(minutes=slot * cfg.reading_interval_minutes)
                rid += 1
                records.append({
                    "reading_id": f"RD-{rid:09d}",
                    "meter_id": m["meter_id"],
                    "read_timestamp": ts,
                    "consumption_kwh": round(float(profile[slot]), 4),
                    "read_type": "A",  # Actual
                })

    df = pd.DataFrame(records)
    n = len(df)
    manifest = {"total_clean_readings": n, "defects": {}}

    def sample_idx(rate):
        # Sample positional indices from the CURRENT df (rows get dropped/added
        # as defects are injected), sizing k off the original clean count.
        k = int(n * rate)
        cur = len(df)
        if k == 0 or cur == 0:
            return np.array([], dtype=int)
        return rng_np.choice(cur, size=min(k, cur), replace=False)

    # --- Estimated reads (legitimate, but must be excluded from 'actuals') ---
    idx = sample_idx(d.get("estimated_read_rate", 0.0))
    df.loc[idx, "read_type"] = "E"
    manifest["defects"]["estimated_reads"] = {
        "count": len(idx), "rule": "read_type == 'E'",
        "sample_ids": df.loc[idx, "reading_id"].head(20).tolist()}

    # --- Null consumption ---
    idx = sample_idx(d.get("null_consumption_rate", 0.0))
    df.loc[idx, "consumption_kwh"] = np.nan
    manifest["defects"]["null_consumption"] = {
        "count": len(idx), "rule": "consumption_kwh IS NULL",
        "sample_ids": df.loc[idx, "reading_id"].head(20).tolist()}

    # --- Negative consumption (impossible) ---
    idx = sample_idx(d.get("negative_consumption_rate", 0.0))
    df.loc[idx, "consumption_kwh"] = -df.loc[idx, "consumption_kwh"].abs() - 0.1
    manifest["defects"]["negative_consumption"] = {
        "count": len(idx), "rule": "consumption_kwh < 0",
        "sample_ids": df.loc[idx, "reading_id"].head(20).tolist()}

    # --- Outlier spikes (meter fault / anomaly — feeds ML POC) ---
    idx = sample_idx(d.get("outlier_rate", 0.0))
    df.loc[idx, "consumption_kwh"] = df.loc[idx, "consumption_kwh"].abs() * rng_np.uniform(15, 45, size=len(idx))
    manifest["defects"]["outlier_spikes"] = {
        "count": len(idx), "rule": "consumption_kwh > plausibility threshold",
        "sample_ids": df.loc[idx, "reading_id"].head(20).tolist()}

    # --- Gaps (drop random intervals) ---
    idx = sample_idx(d.get("gap_rate", 0.0))
    gap_ids = df.loc[idx, "reading_id"].tolist()
    df = df.drop(index=idx).reset_index(drop=True)
    manifest["defects"]["dropped_intervals"] = {
        "count": len(gap_ids), "rule": "missing (meter, timestamp) in expected grid",
        "sample_ids": gap_ids[:20]}

    # --- Late arrivals (timestamp before the batch window) ---
    late_idx = sample_idx(d.get("late_arrival_rate", 0.0))
    late_days = d.get("late_arrival_days", 3)
    df.loc[late_idx, "read_timestamp"] = df.loc[late_idx, "read_timestamp"] - pd.Timedelta(days=late_days)
    manifest["defects"]["late_arrivals"] = {
        "count": len(late_idx), "rule": "read_timestamp < batch_start",
        "sample_ids": df.loc[late_idx, "reading_id"].head(20).tolist()}

    # --- Exact duplicates ---
    idx = sample_idx(d.get("duplicate_rate", 0.0))
    dups = df.loc[idx].copy()
    df = pd.concat([df, dups], ignore_index=True)
    manifest["defects"]["exact_duplicates"] = {
        "count": len(dups), "rule": "duplicate (meter_id, read_timestamp)",
        "sample_ids": dups["reading_id"].head(20).tolist()}

    # --- Orphan readings (meter_id not in meters) ---
    k = int(n * d.get("orphan_reading_rate", 0.0))
    if k > 0:
        base = df.sample(k, random_state=cfg.seed).copy()
        base["meter_id"] = [f"MTR-{900000 + i}" for i in range(k)]  # non-existent
        base["reading_id"] = [f"RD-ORPHAN-{i:06d}" for i in range(k)]
        df = pd.concat([df, base], ignore_index=True)
        manifest["defects"]["orphan_readings"] = {
            "count": k, "rule": "meter_id NOT IN meters",
            "sample_ids": base["reading_id"].head(20).tolist()}

    df = df.sort_values(["read_timestamp", "meter_id"]).reset_index(drop=True)
    return df, manifest


# ----------------------------------------------------------------------------
# Billing
# ----------------------------------------------------------------------------

def gen_invoices(customers, meters, readings, tariffs):
    """Monthly invoices per account from clean actual consumption."""
    rate = tariffs.set_index("tariff_id")
    clean = readings[(readings["read_type"] == "A") & (readings["consumption_kwh"].notna())
                     & (readings["consumption_kwh"] >= 0) & (readings["consumption_kwh"] < 50)]
    m2a = meters.set_index("meter_id")["account_id"].to_dict()
    clean = clean.assign(account_id=clean["meter_id"].map(m2a)).dropna(subset=["account_id"])
    clean["month"] = pd.to_datetime(clean["read_timestamp"]).dt.to_period("M").astype(str)
    agg = clean.groupby(["account_id", "month"]).agg(
        kwh=("consumption_kwh", "sum"), n_reads=("consumption_kwh", "size")).reset_index()
    # Only bill months with substantial coverage (drops phantom partial periods
    # created by late-arriving reads landing in an otherwise-empty month).
    grp = agg[agg["n_reads"] >= 300].rename(columns={"kwh": "consumption_kwh"})

    cust = customers.set_index("account_id")
    rows = []
    inv = 0
    for _, g in grp.iterrows():
        acc = g["account_id"]
        if acc not in cust.index:
            continue
        tariff_id = cust.loc[acc, "tariff_id"]
        unit = rate.loc[tariff_id, "unit_rate_pence"]
        standing = rate.loc[tariff_id, "standing_charge_pence_per_day"]
        period = pd.Period(g["month"])
        days = period.days_in_month
        amount = g["consumption_kwh"] * unit + standing * days
        inv += 1
        rows.append({
            "invoice_id": f"INV-{500000 + inv}",
            "account_id": acc,
            "period_start": period.start_time.strftime("%Y-%m-%d"),
            "period_end": period.end_time.strftime("%Y-%m-%d"),
            "total_kwh": round(float(g["consumption_kwh"]), 2),
            "amount_pence": int(round(amount)),
            "issued_date": (period.end_time + pd.Timedelta(days=5)).strftime("%Y-%m-%d"),
            "status": random.choice(["paid", "paid", "paid", "outstanding", "overdue"]),
        })
    return pd.DataFrame(rows)


def gen_payments(invoices, rng: random.Random):
    rows = []
    pid = 0
    for _, inv in invoices.iterrows():
        if inv["status"] == "paid":
            pid += 1
            rows.append({
                "payment_id": f"PAY-{700000 + pid}",
                "invoice_id": inv["invoice_id"],
                "account_id": inv["account_id"],
                "amount_pence": inv["amount_pence"],
                "paid_date": (datetime.strptime(inv["issued_date"], "%Y-%m-%d")
                             + timedelta(days=rng.randint(1, 25))).strftime("%Y-%m-%d"),
                "method": rng.choice(["direct_debit", "direct_debit", "card", "bank_transfer"]),
            })
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# Output
# ----------------------------------------------------------------------------

def write_outputs(cfg, customers, meters, tariffs, readings, invoices, payments, manifest):
    out = cfg.output_dir
    os.makedirs(os.path.join(out, "readings"), exist_ok=True)
    os.makedirs(os.path.join(out, "readings_drift"), exist_ok=True)

    customers.to_csv(os.path.join(out, "customers.csv"), index=False)
    meters.to_csv(os.path.join(out, "meters.csv"), index=False)
    tariffs.to_csv(os.path.join(out, "tariffs.csv"), index=False)
    # invoices as JSON to exercise multi-format ingestion in bronze
    invoices.to_json(os.path.join(out, "invoices.json"), orient="records", indent=2)
    payments.to_csv(os.path.join(out, "payments.csv"), index=False)

    # Partition readings by day (realistic landing pattern for bronze).
    readings = readings.copy()
    readings["read_timestamp"] = pd.to_datetime(readings["read_timestamp"])
    readings["_day"] = readings["read_timestamp"].dt.strftime("%Y%m%d")
    drift_day = None
    if cfg.defects.get("schema_drift_batch"):
        drift_day = sorted(readings["_day"].unique())[len(readings["_day"].unique()) // 2]

    for day, g in readings.groupby("_day"):
        g = g.drop(columns="_day")
        if day == drift_day:
            # Schema drift: renamed columns + different timestamp format + extra col.
            g = g.rename(columns={"read_timestamp": "ts", "consumption_kwh": "kwh"})
            g["ts"] = pd.to_datetime(g["ts"]).dt.strftime("%d/%m/%Y %H:%M")
            g["source_system"] = "legacy_hh_v1"
            g[["reading_id", "meter_id", "ts", "kwh", "read_type", "source_system"]].to_csv(
                os.path.join(out, "readings_drift", f"readings_{day}_DRIFT.csv"), index=False)
            manifest["defects"]["schema_drift_batch"] = {
                "file": f"readings_drift/readings_{day}_DRIFT.csv",
                "rule": "columns renamed (ts, kwh), dd/mm/yyyy timestamps, extra source_system col"}
        else:
            g.to_csv(os.path.join(out, "readings", f"readings_{day}.csv"), index=False)

    with open(os.path.join(out, "_dq_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/generator_config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)

    rng = random.Random(cfg.seed)
    print(f"Generating Brightwatt Energy data: {cfg.n_customers} customers x {cfg.n_days} days...")

    tariffs = gen_tariffs()
    customers = gen_customers(cfg, rng)
    meters = gen_meters(customers, rng)
    readings, manifest = gen_readings(cfg, meters)
    invoices = gen_invoices(customers, meters, readings, tariffs)
    payments = gen_payments(invoices, rng)
    write_outputs(cfg, customers, meters, tariffs, readings, invoices, payments, manifest)

    print(f"  customers : {len(customers):>8,}")
    print(f"  meters    : {len(meters):>8,}")
    print(f"  readings  : {len(readings):>8,}  (incl. injected defects)")
    print(f"  invoices  : {len(invoices):>8,}")
    print(f"  payments  : {len(payments):>8,}")
    print(f"  defects   : {sum(v.get('count', 0) for v in manifest['defects'].values()):>8,} rows logged to _dq_manifest.json")
    print(f"Output -> {cfg.output_dir}/")


if __name__ == "__main__":
    main()
