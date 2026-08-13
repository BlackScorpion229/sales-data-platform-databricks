# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate Synthetic ERP Data
# MAGIC
# MAGIC **Purpose:** Simulates the source systems (ERP + transactions) by writing
# MAGIC realistic exports to the raw-data landing zone in a **Unity Catalog
# MAGIC volume** — CSV for most files, JSON for `erp/region` (multi-format
# MAGIC ingestion demo). Serverless-compatible: the public DBFS root is disabled,
# MAGIC so `/Volumes/workspace/sales_data/raw_data` replaces `/FileStore`.
# MAGIC Includes **deliberate data-quality issues** so the Bronze → Silver
# MAGIC pipeline has real work to do.
# MAGIC
# MAGIC **Generated datasets (files in the raw volume):**
# MAGIC | File | Format | Rows | Purpose |
# MAGIC |------|--------|------|---------|
# MAGIC | `erp/region/` | JSON | ~15 | Sales regions (dimension source) |
# MAGIC | `erp/sales_rep/` | CSV | ~60 | Sales representatives (dimension source) |
# MAGIC | `erp/currency/` | CSV | ~6 | FX rates for currency standardization |
# MAGIC | `erp/customer/` | CSV | ~1,500 | Customer master |
# MAGIC | `erp/customer_updates/` | CSV | ~40 | Changed + new customer records (SCD2 demo) |
# MAGIC | `erp/product/` | CSV | ~360 | Product master |
# MAGIC | `transactions/dt=YYYY-MM-DD/` | CSV | ~270K lines | Daily order line items, 24 months |
# MAGIC | `orders/` | CSV | ~95K | Order headers (join to lines) |
# MAGIC
# MAGIC **Deliberate DQ issues injected** (deterministic — seeded RNG) — the raw
# MAGIC data is **deliberately NOT clean**; cleaning happens in Silver:
# MAGIC
# MAGIC *Transactions (line items)* — ~3% of rows are affected in total:
# MAGIC - ~1.0% exact duplicate rows (source bug) → dedup in Silver
# MAGIC - ~0.5% `customer_id` missing (NULL) → completeness + RI violation
# MAGIC - ~0.2% orphan `customer_id` (`C99999` — no master record) → RI violation
# MAGIC - ~0.2% deleted/unknown `product_id` (`P9999`) → RI violation
# MAGIC - ~0.4% truly invalid status (`On Hold`, blank) → quarantined
# MAGIC - ~0.6% malformed date (`2024-09-01 08:15:00`) → quarantined
# MAGIC - ~0.4% invalid currency (`XYZ`) → quarantined
# MAGIC - ~3.5% messy-but-recoverable values → **normalized in Silver**, not rejected:
# MAGIC   lowercase / extra-space statuses (`shipped`, `completed `, `COMPLETED`),
# MAGIC   lowercase currencies (`usd`, `gbp`)
# MAGIC - a handful of negative-quantity rows and future-dated rows (validity failures)
# MAGIC
# MAGIC *Master data* — also dirty:
# MAGIC - ~15 duplicate customer records, ~3 duplicate product records (master dedup)
# MAGIC - case/whitespace variants in `customer_status` and `product_status`
# MAGIC - ~2 products with `unit_price = NULL`; ~10 customers with `city = NULL`
# MAGIC - ~30 customers change region/status in the update file → SCD2 demo
# MAGIC
# MAGIC > **Design note:** "recoverable" dirt (case, whitespace, synonyms) must be
# MAGIC > **standardized** in Silver; "unrecoverable" dirt (missing keys, unknown
# MAGIC > values, impossible dates) must be **quarantined**. The pipeline treats the
# MAGIC > two classes differently — that is exactly what production DQ does.

# COMMAND ----------

import random
from datetime import date, datetime, timedelta

rng = random.Random(42)  # deterministic: every run produces identical data

today = date(2026, 8, 11)

# ---------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------
def rdate(start, end):
    days = (end - start).days
    return start + timedelta(days=rng.randint(0, days))

def choice_weighted(items):
    return rng.choices(list(items.keys()), weights=list(items.values()))[0]

def rround(x, n=2):
    return round(x, n)

# ---------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------
REGIONS = [
    {"region_id": "R01", "region_name": "North East",  "country": "United States", "state": "NY", "territory": "Domestic"},
    {"region_id": "R02", "region_name": "South East",  "country": "United States", "state": "FL", "territory": "Domestic"},
    {"region_id": "R03", "region_name": "Mid West",    "country": "United States", "state": "IL", "territory": "Domestic"},
    {"region_id": "R04", "region_name": "South West",  "country": "United States", "state": "TX", "territory": "Domestic"},
    {"region_id": "R05", "region_name": "West",        "country": "United States", "state": "CA", "territory": "Domestic"},
    {"region_id": "R06", "region_name": "Canada",      "country": "Canada",        "state": "ON", "territory": "International"},
    {"region_id": "R07", "region_name": "UK & Ireland","country": "United Kingdom","state": "ENG", "territory": "International"},
    {"region_id": "R08", "region_name": "DACH",        "country": "Germany",       "state": "BE", "territory": "International"},
    {"region_id": "R09", "region_name": "France",      "country": "France",        "state": "IDF", "territory": "International"},
    {"region_id": "R10", "region_name": "India",       "country": "India",         "state": "KA", "territory": "International"},
    {"region_id": "R11", "region_name": "APAC",        "country": "Australia",     "state": "NSW", "territory": "International"},
    {"region_id": "R12", "region_name": "LATAM",       "country": "Mexico",        "state": "DF", "territory": "International"},
]

REGION_BY_STATE = {  # US state -> region mapping (used to derive region from customer)
    "NY": "R01", "MA": "R01", "NJ": "R01", "PA": "R01", "CT": "R01",
    "FL": "R02", "GA": "R02", "NC": "R02", "VA": "R02", "SC": "R02",
    "IL": "R03", "OH": "R03", "MI": "R03", "MN": "R03", "WI": "R03",
    "TX": "R04", "AZ": "R04", "NM": "R04", "OK": "R04",
    "CA": "R05", "WA": "R05", "OR": "R05", "CO": "R05", "NV": "R05",
}
COUNTRY_REGION = {"Canada": "R06", "United Kingdom": "R07", "Germany": "R08",
                  "France": "R09", "India": "R10", "Australia": "R11", "Mexico": "R12"}

US_STATES = list(REGION_BY_STATE.keys())
INTL = [("Canada", "ON", "Toronto"), ("United Kingdom", "ENG", "London"),
        ("Germany", "BE", "Berlin"), ("France", "IDF", "Paris"),
        ("India", "KA", "Bengaluru"), ("Australia", "NSW", "Sydney"), ("Mexico", "DF", "Mexico City")]

FIRST = ["Apex", "Blue", "Cedar", "Delta", "Eagle", "Falcon", "Golden", "Harbor",
         "Iron", "Jade", "Kestrel", "Lakeside", "Meridian", "Northwind", "Oak",
         "Pacific", "Quartz", "Redstone", "Summit", "Titan", "Unity", "Vantage",
         "Westbrook", "Xenon", "Yellowstone", "Zenith"]
SECOND = ["Technologies", "Group", "Solutions", "Industries", "Logistics", "Systems",
          "Manufacturing", "Services", "Dynamics", "Partners", "Retail", "Trading"]

CATEGORIES = {
    "Electronics":      {"sub": ["Laptops", "Desktops", "Monitors", "Accessories", "Audio"],          "brands": ["Voltix", "NovaTech", "PixelPro"], "price": (299, 3499), "cost_pct": (0.55, 0.7)},
    "Furniture":        {"sub": ["Chairs", "Tables", "Storage", "Desks", "Bookcases"],               "brands": ["ComfortLine", "OakCraft", "ErgoForm"], "price": (89, 1899), "cost_pct": (0.5, 0.65)},
    "Office Supplies":  {"sub": ["Paper", "Writing", "Binders", "Storage", "Printing"],              "brands": ["ClearView", "StapleEasy", "PaperMax"], "price": (3, 299), "cost_pct": (0.4, 0.6)},
    "Appliances":       {"sub": ["Refrigerators", "Washers", "Dryers", "Microwaves", "Dishwashers"], "brands": ["ColdFront", "SpinCycle", "HeatWave"], "price": (199, 2499), "cost_pct": (0.6, 0.75)},
    "Networking":       {"sub": ["Routers", "Switches", "Access Points", "Cables", "Security"],       "brands": ["LinkFast", "NetCore", "SafeHarbor"], "price": (49, 1599), "cost_pct": (0.5, 0.68)},
    "Software":         {"sub": ["Licenses", "Cloud Services", "Subscriptions", "Support"],           "brands": ["SoftWorks", "CloudNine", "ByteFlow"], "price": (99, 4999), "cost_pct": (0.25, 0.45)},
}
CAT_WEIGHTS = {"Electronics": 30, "Furniture": 20, "Office Supplies": 25,
               "Appliances": 10, "Networking": 8, "Software": 7}

CURRENCIES = [
    {"currency_code": "USD", "currency_name": "US Dollar",     "exchange_rate_to_usd": 1.0,    "effective_date": "2024-01-01"},
    {"currency_code": "EUR", "currency_name": "Euro",          "exchange_rate_to_usd": 1.08,   "effective_date": "2024-01-01"},
    {"currency_code": "GBP", "currency_name": "British Pound", "exchange_rate_to_usd": 1.27,   "effective_date": "2024-01-01"},
    {"currency_code": "CAD", "currency_name": "Canadian Dollar", "exchange_rate_to_usd": 0.74, "effective_date": "2024-01-01"},
    {"currency_code": "INR", "currency_name": "Indian Rupee",  "exchange_rate_to_usd": 0.012,  "effective_date": "2024-01-01"},
    {"currency_code": "MXN", "currency_name": "Mexican Peso",  "exchange_rate_to_usd": 0.052,  "effective_date": "2024-01-01"},
]
CURRENCY_W = {"USD": 92, "EUR": 3, "GBP": 2, "CAD": 1.5, "INR": 1, "MXN": 0.5}

INDUSTRIES = ["Technology", "Healthcare", "Finance", "Retail", "Manufacturing",
              "Education", "Government", "Logistics", "Energy", "Media"]
INDUSTRY_W = {"Technology": 20, "Healthcare": 14, "Finance": 12, "Retail": 12, "Manufacturing": 12,
              "Education": 10, "Government": 6, "Logistics": 6, "Energy": 4, "Media": 4}
CUST_TYPE_W = {"Enterprise": 12, "Mid-Market": 33, "Small Business": 45, "Distributor": 10}
SEGMENT_BY_TYPE = {"Enterprise": "Corporate", "Mid-Market": "Mid-Market", "Small Business": "Small Business", "Distributor": "Distributor"}
STATUS_W = {"Active": 87, "Inactive": 13}
TAX_RATE = {"United States": 0.07, "Canada": 0.11, "United Kingdom": 0.2, "Germany": 0.19,
            "France": 0.2, "India": 0.18, "Australia": 0.1, "Mexico": 0.16}

print("Reference data + helpers ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reference dimensions (region / sales rep / currency)

# COMMAND ----------

rep_first = ["Alice", "Bob", "Carla", "David", "Emma", "Frank", "Grace", "Hassan",
             "Irene", "Jorge", "Kiran", "Lena", "Marco", "Nina", "Oscar", "Priya",
             "Quinn", "Ravi", "Sofia", "Tom", "Uma", "Victor", "Wendy", "Xavier",
             "Yara", "Zane"]
rep_last = ["Anderson", "Baker", "Chen", "Diaz", "Evans", "Foster", "Garcia",
            "Hughes", "Ivanov", "Jones", "Kim", "Lopez", "Murphy", "Nair",
            "O'Brien", "Patel", "Reyes", "Silva", "Turner", "Ueda", "Voss",
            "Walker", "Xu", "Young", "Zhang"]

sales_reps = []
rep_ids = []
for i in range(60):
    rid = f"SR{i+1:03d}"
    rep_ids.append(rid)
    sales_reps.append({
        "sales_rep_id": rid,
        "sales_rep_name": f"{rng.choice(rep_first)} {rng.choice(rep_last)}",
        "region_id": f"R{rng.randint(1, 12):02d}",
        "sales_rep_email": "",
        "hire_date": rdate(date(2018, 1, 1), date(2025, 12, 31)).isoformat(),
        "status": "Active" if rng.random() < 0.9 else "Inactive",
    })
for rep in sales_reps:
    rep["sales_rep_email"] = rep["sales_rep_name"].lower().replace(" ", ".") + "@salesco.com"

print(f"Generated {len(sales_reps)} sales reps, {len(REGIONS)} regions, {len(CURRENCIES)} currencies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer master
# MAGIC Pareto-weighted (top ~10% of customers will drive ~50% of revenue — this makes
# MAGIC the "Top Customers / revenue contribution" analytics meaningful).

# COMMAND ----------

customers = []
for i in range(1500):
    cid = f"C{10001 + i}"
    country_w = rng.random()
    if country_w < 0.78:
        state = rng.choice(US_STATES)
        country = "United States"
        region_id = REGION_BY_STATE[state]
        city = f"City{state}"
    else:
        country, state, city = rng.choice(INTL)
        region_id = COUNTRY_REGION[country]
    ctype = choice_weighted(CUST_TYPE_W)
    created = rdate(date(2019, 1, 1), date(2026, 7, 1))
    updated = rdate(created, today) if rng.random() < 0.3 else created
    customer = {
        "customer_id": cid,
        "customer_name": f"{rng.choice(FIRST)} {rng.choice(SECOND)}",
        "customer_type": ctype,
        "customer_segment": SEGMENT_BY_TYPE[ctype],
        "country": country,
        "state": state,
        "city": city,
        "region": region_id,
        "industry": choice_weighted(INDUSTRY_W),
        "sales_rep_id": rng.choice(rep_ids),
        "customer_status": choice_weighted(STATUS_W),
        "created_date": created.isoformat(),
        "updated_date": updated.isoformat(),
    }
    customers.append(customer)

# -------- master-data dirt: NULLs, case/whitespace variants, duplicates --------
for idx in rng.sample(range(len(customers)), 10):   # NULL city (incomplete address)
    customers[idx]["city"] = None
for idx in rng.sample(range(len(customers)), 15):   # messy status values (normalized in Silver)
    customers[idx]["customer_status"] = rng.choice(["active", "Active ", "INACTIVE", "ACTIVE "])
for idx in rng.sample(range(len(customers)), 5):    # trailing whitespace in names
    customers[idx]["customer_name"] += " "
for dup in rng.sample(range(len(customers)), 15):   # exact duplicate master records
    customers.append(dict(customers[dup]))
for idx in rng.sample(range(len(customers)), 8):    # case variants in type/segment
    customers[idx]["customer_type"] = rng.choice(["enterprise", "mid-market", "DISTRIBUTOR"])
    customers[idx]["customer_segment"] = rng.choice(["corporate", "small business"])

print(f"Generated {len(customers)} customers (incl. master-data dirt)")
customers[:2]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Product master
# MAGIC ~360 products across 6 categories with realistic price/cost bands.

# COMMAND ----------

products = []
prices = {}
for i in range(360):
    pid = f"P{1001 + i}"
    cat = choice_weighted(CAT_WEIGHTS)
    cfg = CATEGORIES[cat]
    sub = rng.choice(cfg["sub"])
    brand = rng.choice(cfg["brands"])
    lo, hi = cfg["price"]
    price = rround(rng.uniform(lo, hi) * rng.choice([0.9, 1, 1, 1.15]), 2)
    c_lo, c_hi = cfg["cost_pct"]
    cost = rround(price * rng.uniform(c_lo, c_hi), 2)
    eff = rdate(date(2022, 1, 1), date(2026, 6, 1))
    products.append({
        "product_id": pid,
        "product_name": f"{brand} {sub} Model {i+1:03d}",
        "product_category": cat,
        "product_subcategory": sub,
        "brand": brand,
        "unit_price": price,
        "cost": cost,
        "product_status": "Active" if rng.random() < 0.92 else "Inactive",
        "effective_date": eff.isoformat(),
        "updated_date": (eff + timedelta(days=rng.randint(0, 300))).isoformat(),
    })
    prices[pid] = price

# -------- master-data dirt: NULLs, variants, duplicates --------
for idx in rng.sample(range(len(products)), 2):     # unit_price NULL (pricing missing)
    products[idx]["unit_price"] = None
for idx in rng.sample(range(len(products)), 10):    # messy status values (normalized in Silver)
    products[idx]["product_status"] = rng.choice(["inactive", "Active ", "active"])
for idx in rng.sample(range(len(products)), 5):     # trailing whitespace in names
    products[idx]["product_name"] += " "
for dup in rng.sample(range(len(products)), 3):     # exact duplicate master records
    products.append(dict(products[dup]))

print(f"Generated {len(products)} products (incl. master-data dirt)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Orders + transactions (24 months, daily batch files)
# MAGIC
# MAGIC - Order headers written to `orders/` (completed / cancelled / returned)
# MAGIC - Line items written to `transactions/dt=YYYY-MM-DD/` (only completed + returned)
# MAGIC - One file per day — exactly what Auto Loader ingests in **incremental** mode

# COMMAND ----------

def customer_weight(c):
    """Customers follow a Pareto-ish revenue distribution."""
    return rng.expovariate(1 / (1 + (10001 + int(c["customer_id"][1:])) % 100 * 0.6))

start = date(2024, 9, 1)
order_id = 500000
orders = []
txns = []

future_dates = [today + timedelta(days=d) for d in (6, 9, 15, 22)]
dupe_pool = []
n_customers = len(customers)

for day in range((today - start).days + 1):
    day_dt = start + timedelta(days=day)
    is_weekend = day_dt.weekday() >= 5
    n_orders = rng.randint(90, 160) if not is_weekend else rng.randint(55, 95)

    day_customers = [c for c in customers if date.fromisoformat(c["created_date"]) <= day_dt]
    weights = [customer_weight(c) for c in day_customers]

    for _ in range(n_orders):
        order_id += 1
        cust = rng.choices(day_customers, weights=weights)[0]
        o_status = choice_weighted({"Completed": 90, "Cancelled": 6, "Returned": 4})
        o_currency = choice_weighted(CURRENCY_W)
        order = {
            "order_id": str(order_id),
            "customer_id": cust["customer_id"],
            "order_date": day_dt.isoformat(),
            "order_status": o_status,
            "total_amount": 0.0,      # filled after line items are generated
            "currency": o_currency,
            "region_id": cust["region"],
            "sales_rep_id": cust["sales_rep_id"],
            "source_system": "ERP-SALES",
        }
        if o_status != "Cancelled":
            n_lines = rng.randint(1, 6)
            for _ in range(n_lines):
                product_id = rng.choice(list(prices.keys()))
                qty = rng.randint(1, 25)
                unit_price = rround(prices[product_id] * rng.uniform(0.85, 1.05), 2)
                gross = rround(qty * unit_price, 2)
                disc = rround(gross * rng.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]), 2)
                tax = rround((gross - disc) * TAX_RATE[cust["country"]], 2)
                net = rround(gross - disc + tax, 2)
                txn = {
                    "transaction_id": f"T{order_id}-{len(txns) % 7 + 1}-{rng.randint(0, 99999)}",
                    "order_id": str(order_id),
                    "transaction_date": day_dt.isoformat(),
                    "customer_id": cust["customer_id"],
                    "product_id": product_id,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "discount": disc,
                    "tax": tax,
                    "gross_amount": gross,
                    "net_amount": net,
                    "currency": o_currency,
                    "region_id": cust["region"],
                    "sales_rep_id": cust["sales_rep_id"],
                    "transaction_status": "Returned" if o_status == "Returned" else "Completed",
                    "source_system": "ERP-SALES",
                }
                txns.append(txn)
        orders.append(order)

# -------- inject deliberate DQ issues (deterministic, disjoint slices) --------
# Every issue class gets its own disjoint slice of row indices — counts are
# exact and reproducible. "Recoverable" dirt is normalized in Silver;
# "unrecoverable" dirt is quarantined.
n = len(txns)
idx = list(range(n))
rng.shuffle(idx)
offset = 0

def take(k):
    global offset
    s = idx[offset:offset + k]
    offset += k
    return s

dupe_idx   = take(n // 100)                       # 1.0%  exact duplicates (copied later)
null_cust  = take(n // 200)                       # 0.5%  missing customer_id
orph_cust  = take(n // 500)                       # 0.2%  orphan customer_id (no master)
bad_prod   = take(n // 500)                       # 0.2%  unknown product_id
bad_status = take(n // 250)                       # 0.4%  invalid status: 'On Hold' / blank
bad_date   = take(n // 167)                       # 0.6%  malformed date 'YYYY-MM-DD HH:MM:SS'
bad_cur    = take(n // 250)                       # 0.4%  invalid currency 'XYZ'
messy_stat = take(n // 40)                        # 2.5%  case/whitespace status variants
messy_cur  = take(n // 83)                        # 1.2%  lowercase currency codes
neg_qty    = take(8)                              # 8     negative quantity rows

for i in null_cust:
    txns[i]["customer_id"] = None
for i in orph_cust:
    txns[i]["customer_id"] = "C99999"
for i in bad_prod:
    txns[i]["product_id"] = "P9999"
for i in bad_status:
    txns[i]["transaction_status"] = rng.choice(["On Hold", ""])
for i in bad_date:
    txns[i]["transaction_date"] = f"{txns[i]['transaction_date']} 08:15:00"
for i in bad_cur:
    txns[i]["currency"] = "XYZ"
for i in messy_stat:
    txns[i]["transaction_status"] = rng.choice(["shipped", "Shipped ", "completed", "COMPLETED", "Completed "])
for i in messy_cur:
    txns[i]["currency"] = rng.choice(["usd", "gbp", "eur"])
for i in neg_qty:
    txns[i]["quantity"] = -abs(txns[i]["quantity"])
    txns[i]["net_amount"] = rround(-abs(txns[i]["net_amount"]), 2)

# duplicate rows (copies made AFTER mutations, like a real export bug)
for dup in dupe_idx:
    txns.append(dict(txns[dup]))

# future-dated rows
for d in future_dates:
    txns.append(dict(txns[0]) | {"transaction_id": f"T-FUT-{d}", "transaction_date": d.isoformat()})

# order headers get the same messy-status treatment
for o in rng.sample(orders, len(orders) // 50):
    o["order_status"] = rng.choice(["completed", "COMPLETED ", "cancelled", "returned"])

# fill order totals from their lines
line_totals = {}
for t in txns:
    if t["transaction_date"] == today.isoformat() or "FUT" in t["transaction_id"]:
        continue
    line_totals[t["order_id"]] = line_totals.get(t["order_id"], 0) + t["net_amount"]
for o in orders:
    o["total_amount"] = rround(line_totals.get(o["order_id"], 0.0), 2)

print(f"Orders      : {len(orders)}")
print(f"Txns lines  : {len(txns)}  ({n} base + {len(dupe_idx)} duplicates + {len(future_dates)} future-dated)")
print()
print("Injected DQ issues (transaction rows):")
print(f"  unrecoverable (-> quarantine):")
print(f"    null customer_id     : {len(null_cust):>7,}  ({null_cust and len(null_cust)/n*100:.2f}%)")
print(f"    orphan customer_id   : {len(orph_cust):>7,}  ({len(orph_cust)/n*100:.2f}%)")
print(f"    unknown product_id   : {len(bad_prod):>7,}  ({len(bad_prod)/n*100:.2f}%)")
print(f"    invalid status       : {len(bad_status):>7,}  ({len(bad_status)/n*100:.2f}%)")
print(f"    malformed date       : {len(bad_date):>7,}  ({len(bad_date)/n*100:.2f}%)")
print(f"    invalid currency     : {len(bad_cur):>7,}  ({len(bad_cur)/n*100:.2f}%)")
print(f"    negative quantity    : {len(neg_qty):>7,}")
print(f"  recoverable (-> normalized in Silver):")
print(f"    messy status variants: {len(messy_stat):>7,}  ({len(messy_stat)/n*100:.2f}%)")
print(f"    lowercase currencies : {len(messy_cur):>7,}  ({len(messy_cur)/n*100:.2f}%)")
print(f"  duplicates             : {len(dupe_idx):>7,}  ({len(dupe_idx)/n*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Customer update file (SCD2 + incremental demo)
# MAGIC ~30 customers change region/state/status; ~10 are brand-new. This file is
# MAGIC written to `erp/customer_updates/` and ingested by notebook 10 to
# MAGIC demonstrate SCD Type 2 + incremental MERGE.

# COMMAND ----------

import copy

changed = rng.sample(customers, 30)
customer_updates = []
for i, c in enumerate(changed):
    upd = copy.deepcopy(c)
    if i % 3 == 0:
        new_state = rng.choice([s for s in US_STATES if s != c["state"]])
        upd["state"] = new_state
        upd["region"] = REGION_BY_STATE[new_state]
        upd["city"] = f"City{new_state}"
    elif i % 3 == 1:
        upd["customer_status"] = "Inactive" if upd["customer_status"] == "Active" else "Active"
    else:
        upd["customer_segment"] = "Corporate" if upd["customer_segment"] != "Corporate" else "Mid-Market"
    upd["updated_date"] = rdate(date(2026, 6, 1), date(2026, 8, 10)).isoformat()
    customer_updates.append(upd)

for i in range(10):  # brand-new customers
    new_id = f"C{10001 + len(customers) + i}"
    country, state, city = rng.choice(INTL)
    newc = {
        "customer_id": new_id,
        "customer_name": f"{rng.choice(FIRST)} {rng.choice(SECOND)}",
        "customer_type": choice_weighted(CUST_TYPE_W),
        "customer_segment": "Small Business",
        "country": country,
        "state": state,
        "city": city,
        "region": COUNTRY_REGION[country],
        "industry": choice_weighted(INDUSTRY_W),
        "sales_rep_id": rng.choice(rep_ids),
        "customer_status": "Active",
        "created_date": rdate(date(2026, 5, 1), date(2026, 7, 31)).isoformat(),
        "updated_date": date(2026, 8, 1).isoformat(),
    }
    customer_updates.append(newc)

print(f"Customer updates: {len(customer_updates)} ({len(changed)} changed, 10 new)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write everything to the raw volume — CSV ERP exports + one JSON folder
# MAGIC Most exports are CSV (realistic for ERP), but `erp/region` is written as
# MAGIC JSON to demonstrate multi-format ingestion in the Bronze layer. All paths
# MAGIC live under `/Volumes/workspace/sales_data/raw_data` (UC volume — the
# MAGIC serverless replacement for DBFS `/FileStore`).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Checkpoint — landing zone BEFORE generation
# MAGIC Fail-fast sanity check: on a fresh run every folder must be empty or not
# MAGIC yet created (00 only creates the base folders). Re-runs show the previous
# MAGIC exports that `mode("overwrite")` replaces.

# COMMAND ----------

LANDING_PATHS = [
    RAW_CUSTOMER,
    RAW_PRODUCT,
    f"{RAW_BASE}/erp/customer_updates",
    f"{RAW_BASE}/erp/region",
    f"{RAW_BASE}/erp/sales_rep",
    f"{RAW_BASE}/erp/currency",
    f"{RAW_BASE}/orders",
    RAW_TRANSACT,
]

def ls_r(path, max_depth=2, _depth=0):
    """Recursively list files under a volume path (tolerant of missing paths)."""
    files = []
    if _depth > max_depth:
        return files
    try:
        for e in sorted(dbutils.fs.ls(path), key=lambda x: x.path):
            if e.isDir():
                files.extend(ls_r(e.path, max_depth, _depth + 1))
            else:
                files.append(e.path)
    except Exception:
        pass
    return files

def inventory(label, paths):
    """Print a per-folder file count — the landing-zone checkpoint."""
    total = 0
    print(f"== {label} ==")
    for p in paths:
        try:
            dbutils.fs.ls(p)
        except Exception:
            print(f"  {p}  ->  NOT CREATED YET")
            continue
        files = ls_r(p)
        total += len(files)
        print(f"  {p}  ->  {len(files):>6,} file(s)" + ("  (EMPTY)" if not files else ""))
    print(f"  TOTAL: {total:,} file(s) in landing zone")
    print()

inventory("Landing zone BEFORE generation", LANDING_PATHS)

# COMMAND ----------

from pyspark.sql import functions as F

CUST_COLS = ["customer_id", "customer_name", "customer_type", "customer_segment", "country",
             "state", "city", "region", "industry", "sales_rep_id", "customer_status",
             "created_date", "updated_date"]
PROD_COLS = ["product_id", "product_name", "product_category", "product_subcategory", "brand",
             "unit_price", "cost", "product_status", "effective_date", "updated_date"]
ORD_COLS  = ["order_id", "customer_id", "order_date", "order_status", "total_amount", "currency",
             "region_id", "sales_rep_id", "source_system"]
TXN_COLS  = ["transaction_id", "order_id", "transaction_date", "customer_id", "product_id",
             "quantity", "unit_price", "discount", "tax", "gross_amount", "net_amount",
             "currency", "region_id", "sales_rep_id", "transaction_status", "source_system"]

def write_csv(rows, cols, path):
    df = spark.createDataFrame(rows, schema=cols)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    return df.count()

def write_json(rows, path):
    df = spark.createDataFrame(rows)
    df.coalesce(1).write.mode("overwrite").json(path)
    return df.count()

n_cust   = write_csv(customers,        CUST_COLS, RAW_CUSTOMER)
n_prod   = write_csv(products,         PROD_COLS, RAW_PRODUCT)
n_ord    = write_csv(orders,           ORD_COLS,  f"{RAW_BASE}/orders")
n_upd    = write_csv(customer_updates, CUST_COLS, f"{RAW_BASE}/erp/customer_updates")
n_reg    = write_json(REGIONS, f"{RAW_BASE}/erp/region")
n_rep    = write_csv(sales_reps,       ["sales_rep_id", "sales_rep_name", "region_id", "sales_rep_email", "hire_date", "status"], f"{RAW_BASE}/erp/sales_rep")
n_cur    = write_csv(CURRENCIES,       ["currency_code", "currency_name", "exchange_rate_to_usd", "effective_date"], f"{RAW_BASE}/erp/currency")

# transactions partitioned by day -> one file per day under transactions/dt=YYYY-MM-DD/
spark.createDataFrame(txns, schema=TXN_COLS) \
    .withColumn("dt", F.col("transaction_date")) \
    .coalesce(1) \
    .write.mode("overwrite").partitionBy("dt").option("header", True).csv(RAW_TRANSACT)

print(f"customers     : {n_cust:>7,}")
print(f"customer_upd  : {n_upd:>7,}")
print(f"products      : {n_prod:>7,}")
print(f"orders        : {n_ord:>7,}")
print(f"transactions  : {len(txns):>7,} rows ->", len({t['transaction_date'] for t in txns}), "daily files")
print(f"regions/reps/currencies written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Checkpoint — landing zone AFTER generation
# MAGIC Proves every export landed in the volume: CSV everywhere, one JSON folder
# MAGIC (region), and one CSV per transaction day (~730 daily files).

# COMMAND ----------

inventory("Landing zone AFTER generation", LANDING_PATHS)
