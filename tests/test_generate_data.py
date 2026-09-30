import datetime as dt
import random
import sys
from pathlib import Path

import pytest
from faker import Faker

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from generator import generate_data as gd  # noqa: E402


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *args, **kwargs):
        pass


class FakeConn:
    def cursor(self):
        return FakeCursor()

    def commit(self):
        pass

    def close(self):
        pass


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    """generate_data writes merchants/orders straight to Postgres; stub that out."""
    monkeypatch.setattr(gd, "get_connection", lambda: FakeConn())
    monkeypatch.setattr(gd, "execute_values", lambda cur, sql, rows: None)


def make_simulator(seed=42, error_rates=None):
    cfg = {
        "simulation": {"start_date_obj": dt.date(2026, 9, 1)},
        "injected_error_rates": error_rates or {
            "missing_settlement": 0.0, "late_settlement": 0.0, "amount_mismatch": 0.0,
            "duplicate_charge": 0.0, "orphan_payment": 0.0, "status_mismatch": 0.0,
            "invalid_row": 0.0, "pending_resolved": 0.0,
        },
        "paths": {"output_dir": "/tmp/upi_test_data", "answer_key": "/tmp/upi_test_data/answer_key.csv"},
    }
    Faker.seed(seed)
    return gd.Simulator(cfg, Faker(), random.Random(seed), None)


def test_create_merchants_are_unique():
    sim = make_simulator()
    merchants = sim.create_merchants(10)
    assert len(merchants) == 10
    assert len({m.merchant_id for m in merchants}) == 10


def test_same_seed_is_deterministic():
    sim_a = make_simulator(seed=7)
    sim_a.create_merchants(5)
    orders_a, gateway_a = [], []
    sim_a._generate_order(dt.date(2026, 9, 1), dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc), orders_a, gateway_a)

    sim_b = make_simulator(seed=7)
    sim_b.create_merchants(5)
    orders_b, gateway_b = [], []
    sim_b._generate_order(dt.date(2026, 9, 1), dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc), orders_b, gateway_b)

    assert [m.merchant_id for m in sim_a.merchants] == [m.merchant_id for m in sim_b.merchants]
    assert orders_a[0]["order_id"] == orders_b[0]["order_id"]
    assert orders_a[0]["status"] == orders_b[0]["status"]


def test_clean_order_is_marked_matched_when_no_errors_injected():
    sim = make_simulator(error_rates={
        "missing_settlement": 0.0, "late_settlement": 0.0, "amount_mismatch": 0.0,
        "duplicate_charge": 0.0, "orphan_payment": 0.0, "status_mismatch": 0.0,
        "invalid_row": 0.0, "pending_resolved": 0.0,
    })
    sim.create_merchants(3)
    orders, gateway = [], []
    for _ in range(20):
        sim._generate_order(dt.date(2026, 9, 1), dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc), orders, gateway)

    paid_orders = [o for o in orders if o["status"] == "paid"]
    assert paid_orders, "expected at least one paid order out of 20 with zero pending rate"
    for order in paid_orders:
        assert sim.answer_key[order["order_id"]][-1] == "match"


def test_pending_order_is_tracked_for_later_resolution():
    sim = make_simulator(error_rates={
        "missing_settlement": 0.0, "late_settlement": 0.0, "amount_mismatch": 0.0,
        "duplicate_charge": 0.0, "orphan_payment": 0.0, "status_mismatch": 0.0,
        "invalid_row": 0.0, "pending_resolved": 1.0,
    })
    sim.create_merchants(3)
    orders, gateway = [], []
    sim._generate_order(dt.date(2026, 9, 1), dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc), orders, gateway)

    assert len(sim.pending_orders) == 1
    assert orders[0]["status"] == "created"
    assert sim.answer_key[orders[0]["order_id"]][-1] == "pending"


def test_settlement_defaults_to_t_plus_1():
    sim = make_simulator()
    sim.create_merchants(3)
    orders, gateway = [], []
    for _ in range(20):
        sim._generate_order(dt.date(2026, 9, 1), dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc), orders, gateway)

    assert dt.date(2026, 9, 1) not in sim.settlements_by_date, "settlement should not land same-day"
    assert dt.date(2026, 9, 2) in sim.settlements_by_date, "settlement should land T+1 by default"


def test_refunded_order_generates_reversal_settlement():
    sim = make_simulator()
    sim.create_merchants(3)
    orders, gateway = [], []
    for _ in range(200):
        sim._generate_order(dt.date(2026, 9, 1), dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc), orders, gateway)

    refunded_orders = [o for o in orders if o["status"] == "refunded"]
    assert refunded_orders, "expected at least one refunded order out of 200"

    all_settlement_rows = [row for rows in sim.settlements_by_date.values() for row in rows]
    refund_rows = [row for row in all_settlement_rows if row[-1] == "refund"]
    assert refund_rows, "expected at least one refund settlement row"
    for row in refund_rows:
        assert row[2] < 0, "refund gross_amount should be negative"
        assert row[5] < 0, "refund net_settled should be negative"

    for order in refunded_orders:
        assert sim.answer_key[order["order_id"]][-1] in ("refunded", "discrepancy")
