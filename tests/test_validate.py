import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.validate import CONTRACTS, validate_row  # noqa: E402

VALID_ORDER = {
    "order_id": "ORD-1",
    "merchant_id": "M-1",
    "customer_vpa": "alice@upi",
    "amount": "100.50",
    "status": "paid",
    "created_at": "2026-09-01T00:00:00+00:00",
    "updated_at": "2026-09-01T00:05:00+00:00",
}


def test_valid_order_row_passes():
    assert validate_row(VALID_ORDER, CONTRACTS["orders"]) is None


def test_missing_required_field_fails():
    row = {**VALID_ORDER, "order_id": ""}
    reason = validate_row(row, CONTRACTS["orders"])
    assert reason is not None and "missing required field" in reason


def test_negative_amount_fails():
    row = {**VALID_ORDER, "amount": "-5"}
    reason = validate_row(row, CONTRACTS["orders"])
    assert reason is not None and "negative value" in reason


def test_non_numeric_amount_fails():
    row = {**VALID_ORDER, "amount": "not-a-number"}
    reason = validate_row(row, CONTRACTS["orders"])
    assert reason is not None and "non-numeric" in reason


def test_unparseable_timestamp_fails():
    row = {**VALID_ORDER, "created_at": "not-a-date"}
    reason = validate_row(row, CONTRACTS["orders"])
    assert reason is not None and "timestamp" in reason


def test_unexpected_status_value_fails():
    row = {**VALID_ORDER, "status": "bogus"}
    reason = validate_row(row, CONTRACTS["orders"])
    assert reason is not None and "unexpected value" in reason


def test_valid_gateway_row_passes():
    row = {
        "txn_id": "TXN-1", "order_id": "ORD-1", "gateway_status": "success",
        "amount": "100.50", "event_time": "2026-09-01T00:01:00+00:00", "event_type": "payment",
    }
    assert validate_row(row, CONTRACTS["gateway"]) is None


VALID_SETTLEMENT = {
    "settlement_id": "SET-1", "txn_id": "TXN-1", "gross_amount": "100.50",
    "mdr_fee": "1.50", "gst_on_fee": "0.27", "net_settled": "98.73",
    "settled_date": "2026-09-01T00:00:00", "settlement_type": "payment",
}


def test_valid_settlement_row_passes():
    assert validate_row(VALID_SETTLEMENT, CONTRACTS["settlement"]) is None


def test_settlement_negative_amount_fails_for_payment_type():
    row = {**VALID_SETTLEMENT, "gross_amount": "-100.50", "net_settled": "-98.73"}
    reason = validate_row(row, CONTRACTS["settlement"])
    assert reason is not None and "negative value" in reason


def test_settlement_negative_amount_passes_for_refund_type():
    row = {**VALID_SETTLEMENT, "settlement_type": "refund",
           "gross_amount": "-100.50", "mdr_fee": "0", "gst_on_fee": "0", "net_settled": "-100.50"}
    assert validate_row(row, CONTRACTS["settlement"]) is None


def test_settlement_unexpected_type_fails():
    row = {**VALID_SETTLEMENT, "settlement_type": "bogus"}
    reason = validate_row(row, CONTRACTS["settlement"])
    assert reason is not None and "unexpected value" in reason
