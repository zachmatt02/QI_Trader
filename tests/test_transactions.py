import sqlite3

import pytest

from gateway import execution, transactions


def test_record_and_list_transaction(tmp_path):
    db = tmp_path / "transactions.db"
    row_id = transactions.record_transaction(
        "tsla", 420.5, 3, "BUY", isin="US88160R1014", currency="USD",
        when="2026-07-03T14:30:00Z", order_id="12345",
        account_id="DU000000", conid=76792991, db_path=db)
    assert row_id == 1

    rows = transactions.list_transactions(db_path=db)
    assert len(rows) == 1
    row = rows[0]
    assert row["ticker"] == "TSLA"  # stored upper-cased
    assert row["isin"] == "US88160R1014"
    assert row["share_price"] == 420.5
    assert row["currency"] == "USD"
    assert row["datetime"] == "2026-07-03T14:30:00Z"
    assert row["shares"] == 3
    assert row["buy"] == transactions.BUY
    assert row["status"] == "Filled"
    assert row["created_at"]  # filled in by the schema default


def test_side_accepts_strings_and_flags(tmp_path):
    db = tmp_path / "transactions.db"
    transactions.record_transaction("TSLA", 400, 1, "SELL", db_path=db)
    transactions.record_transaction("TSLA", 400, 1, 1, db_path=db)
    rows = transactions.list_transactions(db_path=db)
    assert sorted(r["buy"] for r in rows) == [transactions.SELL,
                                              transactions.BUY]
    with pytest.raises(ValueError):
        transactions.record_transaction("TSLA", 400, 1, "SHORT", db_path=db)


def test_schema_rejects_bad_rows(tmp_path):
    db = tmp_path / "transactions.db"
    with pytest.raises(sqlite3.IntegrityError):  # shares must be positive
        transactions.record_transaction("TSLA", 400, 0, "BUY", db_path=db)
    with pytest.raises(sqlite3.IntegrityError):  # price must be positive
        transactions.record_transaction("TSLA", -1, 1, "BUY", db_path=db)
    assert transactions.list_transactions(db_path=db) == []


def test_position_nets_buys_and_sells(tmp_path):
    db = tmp_path / "transactions.db"
    transactions.record_transaction("TSLA", 400, 5, "BUY", db_path=db)
    transactions.record_transaction("TSLA", 410, 2, "SELL", db_path=db)
    transactions.record_transaction("AAPL", 200, 1, "BUY", db_path=db)
    assert transactions.position("TSLA", db_path=db) == 3
    assert transactions.position("AAPL", db_path=db) == 1
    assert transactions.position("MSFT", db_path=db) == 0


def test_execution_record_fill_writes_ledger_row(tmp_path, monkeypatch):
    db = tmp_path / "transactions.db"
    monkeypatch.setattr(transactions, "DB_PATH", db)
    # a filled order as returned by the gateway's /iserver/account/orders
    order_state = {"status": "Filled", "orderId": 987, "ticker": "TSLA",
                   "side": "BUY", "avgPrice": "419.75",
                   "filledQuantity": 2, "cashCcy": "USD", "conid": 76792991}
    execution.record_fill(order_state, "DU000000", "TSLA",
                          quantity=2, limit_price=420.0)

    rows = transactions.list_transactions(db_path=db)
    assert len(rows) == 1
    row = rows[0]
    assert row["share_price"] == 419.75  # avgPrice preferred over the limit
    assert row["shares"] == 2
    assert row["buy"] == transactions.BUY
    assert row["order_id"] == "987"
    assert row["account_id"] == "DU000000"


def test_execution_record_fill_skips_unfilled_orders(tmp_path, monkeypatch):
    db = tmp_path / "transactions.db"
    monkeypatch.setattr(transactions, "DB_PATH", db)
    execution.record_fill({"status": "Cancelled"}, "DU000000", "TSLA", 1, 420)
    execution.record_fill(None, "DU000000", "TSLA", 1, 420)
    assert transactions.list_transactions(db_path=db) == []
