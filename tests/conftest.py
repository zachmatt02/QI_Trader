import pytest

from agents import transactions


@pytest.fixture(autouse=True)
def isolated_transaction_ledger(tmp_path, monkeypatch):
    """Points the transaction ledger at a throwaway file for every test so
    simulated fills never land in the real data/transactions.db."""
    monkeypatch.setattr(transactions, "DB_PATH", tmp_path / "transactions.db")
