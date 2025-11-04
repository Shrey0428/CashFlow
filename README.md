# Cashflow (simple Python + Flask + SQLite)

A simple, self-contained cashflow manager:

- Multiple accounts (BANK, WALLET, STASH, etc.)
- Transactions: EXPENSE, INCOME, TRANSFER
- Budgets per category per month
- Dashboard: total + per-account balance, monthly overview, top categories
- CSV export

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Then open http://localhost:5000

## Notes

- Uses a single `cashflow.db` SQLite file; created on first run.
- Currencies are not converted; set the currency label per account.
- Forecasting, alerts, OCR, and bank sync are out of scope here but can be added later.
