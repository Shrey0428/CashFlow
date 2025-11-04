from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import sqlite3, os, csv
from datetime import datetime, date
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(__file__), "cashflow.db")

app = Flask(__name__)
app.secret_key = "dev-secret"  # replace in production

@contextmanager
def db() :
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    with db() as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS accounts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL CHECK (type IN ('BANK','WALLET','STASH','CREDIT','OTHER')),
            currency TEXT NOT NULL DEFAULT 'USD',
            opening_balance REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            type TEXT NOT NULL CHECK (type IN ('EXPENSE','INCOME','TRANSFER')),
            amount REAL NOT NULL,
            category TEXT,
            merchant TEXT,
            memo TEXT,
            booked_at TEXT NOT NULL,
            transfer_account_id INTEGER REFERENCES accounts(id),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS budgets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
            year INTEGER NOT NULL,
            category TEXT NOT NULL,
            limit_amount REAL NOT NULL
        );
        """)

def get_accounts():
    with db() as conn:
        rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
        return [dict(r) for r in rows]

def account_balance(account_id):
    with db() as conn:
        # opening + incomes - expenses + transfers in - transfers out
        opening = conn.execute("SELECT opening_balance FROM accounts WHERE id=?", (account_id,)).fetchone()
        if not opening:
            return 0.0
        opening = opening["opening_balance"]
        inc = conn.execute("SELECT COALESCE(SUM(amount),0) AS s FROM transactions WHERE account_id=? AND type='INCOME'", (account_id,)).fetchone()["s"]
        exp = conn.execute("SELECT COALESCE(SUM(amount),0) AS s FROM transactions WHERE account_id=? AND type='EXPENSE'", (account_id,)).fetchone()["s"]
        tin = conn.execute("SELECT COALESCE(SUM(amount),0) AS s FROM transactions WHERE transfer_account_id=? AND type='TRANSFER'", (account_id,)).fetchone()["s"]
        tout = conn.execute("SELECT COALESCE(SUM(amount),0) AS s FROM transactions WHERE account_id=? AND type='TRANSFER'", (account_id,)).fetchone()["s"]
        return float(opening + inc - exp + tin - tout)

def totals():
    accs = get_accounts()
    per = []
    total = 0.0
    for a in accs:
        bal = account_balance(a["id"])
        per.append({"id": a["id"], "name": a["name"], "type": a["type"], "currency": a["currency"], "balance": bal})
        total += bal
    return total, per

def month_bounds(d: date):
    start = d.replace(day=1)
    if d.month == 12:
        end = d.replace(year=d.year+1, month=1, day=1)
    else:
        end = d.replace(month=d.month+1, day=1)
    return start, end

@app.route("/")
def index():
    init_db()
    total, per = totals()
    today = date.today()
    mstart, mend = month_bounds(today)
    with db() as conn:
        spent = conn.execute(
            "SELECT COALESCE(SUM(amount),0) AS s FROM transactions WHERE type='EXPENSE' AND booked_at>=? AND booked_at<?",
            (mstart.isoformat(), mend.isoformat())
        ).fetchone()["s"]
        income = conn.execute(
            "SELECT COALESCE(SUM(amount),0) AS s FROM transactions WHERE type='INCOME' AND booked_at>=? AND booked_at<?",
            (mstart.isoformat(), mend.isoformat())
        ).fetchone()["s"]
        top_cats = conn.execute(
            "SELECT category, ROUND(SUM(amount),2) as total FROM transactions WHERE type='EXPENSE' AND booked_at>=? AND booked_at<? GROUP BY category ORDER BY total DESC LIMIT 5",
            (mstart.isoformat(), mend.isoformat())
        ).fetchall()
        budgets = conn.execute("SELECT * FROM budgets WHERE month=? AND year=?", (today.month, today.year)).fetchall()
        budget_progress = []
        for b in budgets:
            spent_cat = conn.execute(
                "SELECT COALESCE(SUM(amount),0) AS s FROM transactions WHERE type='EXPENSE' AND category=? AND booked_at>=? AND booked_at<?",
                (b["category"], mstart.isoformat(), mend.isoformat())
            ).fetchone()["s"]
            remaining = float(b["limit_amount"] - spent_cat)
            budget_progress.append({"category": b["category"], "limit": b["limit_amount"], "spent": float(spent_cat), "remaining": remaining})
    return render_template("index.html",
                           total=total, per_accounts=per,
                           month=today.strftime("%B %Y"),
                           spent=spent, income=income,
                           top_cats=top_cats, budgets=budget_progress)

@app.route("/accounts", methods=["GET","POST"])
def accounts():
    init_db()
    if request.method == "POST":
        name = request.form["name"].strip()
        atype = request.form.get("type","BANK")
        currency = request.form.get("currency","USD")
        opening = float(request.form.get("opening_balance","0") or 0)
        if not name:
            flash("Name is required","error")
        else:
            with db() as conn:
                conn.execute("INSERT INTO accounts(name,type,currency,opening_balance) VALUES (?,?,?,?)",
                             (name, atype, currency, opening))
            flash("Account created","success")
        return redirect(url_for("accounts"))
    accs = get_accounts()
    # include computed balances
    enriched = []
    for a in accs:
        a = dict(a)
        a["balance"] = account_balance(a["id"])
        enriched.append(a)
    return render_template("accounts.html", accounts=enriched)

@app.route("/transactions", methods=["GET","POST"])
def transactions():
    init_db()
    with db() as conn:
        if request.method == "POST":
            ttype = request.form["type"]
            account_id = int(request.form["account_id"])
            amount = float(request.form["amount"])
            category = request.form.get("category") or None
            merchant = request.form.get("merchant") or None
            memo = request.form.get("memo") or None
            booked_at = request.form.get("booked_at") or datetime.today().date().isoformat()
            transfer_account_id = request.form.get("transfer_account_id")
            if ttype == "TRANSFER":
                if not transfer_account_id:
                    flash("Transfer requires target account","error")
                    return redirect(url_for("transactions"))
                transfer_account_id = int(transfer_account_id)
            else:
                transfer_account_id = None
            conn.execute("""
                INSERT INTO transactions(account_id,type,amount,category,merchant,memo,booked_at,transfer_account_id)
                VALUES (?,?,?,?,?,?,?,?)
            """,(account_id, ttype, amount, category, merchant, memo, booked_at, transfer_account_id))
            flash("Transaction added","success")
            return redirect(url_for("transactions"))
        # filters
        q = "SELECT * FROM transactions WHERE 1=1"
        params = []
        if request.args.get("type"):
            q += " AND type=?"; params.append(request.args["type"])
        if request.args.get("account_id"):
            q += " AND account_id=?"; params.append(request.args["account_id"])
        if request.args.get("category"):
            q += " AND category=?"; params.append(request.args["category"])
        if request.args.get("date_from"):
            q += " AND booked_at>=?"; params.append(request.args["date_from"])
        if request.args.get("date_to"):
            q += " AND booked_at<?"; params.append(request.args["date_to"])
        q += " ORDER BY booked_at DESC, id DESC LIMIT 500"
        rows = conn.execute(q, params).fetchall()
        accounts = get_accounts()
        return render_template("transactions.html", transactions=rows, accounts=accounts)

@app.route("/budgets", methods=["GET","POST"])
def budgets():
    init_db()
    with db() as conn:
        if request.method == "POST":
            month = int(request.form["month"])
            year = int(request.form["year"])
            category = request.form["category"].strip()
            limit_amount = float(request.form["limit_amount"])
            conn.execute("INSERT INTO budgets(month,year,category,limit_amount) VALUES (?,?,?,?)",
                         (month, year, category, limit_amount))
            flash("Budget created","success")
            return redirect(url_for("budgets"))
        rows = conn.execute("SELECT * FROM budgets ORDER BY year DESC, month DESC, category").fetchall()
        return render_template("budgets.html", budgets=rows)

@app.route("/export/transactions.csv")
def export_csv():
    init_db()
    with db() as conn:
        rows = conn.execute("SELECT * FROM transactions ORDER BY booked_at DESC, id DESC").fetchall()
    path = os.path.join(os.path.dirname(__file__), "transactions_export.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(rows[0].keys() if rows else ["id","account_id","type","amount","category","merchant","memo","booked_at","transfer_account_id","created_at"])
        for r in rows:
            w.writerow([r[k] for k in r.keys()])
    return send_file(path, as_attachment=True, download_name="transactions.csv")

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
