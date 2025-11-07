
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Cashflow", page_icon="💸", layout="wide")

DB_PATH = os.path.join(os.environ.get("DATA_DIR", "."), "cashflow.db")

@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def column_exists(conn, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r["name"] == column for r in cur.fetchall())

def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
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
        );""")
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
        );""")
        # Add rollback/void columns if missing
        if not column_exists(conn, "transactions", "voided"):
            c.execute("ALTER TABLE transactions ADD COLUMN voided INTEGER NOT NULL DEFAULT 0")
        if not column_exists(conn, "transactions", "voided_at"):
            c.execute("ALTER TABLE transactions ADD COLUMN voided_at TEXT")

        c.execute("""
        CREATE TABLE IF NOT EXISTS budgets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
            year INTEGER NOT NULL,
            category TEXT NOT NULL,
            limit_amount REAL NOT NULL
        );""")

def get_accounts_df():
    with db() as conn:
        return pd.read_sql_query("SELECT * FROM accounts ORDER BY id", conn)

def account_balance(conn, account_id: int) -> float:
    opening = conn.execute("SELECT opening_balance FROM accounts WHERE id=?", (account_id,)).fetchone()
    if not opening:
        return 0.0
    opening = opening["opening_balance"]
    inc = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE account_id=? AND type='INCOME' AND voided=0", (account_id,)).fetchone()[0]
    exp = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE account_id=? AND type='EXPENSE' AND voided=0", (account_id,)).fetchone()[0]
    tin = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE transfer_account_id=? AND type='TRANSFER' AND voided=0", (account_id,)).fetchone()[0]
    tout = conn.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE account_id=? AND type='TRANSFER' AND voided=0", (account_id,)).fetchone()[0]
    return float(opening + inc - exp + tin - tout)

def balances_summary():
    with db() as conn:
        accs = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
        per = []
        total = 0.0
        for a in accs:
            bal = account_balance(conn, a["id"])
            per.append({"id": a["id"], "name": a["name"], "type": a["type"], "currency": a["currency"], "balance": bal})
            total += bal
        df = pd.DataFrame(per)
        return total, df

def month_bounds(d: date):
    start = d.replace(day=1)
    if d.month == 12:
        end = d.replace(year=d.year+1, month=1, day=1)
    else:
        end = d.replace(month=d.month+1, day=1)
    return start, end

def transactions_df(filters=None):
    filters = filters or {}
    q = "SELECT * FROM transactions WHERE voided=0"
    params = []
    if filters.get("type"):
        q += " AND type=?"; params.append(filters["type"])
    if filters.get("account_id"):
        q += " AND account_id=?"; params.append(filters["account_id"])
    if filters.get("category"):
        q += " AND category=?"; params.append(filters["category"])
    if filters.get("date_from"):
        q += " AND booked_at>=?"; params.append(filters["date_from"])
    if filters.get("date_to"):
        q += " AND booked_at<?"; params.append(filters["date_to"])
    q += " ORDER BY booked_at DESC, id DESC"
    with db() as conn:
        return pd.read_sql_query(q, conn, params=params)

def add_transaction(ttype, account_id, amount, category, merchant, memo, booked_at, transfer_account_id):
    with db() as conn:
        conn.execute("""
            INSERT INTO transactions(account_id,type,amount,category,merchant,memo,booked_at,transfer_account_id)
            VALUES (?,?,?,?,?,?,?,?)
        """, (account_id, ttype, amount, category, merchant, memo, booked_at, transfer_account_id))

def rollback_transaction(tx_id: int):
    with db() as conn:
        # Mark transaction as voided; balances will exclude it
        conn.execute("UPDATE transactions SET voided=1, voided_at=? WHERE id=? AND voided=0", (datetime.utcnow().isoformat(), tx_id))

def add_account(name, atype, currency, opening):
    with db() as conn:
        conn.execute("INSERT INTO accounts(name,type,currency,opening_balance) VALUES (?,?,?,?)",
                     (name, atype, currency, opening))

def add_budget(month, year, category, limit_amount):
    with db() as conn:
        conn.execute("INSERT INTO budgets(month,year,category,limit_amount) VALUES (?,?,?,?)",
                     (month, year, category, limit_amount))

def budgets_progress(month: int, year: int):
    with db() as conn:
        rows = conn.execute("SELECT * FROM budgets WHERE month=? AND year=?", (month, year)).fetchall()
        data = []
        mstart = date(year, month, 1)
        mstart, mend = month_bounds(mstart)
        for b in rows:
            spent = conn.execute("""
                SELECT COALESCE(SUM(amount),0) FROM transactions
                WHERE type='EXPENSE' AND voided=0 AND category=? AND booked_at>=? AND booked_at<?
            """, (b["category"], mstart.isoformat(), mend.isoformat())).fetchone()[0]
            data.append({
                "category": b["category"],
                "limit": float(b["limit_amount"]),
                "spent": float(spent),
                "remaining": float(b["limit_amount"] - spent)
            })
        return pd.DataFrame(data)

def export_transactions_csv(include_voided=False) -> bytes:
    df = transactions_df()
    if include_voided:
        with db() as conn:
            df = pd.read_sql_query("SELECT * FROM transactions ORDER BY booked_at DESC, id DESC", conn)
    return df.to_csv(index=False).encode("utf-8")

def get_account_maps():
    """Returns (id_to_name, name_to_id)."""
    df = get_accounts_df()
    id_to_name = dict(zip(df["id"], df["name"])) if not df.empty else {}
    name_to_id = {v: k for k, v in id_to_name.items()}
    return id_to_name, name_to_id

def update_transaction(tx_id: int, **fields):
    """Dynamically update a transaction row by id. Ignores unknown/None fields."""
    allowed = {"account_id","type","amount","category","merchant","memo","booked_at","transfer_account_id","voided","voided_at"}
    sets, params = [], []
    for k, v in fields.items():
        if k in allowed and v is not None:
            sets.append(f"{k}=?")
            params.append(v)
    if not sets:
        return False
    params.append(tx_id)
    with db() as conn:
        conn.execute(f"UPDATE transactions SET {', '.join(sets)} WHERE id=?", params)
    return True

# ---------------- UI (no custom CSS) ----------------
init_db()
st.title("💸 Cashflow")

with st.sidebar:
    st.header("Navigation")
    page = st.radio(
    "Go to",
    ["Dashboard", "Accounts", "Transactions", "Budgets", "Reports / Export", "Logs (Edit)"],
    label_visibility="collapsed"
)

    st.caption("Tips: Use the Transactions tab to add entries or rollback mistakes.")

if page == "Dashboard":
    st.subheader("Overview")
    total, per_df = balances_summary()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Balance", f"{total:,.2f}")
    # month metrics
    today = date.today()
    mstart, mend = month_bounds(today)
    df_exp_month = transactions_df({"type": "EXPENSE", "date_from": mstart.isoformat(), "date_to": mend.isoformat()})
    df_inc_month = transactions_df({"type": "INCOME", "date_from": mstart.isoformat(), "date_to": mend.isoformat()})
    spent = float(df_exp_month["amount"].sum()) if not df_exp_month.empty else 0.0
    income = float(df_inc_month["amount"].sum()) if not df_inc_month.empty else 0.0
    col2.metric("Spent (this month)", f"{spent:,.2f}")
    col3.metric("Income (this month)", f"{income:,.2f}")
    st.divider()

    st.subheader("Accounts")
    if not per_df.empty:
        st.dataframe(per_df[["name","type","currency","balance"]], use_container_width=True)
    else:
        st.info("No accounts yet. Add one from the Accounts tab.")

    st.subheader("Top Categories (this month)")
    if not df_exp_month.empty:
        top = df_exp_month.groupby(df_exp_month["category"].fillna("Uncategorized"))["amount"].sum().sort_values(ascending=False).head(5)
        st.bar_chart(top)
    else:
        st.info("No expenses this month yet.")

elif page == "Accounts":
    st.subheader("Add Account")
    with st.form("add_account"):
        c1,c2,c3,c4 = st.columns(4)
        name = c1.text_input("Name")
        atype = c2.selectbox("Type", ["BANK","WALLET","STASH","CREDIT","OTHER"])
        currency = c3.text_input("Currency", "USD")
        opening = c4.number_input("Opening balance", value=0.0, step=0.01, format="%.2f")
        submitted = st.form_submit_button("Add account")
        if submitted:
            if not name:
                st.error("Name is required")
            else:
                add_account(name, atype, currency, float(opening))
                st.success("Account added")
    st.divider()
    accs = get_accounts_df()
    if not accs.empty:
        with db() as conn:
            balances = [account_balance(conn, int(aid)) for aid in accs["id"].tolist()]
        accs = accs.assign(balance=balances)
        st.subheader("All Accounts")
        st.dataframe(accs[["id","name","type","currency","opening_balance","balance"]], use_container_width=True)
    else:
        st.info("No accounts yet.")

elif page == "Transactions":
    st.subheader("Add Transaction")
    accs = get_accounts_df()
    if accs.empty:
        st.warning("Create an account first in the Accounts page.")
    else:
        with st.form("add_tx"):
            c1,c2,c3 = st.columns(3)
            ttype = c1.selectbox("Type", ["EXPENSE","INCOME","TRANSFER"])
            account_id = c2.selectbox("Account", accs["name"], index=0)
            account_id_val = int(accs.set_index("name").loc[account_id, "id"])
            amount = c3.number_input("Amount", value=0.0, step=0.01, format="%.2f")
            c4,c5,c6 = st.columns(3)
            category = c4.text_input("Category", value="")
            merchant = c5.text_input("Merchant", value="")
            memo = c6.text_input("Memo", value="")
            c7,c8 = st.columns(2)
            booked_at = c7.date_input("Date", value=date.today()).isoformat()
            transfer_to = None
            if ttype == "TRANSFER":
                to_name = c8.selectbox("Transfer to", accs["name"])
                transfer_to = int(accs.set_index("name").loc[to_name, "id"])
            submitted = st.form_submit_button("Add")
            if submitted:
                if ttype == "TRANSFER" and transfer_to is None:
                    st.error("Transfer requires a target account")
                else:
                    add_transaction(ttype, account_id_val, float(amount), category or None, merchant or None, memo or None, booked_at, transfer_to)
                    st.success("Transaction added")

    st.divider()
    st.subheader("Browse / Filter / Rollback")
    with st.expander("Filters", expanded=True):
        fc1, fc2, fc3, fc4, fc5 = st.columns(5)
        f_type = fc1.selectbox("Type", ["", "EXPENSE","INCOME","TRANSFER"], index=0)
        f_acc = fc2.selectbox("Account", [""] + accs["name"].tolist()) if not accs.empty else ""
        f_acc_id = int(accs.set_index("name").loc[f_acc, "id"]) if f_acc and not accs.empty else None
        f_cat = fc3.text_input("Category equals")
        f_from = fc4.date_input("From", value=None)
        f_to = fc5.date_input("To", value=None)
        filters = {}
        if f_type: filters["type"] = f_type
        if f_acc_id: filters["account_id"] = f_acc_id
        if f_cat: filters["category"] = f_cat
        if f_from: filters["date_from"] = f_from.isoformat()
        if f_to: filters["date_to"] = f_to.isoformat()

    df = transactions_df(filters)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        ids = df["id"].tolist()
        st.write("Select a transaction ID to rollback:")
        sel_id = st.selectbox("Transaction ID", ids)
        colA, colB = st.columns([1,3])
        if colA.button("Rollback selected"):
            rollback_transaction(int(sel_id))
            st.success(f"Transaction {sel_id} rolled back.")
        with st.expander("Rollback multiple"):
            multi = st.multiselect("Choose IDs", ids, default=[])
            if st.button("Rollback selected IDs"):
                for tid in multi:
                    rollback_transaction(int(tid))
                st.success(f"Rolled back {len(multi)} transactions.")
    else:
        st.info("No transactions found for the selected filters.")

    st.caption("Rollback marks a transaction as voided so it no longer affects balances.")

elif page == "Budgets":
    st.subheader("Budgets")
    today = date.today()
    with st.form("add_budget"):
        c1,c2,c3,c4 = st.columns(4)
        month = c1.number_input("Month", min_value=1, max_value=12, value=today.month)
        year = c2.number_input("Year", min_value=2000, max_value=2100, value=today.year)
        category = c3.text_input("Category")
        limit_amount = c4.number_input("Limit", value=0.0, step=0.01, format="%.2f")
        submitted = st.form_submit_button("Add budget")
        if submitted:
            if not category:
                st.error("Category is required")
            else:
                add_budget(int(month), int(year), category, float(limit_amount))
                st.success("Budget added")
    prog = budgets_progress(today.month, today.year)
    if not prog.empty:
        st.subheader(f"Progress — {today.strftime('%B %Y')}")
        st.dataframe(prog, use_container_width=True)
        st.bar_chart(prog.set_index("category")[["spent","limit"]])
    else:
        st.info("No budgets for this month yet.")

elif page == "Reports / Export":
    st.subheader("Transactions Export")
    df_all = transactions_df()
    if not df_all.empty:
        st.dataframe(df_all, use_container_width=True)
        st.download_button("Export all (CSV)", export_transactions_csv(), file_name="transactions_all.csv", mime="text/csv")
        st.download_button("Export including voided (CSV)", export_transactions_csv(include_voided=True), file_name="transactions_with_voided.csv", mime="text/csv")
    else:
        st.info("No transactions recorded yet.")

elif page == "Logs (Edit)":
    st.subheader("Transactions — View & Edit")
    st.caption("Tip: You can edit amount, type, account, category, merchant, memo, date, and transfer target. Use the Save button to persist.")

    accs = get_accounts_df()
    if accs.empty:
        st.warning("Create an account first in the Accounts page.")
    else:
        id_to_name, name_to_id = get_account_maps()

        # Load ALL transactions (including voided if you want—toggle below)
        include_voided = st.checkbox("Show voided", value=False)
        base_filters = {}
        df_all = transactions_df(base_filters)
        if include_voided:
            with db() as conn:
                df_all = pd.read_sql_query("SELECT * FROM transactions ORDER BY booked_at DESC, id DESC", conn)

        if df_all.empty:
            st.info("No transactions yet.")
        else:
            # Present friendly columns for editing
            edit_df = df_all.copy()

            # Map account ids to names for selection
            edit_df["account"] = edit_df["account_id"].map(id_to_name).fillna("")
            edit_df["transfer_to"] = edit_df["transfer_account_id"].map(id_to_name).fillna("")
            # Show booked_at as date for nicer editing; keep original in hidden column
            # (Streamlit will keep string if left alone; we'll coerce on save)
            # Keep an immutable id column visible
            edit_df = edit_df[[
                "id","type","amount","account","transfer_to","category","merchant","memo","booked_at","voided"
            ]]

            # Configure editor widgets
            type_options = ["EXPENSE","INCOME","TRANSFER"]
            account_options = accs["name"].tolist()

            edited = st.data_editor(
                edit_df,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "id": st.column_config.NumberColumn("ID", disabled=True),
                    "type": st.column_config.SelectboxColumn("Type", options=type_options),
                    "amount": st.column_config.NumberColumn("Amount", step=0.01, format="%.2f"),
                    "account": st.column_config.SelectboxColumn("Account", options=account_options),
                    "transfer_to": st.column_config.SelectboxColumn("Transfer to", options=[""] + account_options, help="Required when Type=TRANSFER; blank otherwise."),
                    "category": st.column_config.TextColumn("Category"),
                    "merchant": st.column_config.TextColumn("Merchant"),
                    "memo": st.column_config.TextColumn("Memo"),
                    "booked_at": st.column_config.TextColumn("Date (YYYY-MM-DD)", help="YYYY-MM-DD"),
                    "voided": st.column_config.CheckboxColumn("Voided")
                },
                hide_index=True,
                key="editor_transactions"
            )

            # Save changes
            if st.button("Save changes", type="primary"):
                updates = 0
                errors = []
                # Compare row-by-row by ID
                orig_by_id = {int(r["id"]): r for _, r in df_all.iterrows()}
                for _, row in edited.iterrows():
                    try:
                        tx_id = int(row["id"])
                        orig = orig_by_id.get(tx_id, None)
                        if orig is None:
                            # New row (created via data_editor). Minimal create path: we’ll insert.
                            # If you don’t want row-creation here, skip/collect error instead.
                            ttype = str(row["type"]).strip() if row["type"] else "EXPENSE"
                            acct_name = str(row["account"]).strip()
                            if not acct_name:
                                errors.append(f"Row with (new) id=? missing Account.")
                                continue
                            account_id = name_to_id.get(acct_name)
                            if account_id is None:
                                errors.append(f"Unknown account '{acct_name}' for new row.")
                                continue
                            amt = float(row["amount"] or 0.0)
                            cat = (row["category"] or None)
                            mer = (row["merchant"] or None)
                            memo = (row["memo"] or None)
                            date_str = (row["booked_at"] or date.today().isoformat())
                            # Transfer handling
                            transfer_to = None
                            if ttype == "TRANSFER":
                                tname = str(row.get("transfer_to") or "").strip()
                                if not tname:
                                    errors.append(f"New row: TRANSFER requires 'Transfer to'.")
                                    continue
                                transfer_to = name_to_id.get(tname)
                                if not transfer_to or transfer_to == account_id:
                                    errors.append(f"New row: invalid transfer target.")
                                    continue
                            add_transaction(ttype, account_id, amt, cat, mer, memo, date_str, transfer_to)
                            updates += 1
                            continue

                        # Build set of changed fields
                        changes = {}

                        # Type
                        new_type = str(row["type"]).strip()
                        if new_type not in ("EXPENSE","INCOME","TRANSFER"):
                            errors.append(f"Tx {tx_id}: invalid type '{new_type}'.")
                            continue
                        if new_type != orig["type"]:
                            changes["type"] = new_type

                        # Amount
                        new_amt = float(row["amount"])
                        if float(orig["amount"]) != new_amt:
                            changes["amount"] = new_amt

                        # Account
                        acct_name = str(row["account"]).strip()
                        account_id = name_to_id.get(acct_name)
                        if account_id is None:
                            errors.append(f"Tx {tx_id}: unknown account '{acct_name}'.")
                            continue
                        if int(orig["account_id"]) != int(account_id):
                            changes["account_id"] = int(account_id)

                        # Transfer target
                        tname = str(row["transfer_to"] or "").strip()
                        transfer_id = None
                        if tname:
                            transfer_id = name_to_id.get(tname)
                            if transfer_id is None:
                                errors.append(f"Tx {tx_id}: unknown transfer account '{tname}'.")
                                continue

                        # Validate type vs transfer target
                        if new_type == "TRANSFER":
                            if not transfer_id:
                                errors.append(f"Tx {tx_id}: TRANSFER requires 'Transfer to'.")
                                continue
                            if transfer_id == account_id:
                                errors.append(f"Tx {tx_id}: transfer target cannot equal source account.")
                                continue
                        else:
                            transfer_id = None  # normalize

                        # Persist transfer target if changed
                        orig_transfer = orig["transfer_account_id"]
                        orig_transfer = int(orig_transfer) if pd.notna(orig_transfer) else None
                        if transfer_id != orig_transfer:
                            changes["transfer_account_id"] = transfer_id

                        # Category / merchant / memo
                        for col in ["category","merchant","memo"]:
                            new_val = row[col] if row[col] != "" else None
                            orig_val = orig[col] if pd.notna(orig[col]) else None
                            if new_val != orig_val:
                                changes[col] = new_val

                        # Date
                        date_str = str(row["booked_at"]).strip() if row["booked_at"] else None
                        if not date_str:
                            errors.append(f"Tx {tx_id}: Date is required (YYYY-MM-DD).")
                            continue
                        # quick sanity check
                        try:
                            _ = datetime.fromisoformat(date_str)
                        except Exception:
                            # allow YYYY-MM-DD without time
                            try:
                                _ = datetime.strptime(date_str, "%Y-%m-%d")
                            except Exception:
                                errors.append(f"Tx {tx_id}: invalid date format '{date_str}'. Use YYYY-MM-DD.")
                                continue
                        if str(orig["booked_at"]) != date_str:
                            changes["booked_at"] = date_str

                        # Voided toggle
                        new_voided = bool(row["voided"])
                        orig_voided = bool(orig["voided"])
                        if new_voided != orig_voided:
                            changes["voided"] = 1 if new_voided else 0
                            changes["voided_at"] = datetime.utcnow().isoformat() if new_voided else None

                        if changes:
                            ok = update_transaction(tx_id, **changes)
                            if ok:
                                updates += 1
                        # else: no changes for this row
                    except Exception as e:
                        errors.append(f"Tx {row.get('id','?')}: {e}")

                if updates:
                    st.success(f"Saved {updates} change(s).")
                else:
                    st.info("No changes to save.")

                if errors:
                    with st.expander("Some rows had issues (details)"):
                        for e in errors:
                            st.error(e)

