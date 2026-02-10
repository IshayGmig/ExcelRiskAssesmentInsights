#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path

KEY = "src.vs.platform.reactivestorage.browser.reactiveStorageServiceImpl.persistentStorage.applicationUser"

def backup_db(db_path: Path) -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    b = db_path.with_name(db_path.name + f".bak-{ts}")
    shutil.copy2(db_path, b)
    return b

def connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path), timeout=30)
    con.execute("PRAGMA busy_timeout = 30000;")
    return con

def begin_immediate(con: sqlite3.Connection, tries: int = 80, sleep_s: float = 0.25) -> None:
    for _ in range(tries):
        try:
            con.execute("BEGIN IMMEDIATE;")
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower():
                raise
            time.sleep(sleep_s)
    raise sqlite3.OperationalError("database is locked (after retries)")

def get_blob(con: sqlite3.Connection) -> str:
    cur = con.cursor()
    cur.execute("SELECT value FROM ItemTable WHERE key=?", (KEY,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Key not found: {KEY}")
    return row[0]

def set_blob(con: sqlite3.Connection, new_value: str) -> None:
    cur = con.cursor()
    cur.execute("UPDATE ItemTable SET value=? WHERE key=?", (new_value, KEY))
    if cur.rowcount != 1:
        raise RuntimeError(f"UPDATE affected {cur.rowcount} rows (expected 1)")

def unique_preserve(xs):
    seen=set(); out=[]
    for x in xs:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def extract_state(raw: str):
    d = json.loads(raw)
    cs = d.get("composerState", {})
    return cs.get("yoloCommandAllowlist"), cs.get("yoloDeleteFileDisabled"), d

def patch(raw: str, add_cmds, deletion_protection):
    allow, delprot, d = extract_state(raw)
    cs = d.get("composerState")
    if cs is None or not isinstance(cs, dict):
        cs = {}
        d["composerState"] = cs

    allow = cs.get("yoloCommandAllowlist", [])
    if allow is None:
        allow = []
    if not isinstance(allow, list) or any(not isinstance(x, str) for x in allow):
        raise RuntimeError("yoloCommandAllowlist is not list[str]")

    if add_cmds:
        cs["yoloCommandAllowlist"] = unique_preserve(allow + add_cmds)

    if deletion_protection is not None:
        cs["yoloDeleteFileDisabled"] = bool(deletion_protection)

    return json.dumps(d, ensure_ascii=False, separators=(",", ":"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--add", nargs="*", default=[])
    ap.add_argument("--enable-file-deletion-protection", action="store_true")
    ap.add_argument("--disable-file-deletion-protection", action="store_true")
    ap.add_argument("--db", default=r"%APPDATA%\Cursor\User\globalStorage\state.vscdb")
    args = ap.parse_args()

    if args.enable_file_deletion_protection and args.disable_file_deletion_protection:
        print("Choose only one of enable/disable deletion protection")
        return 2

    deletion_protection = None
    if args.enable_file_deletion_protection:
        deletion_protection = True
    elif args.disable_file_deletion_protection:
        deletion_protection = False

    db_path = Path(os.path.expandvars(args.db))
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 2

    print(f"DB: {db_path}")

    con = connect(db_path)
    try:
        raw_before = get_blob(con)
        allow_before, del_before, _ = extract_state(raw_before)
        print("BEFORE allowlist:", allow_before)
        print("BEFORE delete_protect:", del_before)

        new_raw = patch(raw_before, args.add, deletion_protection)

        # Backup then write
        backup = backup_db(db_path)
        print("Backup:", backup)

        begin_immediate(con)
        set_blob(con, new_raw)
        con.commit()

        # Re-read immediately (this is the truth)
        raw_after = get_blob(con)
        allow_after, del_after, _ = extract_state(raw_after)
        print("AFTER allowlist:", allow_after)
        print("AFTER delete_protect:", del_after)

        # Assert it stuck
        expected_allow = unique_preserve((allow_before or []) + args.add) if args.add else (allow_before or [])
        expected_del = del_before if deletion_protection is None else deletion_protection

        ok = (allow_after == expected_allow) and (del_after == expected_del)
        if not ok:
            print("FAIL: changes did not persist exactly as expected.")
            return 1

        print("OK: changes persisted in globalStorage.")
        return 0

    finally:
        con.close()

if __name__ == "__main__":
    raise SystemExit(main())
