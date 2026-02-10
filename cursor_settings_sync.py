

import argparse
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Tuple

STATE_KEY = "src.vs.platform.reactivestorage.browser.reactiveStorageServiceImpl.persistentStorage.applicationUser"


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


def backup_file(p: Path) -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    b = p.with_name(p.name + f".bak-{ts}")
    shutil.copy2(p, b)
    return b


def read_state_blob(con: sqlite3.Connection) -> str:
    cur = con.cursor()
    cur.execute("SELECT value FROM ItemTable WHERE key=?", (STATE_KEY,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Key not found in ItemTable: {STATE_KEY}")
    return row[0]


def write_state_blob(con: sqlite3.Connection, new_value: str) -> None:
    cur = con.cursor()
    cur.execute("UPDATE ItemTable SET value=? WHERE key=?", (new_value, STATE_KEY))
    if cur.rowcount != 1:
        raise RuntimeError(f"UPDATE affected {cur.rowcount} rows (expected 1)")


def load_state_json(raw: str) -> Dict[str, Any]:
    return json.loads(raw)


def dump_compact(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"))


def export_config(db_path: Path, out_path: Path) -> None:
    con = connect(db_path)
    try:
        raw = read_state_blob(con)
        state = load_state_json(raw)
        composer = state.get("composerState", {})
        if not isinstance(composer, dict):
            composer = {}
        out = {"composerState": composer}
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    finally:
        con.close()


def merge_apply(db_path: Path, cfg_path: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Merge cfg["composerState"] into state["composerState"].
    Only keys present in cfg are updated. Everything else stays untouched.
    Returns (before_subset, after_subset) for visibility.
    """
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    if "composerState" not in cfg or not isinstance(cfg["composerState"], dict):
        raise RuntimeError("Config must contain an object: { \"composerState\": { ... } }")

    con = connect(db_path)
    try:
        raw_before = read_state_blob(con)
        state = load_state_json(raw_before)

        cs = state.get("composerState")
        if cs is None or not isinstance(cs, dict):
            cs = {}
            state["composerState"] = cs

        before_subset = {k: cs.get(k) for k in cfg["composerState"].keys()}

        # merge only keys provided in config
        for k, v in cfg["composerState"].items():
            cs[k] = v

        after_subset = {k: cs.get(k) for k in cfg["composerState"].keys()}

        # write
        backup_file(db_path)
        begin_immediate(con)
        write_state_blob(con, dump_compact(state))
        con.commit()

        return before_subset, after_subset
    finally:
        con.close()


def watch_and_apply(db_path: Path, cfg_path: Path, interval_s: float) -> None:
    last_mtime = None
    print(f"Watching: {cfg_path}")
    print("Tip: keep Cursor closed when editing/applying to avoid it overwriting immediately.")
    while True:
        try:
            st = cfg_path.stat()
            mtime = st.st_mtime
            if last_mtime is None:
                last_mtime = mtime
            elif mtime != last_mtime:
                last_mtime = mtime
                before, after = merge_apply(db_path, cfg_path)
                print("\nApplied changes:")
                print("  BEFORE:", before)
                print("  AFTER :", after)
        except KeyboardInterrupt:
            print("\nStopped.")
            return
        except Exception as e:
            print("Error:", e)
        time.sleep(interval_s)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=r"%APPDATA%\Cursor\User\globalStorage\state.vscdb",
                    help="Path to Cursor globalStorage state.vscdb")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_exp = sub.add_parser("export", help="Export composerState to JSON")
    p_exp.add_argument("--out", default="cursor_autorun_config.json", help="Output JSON file")

    p_app = sub.add_parser("apply", help="Apply JSON to state.vscdb (merge)")
    p_app.add_argument("--config", default="cursor_autorun_config.json", help="Config JSON file")

    p_watch = sub.add_parser("watch", help="Watch JSON file and apply on changes")
    p_watch.add_argument("--config", default="cursor_autorun_config.json", help="Config JSON file")
    p_watch.add_argument("--interval", type=float, default=1.0, help="Polling interval seconds")

    args = ap.parse_args()
    db_path = Path(os.path.expandvars(args.db))

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    if args.cmd == "export":
        out_path = Path(args.out)
        export_config(db_path, out_path)
        print(f"Exported to: {out_path.resolve()}")
        return 0

    if args.cmd == "apply":
        cfg_path = Path(args.config)
        before, after = merge_apply(db_path, cfg_path)
        print("Applied.")
        print("BEFORE:", before)
        print("AFTER :", after)
        return 0

    if args.cmd == "watch":
        cfg_path = Path(args.config)
        watch_and_apply(db_path, cfg_path, args.interval)
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
