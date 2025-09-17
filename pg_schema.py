#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pg_ascii_schema.py — потоковый ASCII/JSON-обход структуры PostgreSQL.

Поддерживает:
- ASCII вывод (по умолчанию).
- JSON вывод (стриминг), опции --format json, --pretty, --output FILE.

Объекты:
- схемы
  - таблицы / представления / матвью / foreign tables
    - колонки (тип, NULLability, DEFAULT, имя функции из DEFAULT)
    - (опц.) индексы
    - (опц.) внешние ключи (outgoing / incoming)
    - (опц.) триггеры
  - (опц.) функции/процедуры

Зависимости: psycopg (v3).
Установка: pip install "psycopg[binary]"
"""

import argparse
import json
import re
import sys
from typing import Iterable, Optional, Tuple, List

import psycopg
from psycopg.rows import tuple_row

# ───────── ASCII helpers ─────────

def _branch(prefix: str, is_last: bool) -> str:
    return f"{prefix}{'└─ ' if is_last else '├─ '}"

def _child_prefix(prefix: str, is_last: bool) -> str:
    return f"{prefix}{'   ' if is_last else '│  '}"


# ───────── Extractors ─────────

FUNC_NAME_RE = re.compile(r"""(?ix)
    ^\s*(?:([a-z_][\w$]*)\.)?(?P<func>[a-z_][\w$]*)\s*\(
""")

def extract_default_func_name(default_expr: Optional[str]) -> Optional[str]:
    if not default_expr:
        return None
    m = FUNC_NAME_RE.match(default_expr)
    return m.group("func") if m else None


# ───────── Queries ─────────

Q_SCHEMAS = """
SELECT n.nspname
FROM pg_namespace n
WHERE n.nspname NOT LIKE 'pg\\_%'
  AND n.nspname <> 'information_schema'
ORDER BY 1
"""

def q_tables(include_views: bool, include_ft: bool, include_matviews: bool) -> str:
    kinds = ["'r'","'p'"]
    if include_views: kinds.append("'v'")
    if include_matviews: kinds.append("'m'")
    if include_ft: kinds.append("'f'")
    kinds_list = ",".join(kinds)
    return f"""
SELECT c.relname, c.relkind
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %(schema)s
  AND c.relkind IN ({kinds_list})
ORDER BY 1
"""

Q_REL_OID = """
SELECT c.oid
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %(schema)s AND c.relname = %(table)s
"""

Q_COLUMNS = """
SELECT a.attname,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
       a.attnotnull,
       pg_get_expr(ad.adbin, ad.adrelid) AS default_expr
FROM pg_attribute a
LEFT JOIN pg_attrdef ad
  ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
WHERE a.attrelid = %(relid)s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
"""

Q_FUNCTIONS = """
SELECT p.proname,
       pg_catalog.pg_get_function_identity_arguments(p.oid) AS args,
       pg_catalog.format_type(p.prorettype, NULL)          AS rettype
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = %(schema)s
  AND p.prokind IN ('f','p')  -- f=function, p=procedure
ORDER BY 1, 2
"""

Q_INDEXES = """
SELECT
  c2.relname               AS idxname,
  i.indisprimary,
  i.indisunique,
  NOT i.indisvalid         AS is_invalid,
  pg_get_indexdef(i.indexrelid) AS idxdef
FROM pg_index i
JOIN pg_class c  ON c.oid  = i.indrelid
JOIN pg_class c2 ON c2.oid = i.indexrelid
WHERE c.oid = %(relid)s
ORDER BY 1
"""

Q_FKEYS_OUT = """
SELECT conname,
       pg_get_constraintdef(oid, true) AS def,
       confrelid::regclass::text       AS ref_table
FROM pg_constraint
WHERE conrelid = %(relid)s AND contype = 'f'
ORDER BY 1
"""

Q_FKEYS_IN = """
SELECT conname,
       pg_get_constraintdef(oid, true) AS def,
       conrelid::regclass::text        AS src_table
FROM pg_constraint
WHERE confrelid = %(relid)s AND contype = 'f'
ORDER BY 1
"""

Q_TRIGGERS = """
SELECT t.tgname,
       pg_get_triggerdef(t.oid, true) AS tgdef,
       p.proname                      AS func_name
FROM pg_trigger t
LEFT JOIN pg_proc p ON p.oid = t.tgfoid
WHERE t.tgrelid = %(relid)s
  AND NOT t.tgisinternal
ORDER BY 1
"""

# ───────── Streaming iterators ─────────

def iter_schemas(conn, args) -> Iterable[str]:
    q = ("SELECT nspname FROM pg_namespace ORDER BY 1"
         if args.include_all_schemas else Q_SCHEMAS)
    with conn.cursor(name="schemas_cur", row_factory=tuple_row) as cur:
        cur.execute(q)
        for (nspname,) in cur:
            if args.schema_regex:
                if args.schema and not re.search(args.schema, nspname):
                    continue
            elif args.schema and nspname != args.schema:
                continue
            yield nspname

def iter_tables(conn, schema: str, args) -> Iterable[Tuple[str, str]]:
    q = q_tables(args.include_views, args.include_foreign, args.include_matviews)
    with conn.cursor(name=f"tables_{schema}", row_factory=tuple_row) as cur:
        cur.execute(q, {"schema": schema})
        for row in cur:
            yield row  # (relname, relkind)

def get_rel_oid(conn, schema: str, table: str) -> Optional[int]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(Q_REL_OID, {"schema": schema, "table": table})
        r = cur.fetchone()
        return r[0] if r else None

def fetch_columns(conn, rel_oid: int) -> Iterable[Tuple[str, str, bool, Optional[str]]]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(Q_COLUMNS, {"relid": rel_oid})
        yield from cur

def fetch_indexes(conn, rel_oid: int) -> Iterable[Tuple[str, bool, bool, bool, str]]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(Q_INDEXES, {"relid": rel_oid})
        yield from cur  # idxname, isPK, isUNIQ, isInvalid, idxdef

def fetch_fkeys_out(conn, rel_oid: int) -> Iterable[Tuple[str, str, str]]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(Q_FKEYS_OUT, {"relid": rel_oid})
        yield from cur  # conname, def, ref_table

def fetch_fkeys_in(conn, rel_oid: int) -> Iterable[Tuple[str, str, str]]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(Q_FKEYS_IN, {"relid": rel_oid})
        yield from cur  # conname, def, src_table

def fetch_triggers(conn, rel_oid: int) -> Iterable[Tuple[str, str, Optional[str]]]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(Q_TRIGGERS, {"relid": rel_oid})
        yield from cur  # tgname, tgdef, func_name


# ───────── Printing: ASCII ─────────

RELKIND_LABEL = {
    "r": "[table]",
    "p": "[part_table]",
    "v": "[view]",
    "m": "[matview]",
    "f": "[foreign]",
}

def print_schema_tree_ascii(conn, schema: str, args, out):
    print(schema, file=out)

    has_funcs = args.include_funcs
    table_count = sum(1 for _ in iter_tables(conn, schema, args))
    top_children = table_count + (1 if has_funcs else 0)

    idx_top = 0
    if has_funcs:
        is_last = (idx_top == top_children - 1) if table_count == 0 else False
        print(_branch("", is_last) + "functions", file=out)
        func_prefix = _child_prefix("", is_last)
        funcs = list(iter_functions(conn, schema))
        for i, (fname, fargs, rettype) in enumerate(funcs):
            last_f = (i == len(funcs) - 1)
            print(_branch(func_prefix, last_f) + f"{fname}({fargs}) -> {rettype}", file=out)
        idx_top += 1

    idx = 0
    for tname, relkind in iter_tables(conn, schema, args):
        is_last_table = (idx_top + idx == top_children - 1)
        label = RELKIND_LABEL.get(relkind, "[rel]")
        print(_branch("", is_last_table) + f"{tname} {label}", file=out)
        level1_prefix = _child_prefix("", is_last_table)

        rel_oid = get_rel_oid(conn, schema, tname)
        if rel_oid is None:
            print(_branch(level1_prefix, True) + "(rel not found)", file=out)
            idx += 1
            continue

        # 1) COLUMNS
        cols = list(fetch_columns(conn, rel_oid))
        more_groups = any([args.include_indexes, args.include_fkeys, args.include_triggers])
        print(_branch(level1_prefix, not more_groups and not cols) + "columns", file=out)
        col_prefix = _child_prefix(level1_prefix, not more_groups and not cols)
        for j, (cname, dtype, notnull, default_expr) in enumerate(cols):
            last_c = (j == len(cols) - 1)
            parts: List[str] = [f"{cname}: {dtype}"]
            if notnull:
                parts.append("NOT NULL")
            if default_expr:
                parts.append(f"DEFAULT {default_expr}")
                fn = extract_default_func_name(default_expr)
                if fn:
                    parts.append(f"[func: {fn}]")
            print(_branch(col_prefix, last_c) + " ".join(parts), file=out)

        # 2) Other groups (optional)
        enabled = []
        if args.include_indexes:
            idxs = list(fetch_indexes(conn, rel_oid))
            enabled.append(("indexes", idxs))
        if args.include_fkeys:
            fko = list(fetch_fkeys_out(conn, rel_oid))
            fki = list(fetch_fkeys_in(conn, rel_oid))
            enabled.append(("foreign_keys", (fko, fki)))
        if args.include_triggers:
            trgs = list(fetch_triggers(conn, rel_oid))
            enabled.append(("triggers", trgs))

        for g_idx, (gname, gdata) in enumerate(enabled):
            is_last_group = (g_idx == len(enabled) - 1)
            print(_branch(level1_prefix, is_last_group) + gname, file=out)
            g_prefix = _child_prefix(level1_prefix, is_last_group)

            if gname == "indexes":
                for k, (idxname, ispk, isuniq, is_invalid, idxdef) in enumerate(gdata):  # type: ignore
                    tags = []
                    if ispk: tags.append("PK")
                    if isuniq and not ispk: tags.append("UNIQ")
                    if is_invalid: tags.append("INVALID")
                    tag = f" [{'|'.join(tags)}]" if tags else ""
                    oneline = " ".join(idxdef.split())
                    print(_branch(g_prefix, k == len(gdata) - 1) + f"{idxname}{tag} :: {oneline}", file=out)  # type: ignore

            elif gname == "foreign_keys":
                fko, fki = gdata  # type: ignore
                print(_branch(g_prefix, False if fki else True) + "outgoing", file=out)
                out_prefix = _child_prefix(g_prefix, False if fki else True)
                if fko:
                    for kk, (conname, defn, ref_table) in enumerate(fko):
                        last = (kk == len(fko) - 1)
                        one = " ".join(defn.split())
                        print(_branch(out_prefix, last) + f"{conname} -> {ref_table} :: {one}", file=out)
                else:
                    print(_branch(out_prefix, True) + "(none)", file=out)

                if fki:
                    print(_branch(g_prefix, True) + "incoming", file=out)
                    in_prefix = _child_prefix(g_prefix, True)
                    for kk, (conname, defn, src_table) in enumerate(fki):
                        last = (kk == len(fki) - 1)
                        one = " ".join(defn.split())
                        print(_branch(in_prefix, last) + f"{conname} <- {src_table} :: {one}", file=out)
                else:
                    print(_branch(g_prefix, True) + "incoming", file=out)
                    in_prefix = _child_prefix(g_prefix, True)
                    print(_branch(in_prefix, True) + "(none)", file=out)

            elif gname == "triggers":
                trgs = gdata  # type: ignore
                if trgs:
                    for k, (tgname, tgdef, func_name) in enumerate(trgs):
                        last = (k == len(trgs) - 1)
                        one = " ".join(tgdef.split())
                        fn = f" [func: {func_name}]" if func_name else ""
                        print(_branch(g_prefix, last) + f"{tgname}{fn} :: {one}", file=out)
                else:
                    print(_branch(g_prefix, True) + "(none)", file=out)

        idx += 1


def iter_functions(conn, schema: str) -> Iterable[Tuple[str, str, str]]:
    with conn.cursor(name=f"funcs_{schema}", row_factory=tuple_row) as cur:
        cur.execute(Q_FUNCTIONS, {"schema": schema})
        for row in cur:
            yield row


# ───────── Printing: JSON (streamed) ─────────

def json_dump_min(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def json_dump_pretty(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2)

class JsonStream:
    """
    Мини-стример JSON: руками пишет структурные скобки и вставляет запятые между элементами.
    Не держит всё в памяти.
    """
    def __init__(self, out, pretty=False, indent=2):
        self.out = out
        self.pretty = pretty
        self.indent = indent
        self.level = 0
        self.first_stack = []  # отслеживаем, печатали ли 1й элемент в текущем контейнере

    # базовые helpers
    def _nl(self):
        if self.pretty: self.out.write("\n")

    def _pad(self):
        if self.pretty: self.out.write(" " * (self.level * self.indent))

    def begin_obj(self):
        self.out.write("{"); self.level += 1; self.first_stack.append(True)

    def end_obj(self):
        self.level -= 1; self._nl(); self._pad(); self.out.write("}"); self.first_stack.pop()

    def begin_array(self):
        self.out.write("["); self.level += 1; self.first_stack.append(True)

    def end_array(self):
        self.level -= 1; self._nl(); self._pad(); self.out.write("]"); self.first_stack.pop()

    def _comma_if_needed(self):
        if not self.first_stack:
            return
        if self.first_stack[-1]:
            self.first_stack[-1] = False
        else:
            self.out.write(",")

    def key(self, k: str):
        self._comma_if_needed()
        self._nl(); self._pad()
        self.out.write(json.dumps(k, ensure_ascii=False))
        self.out.write(":" if not self.pretty else ": ")

    def value(self, v):
        self._comma_if_needed()
        self._nl(); self._pad()
        self.out.write(json.dumps(v, ensure_ascii=False))

    def item(self, v):
        self._comma_if_needed()
        self._nl(); self._pad()
        self.out.write(json.dumps(v, ensure_ascii=False))

def print_schema_tree_json(conn, schema: str, args, js: JsonStream):
    # { "name": "...", "functions": [...], "relations": [...] }
    js.begin_obj()
    js.key("name"); js.value(schema)

    # functions
    if args.include_funcs:
        js.key("functions"); js.begin_array()
        first_any = False
        for (fname, fargs, rettype) in iter_functions(conn, schema):
            js.item({"name": fname, "args": fargs, "return_type": rettype})
            first_any = True
        js.end_array()
    # relations
    js.key("relations"); js.begin_array()
    for (tname, relkind) in iter_tables(conn, schema, args):
        rel_obj = {"name": tname, "kind": RELKIND_LABEL.get(relkind, "[rel]")}
        js.begin_obj()
        js.key("name"); js.value(tname)
        js.key("kind"); js.value(RELKIND_LABEL.get(relkind, "[rel]"))

        rel_oid = get_rel_oid(conn, schema, tname)
        if rel_oid is None:
            js.key("error"); js.value("rel not found")
            js.end_obj()
            continue

        # columns
        js.key("columns"); js.begin_array()
        for (cname, dtype, notnull, default_expr) in fetch_columns(conn, rel_oid):
            entry = {
                "name": cname,
                "type": dtype,
                "not_null": bool(notnull),
            }
            if default_expr:
                entry["default"] = default_expr
                fn = extract_default_func_name(default_expr)
                if fn: entry["default_func"] = fn
            js.item(entry)
        js.end_array()

        # indexes
        if args.include_indexes:
            js.key("indexes"); js.begin_array()
            for (idxname, ispk, isuniq, is_invalid, idxdef) in fetch_indexes(conn, rel_oid):
                js.item({
                    "name": idxname,
                    "primary": bool(ispk),
                    "unique": bool(isuniq),
                    "invalid": bool(is_invalid),
                    "definition": " ".join(idxdef.split()),
                })
            js.end_array()

        # foreign keys
        if args.include_fkeys:
            # outgoing
            js.key("foreign_keys"); js.begin_obj()

            js.key("outgoing"); js.begin_array()
            for (conname, defn, ref_table) in fetch_fkeys_out(conn, rel_oid):
                js.item({
                    "name": conname,
                    "ref_table": ref_table,
                    "definition": " ".join(defn.split()),
                })
            js.end_array()

            js.key("incoming"); js.begin_array()
            for (conname, defn, src_table) in fetch_fkeys_in(conn, rel_oid):
                js.item({
                    "name": conname,
                    "src_table": src_table,
                    "definition": " ".join(defn.split()),
                })
            js.end_array()

            js.end_obj()

        # triggers
        if args.include_triggers:
            js.key("triggers"); js.begin_array()
            for (tgname, tgdef, func_name) in fetch_triggers(conn, rel_oid):
                js.item({
                    "name": tgname,
                    "function": func_name,
                    "definition": " ".join(tgdef.split()),
                })
            js.end_array()

        js.end_obj()  # relation
    js.end_array()  # relations

    js.end_obj()  # schema


# ───────── CLI ─────────

def main():
    ap = argparse.ArgumentParser(description="Стриминговый ASCII/JSON-обход структуры PostgreSQL.")
    ap.add_argument("--dsn", required=True, help="DSN вида postgresql://user:pass@host:port/dbname")
    ap.add_argument("--schema", help="Имя схемы или regex (см. --schema-regex)")
    ap.add_argument("--schema-regex", action="store_true", help="Интерпретировать --schema как регулярное выражение")
    ap.add_argument("--include-views", action="store_true", help="Включать VIEW")
    ap.add_argument("--include-matviews", action="store_true", help="Включать MATERIALIZED VIEW")
    ap.add_argument("--include-foreign", action="store_true", help="Включать FOREIGN TABLE")
    ap.add_argument("--include-funcs", action="store_true", help="Печатать функции/процедуры схемы")
    ap.add_argument("--include-all-schemas", action="store_true", help="Включать системные схемы (pg_*, information_schema)")
    ap.add_argument("--include-indexes", action="store_true", help="Показывать индексы таблиц")
    ap.add_argument("--include-fkeys", action="store_true", help="Показывать внешние ключи (исходящие/входящие)")
    ap.add_argument("--include-triggers", action="store_true", help="Показывать триггеры")
    ap.add_argument("--statement-timeout-ms", type=int, default=0, help="SET LOCAL statement_timeout (мс); 0 — не задавать")
    ap.add_argument("--application-name", default="pg_ascii_schema", help="application_name для подключения")
    ap.add_argument("--output", help="Путь к файлу для сохранения результата (по умолчанию — stdout)")
    ap.add_argument("--format", choices=["ascii", "json"], default="ascii", help="Формат вывода")
    ap.add_argument("--pretty", action="store_true", help="Красивый JSON (отступы)")
    args = ap.parse_args()

    # Куда писать: файл или stdout
    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        with psycopg.connect(conninfo=args.dsn, autocommit=False, row_factory=tuple_row) as conn:
            # READ ONLY транзакция
            conn.execute("BEGIN READ ONLY")
            if args.statement_timeout_ms and args.statement_timeout_ms > 0:
                conn.execute(f"SET LOCAL statement_timeout = {int(args.statement_timeout_ms)}")

            try:
                schemas_iter = iter_schemas(conn, args)

                if args.format == "ascii":
                    first = True
                    for s_idx, schema in enumerate(schemas_iter):
                        if s_idx > 0:
                            print("", file=out)  # разделитель
                        print_schema_tree_ascii(conn, schema, args, out)
                else:
                    # JSON: { "schemas": [ ... ] }
                    js = JsonStream(out, pretty=args.pretty, indent=2)
                    js.begin_obj()
                    js.key("schemas"); js.begin_array()
                    any_schema = False
                    for schema in schemas_iter:
                        any_schema = True
                        print_schema_tree_json(conn, schema, args, js)
                    js.end_array()
                    js.end_obj()
                    if args.pretty:
                        out.write("\n")
            finally:
                conn.execute("COMMIT")
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.stderr.write("Interrupted by user.\n")
        sys.exit(130)
