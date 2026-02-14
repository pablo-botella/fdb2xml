#!/usr/bin/env python3
"""
fdb2xml - Generic Firebird .FDB to flat XML.

Usage:
    python fdb2xml.py <database.fdb> [-o outdir]

Uses Firebird Embedded (fbembed.dll / fbclient.dll) from runtime/ folder.

Output:
    <name>.xml - Exact flat XML representation of the database
"""

import argparse
import base64
import datetime
import decimal
import os
import sys
import xml.etree.ElementTree as ET

import fdb


# ---------------------------------------------------------------------------
# Firebird native type mapping
# ---------------------------------------------------------------------------

def get_fb_type(field_type, field_sub_type, field_length, field_precision, field_scale,
                char_length=None):
    field_sub_type = field_sub_type or 0
    field_precision = field_precision or 0
    field_scale = field_scale or 0

    if field_type in (7, 8, 16) and field_sub_type in (1, 2):
        t = "NUMERIC" if field_sub_type == 1 else "DECIMAL"
        return f"{t}({field_precision},{-field_scale})"
    if field_type == 7:
        return "SMALLINT"
    if field_type == 8:
        return "INTEGER"
    if field_type == 16:
        return "BIGINT"
    if field_type == 10:
        return "FLOAT"
    if field_type == 27:
        return "DOUBLE PRECISION"
    if field_type == 12:
        return "DATE"
    if field_type == 13:
        return "TIME"
    if field_type == 35:
        return "TIMESTAMP"
    if field_type == 14:
        n = char_length if char_length else field_length
        return f"CHAR({n})"
    if field_type in (37, 40):
        n = char_length if char_length else field_length
        return f"VARCHAR({n})"
    if field_type == 261:
        if field_sub_type == 0:
            return "BLOB SUB_TYPE BINARY"
        if field_sub_type == 1:
            return "BLOB SUB_TYPE TEXT"
        return f"BLOB SUB_TYPE {field_sub_type}"
    return f"VARCHAR(255) /* unknown fb type {field_type} */"


# ---------------------------------------------------------------------------
# XML value formatting
# ---------------------------------------------------------------------------

def safe_str(val):
    if val is None:
        return ""
    if isinstance(val, datetime.datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, datetime.date):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, datetime.time):
        return val.strftime("%H:%M:%S")
    if isinstance(val, decimal.Decimal):
        return str(val)
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8").rstrip()
        except UnicodeDecodeError:
            return base64.b64encode(val).decode("ascii")
    if isinstance(val, str):
        return val.rstrip()
    return str(val)


# ---------------------------------------------------------------------------
# Database metadata
# ---------------------------------------------------------------------------

def get_user_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(RDB$RELATION_NAME)
        FROM RDB$RELATIONS
        WHERE RDB$SYSTEM_FLAG = 0
          AND RDB$VIEW_BLR IS NULL
        ORDER BY RDB$RELATION_NAME
    """)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables


def get_table_columns(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            TRIM(rf.RDB$FIELD_NAME),
            f.RDB$FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE,
            f.RDB$FIELD_LENGTH,
            f.RDB$FIELD_PRECISION,
            f.RDB$FIELD_SCALE,
            rf.RDB$NULL_FLAG,
            f.RDB$CHARACTER_LENGTH
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        WHERE rf.RDB$RELATION_NAME = ?
        ORDER BY rf.RDB$FIELD_POSITION
    """, (table_name,))
    columns = []
    for name, ftype, fsub, flen, fprec, fscale, null_flag, char_len in cur.fetchall():
        columns.append({
            "name": name,
            "fb_type": get_fb_type(ftype, fsub, flen, fprec, fscale, char_len),
            "not_null": null_flag is not None and null_flag == 1,
        })
    cur.close()
    return columns


def get_primary_key(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(sg.RDB$FIELD_NAME)
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$INDEX_SEGMENTS sg ON rc.RDB$INDEX_NAME = sg.RDB$INDEX_NAME
        WHERE rc.RDB$RELATION_NAME = ?
          AND rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY sg.RDB$FIELD_POSITION
    """, (table_name,))
    pk = [row[0] for row in cur.fetchall()]
    cur.close()
    return pk


def get_foreign_keys(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            TRIM(rc.RDB$CONSTRAINT_NAME),
            TRIM(sg.RDB$FIELD_NAME),
            TRIM(rc2.RDB$RELATION_NAME),
            TRIM(sg2.RDB$FIELD_NAME)
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$INDEX_SEGMENTS sg ON rc.RDB$INDEX_NAME = sg.RDB$INDEX_NAME
        JOIN RDB$REF_CONSTRAINTS ref ON rc.RDB$CONSTRAINT_NAME = ref.RDB$CONSTRAINT_NAME
        JOIN RDB$RELATION_CONSTRAINTS rc2 ON ref.RDB$CONST_NAME_UQ = rc2.RDB$CONSTRAINT_NAME
        JOIN RDB$INDEX_SEGMENTS sg2 ON rc2.RDB$INDEX_NAME = sg2.RDB$INDEX_NAME
        WHERE rc.RDB$RELATION_NAME = ?
          AND rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
    """, (table_name,))
    fks = [{"name": r[0], "column": r[1], "ref_table": r[2], "ref_column": r[3]}
           for r in cur.fetchall()]
    cur.close()
    return fks


def get_generators(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(RDB$GENERATOR_NAME)
        FROM RDB$GENERATORS
        WHERE RDB$SYSTEM_FLAG = 0
        ORDER BY RDB$GENERATOR_NAME
    """)
    generators = []
    for (name,) in cur.fetchall():
        cur2 = conn.cursor()
        cur2.execute(f'SELECT GEN_ID("{name}", 0) FROM RDB$DATABASE')
        val = cur2.fetchone()[0]
        cur2.close()
        generators.append({"name": name, "value": val})
    cur.close()
    return generators


# ---------------------------------------------------------------------------
# Embedded connection
# ---------------------------------------------------------------------------

def connect_embedded(fdb_path):
    fdb_path = os.path.abspath(fdb_path)
    if getattr(sys, 'frozen', False):
        exe_dir = os.path.dirname(os.path.abspath(sys.executable))
    else:
        exe_dir = os.path.dirname(os.path.abspath(__file__))
    fdb_dir = os.path.dirname(fdb_path)

    search_dirs = [
        os.path.join(exe_dir, "runtime"),
        os.getcwd(),
        exe_dir,
        fdb_dir,
        os.path.join(os.environ.get("ProgramFiles", ""), "Firebird", "Firebird_2_5", "bin"),
        os.path.join(os.environ.get("ProgramFiles", ""), "Firebird", "Firebird_3_0"),
        os.path.join(os.environ.get("ProgramFiles(x86)", ""), "Firebird", "Firebird_2_5", "bin"),
    ]

    for d in search_dirs:
        if not d or not os.path.isdir(d):
            continue
        for dll in ("fbembed.dll", "fbclient.dll"):
            path = os.path.join(d, dll)
            if os.path.exists(path):
                print(f"  Trying: {path}")
                try:
                    return fdb.connect(
                        database=fdb_path,
                        user="sysdba",
                        password="masterkey",
                        fb_library_name=path,
                    )
                except Exception as e:
                    print(f"    Failed: {e}")

    print("ERROR: No fbembed.dll / fbclient.dll found.")
    print("  Searched: " + ", ".join(d for d in search_dirs if d))
    sys.exit(1)


# ---------------------------------------------------------------------------
# Read all data
# ---------------------------------------------------------------------------

def read_all_tables(conn):
    tables = get_user_tables(conn)
    print(f"  {len(tables)} tables: {', '.join(tables)}")

    data = {}
    for table in tables:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM "{table}"')
        col_names = [desc[0] for desc in cur.description]
        rows = []
        for raw in cur.fetchall():
            row = {}
            for i, col in enumerate(col_names):
                val = raw[i]
                if isinstance(val, str):
                    val = val.rstrip()
                row[col] = val
            rows.append(row)
        cur.close()
        data[table] = rows
        print(f"    {table}: {len(rows)} rows")

    return data


# ---------------------------------------------------------------------------
# XML generation
# ---------------------------------------------------------------------------

def xml_col(row_el, name, fb_type, val):
    attrs = {"name": name, "type": fb_type}

    if val is None:
        attrs["null"] = "true"
        ET.SubElement(row_el, "col", **attrs)
        return

    if isinstance(val, bytes):
        try:
            text = val.decode("utf-8").rstrip()
        except UnicodeDecodeError:
            attrs["enc"] = "base64"
            text = base64.b64encode(val).decode("ascii")
        col_el = ET.SubElement(row_el, "col", **attrs)
        col_el.text = text
        return

    col_el = ET.SubElement(row_el, "col", **attrs)
    col_el.text = safe_str(val)


def generate_xml(conn, data, fdb_path, output_path):
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    source = os.path.basename(fdb_path)

    col_meta = {}
    for table_name in data:
        col_meta[table_name] = get_table_columns(conn, table_name)

    root = ET.Element("database", source=source, exported=now)

    # Schema: generators, tables with columns, PKs, FKs
    schema_el = ET.SubElement(root, "schema")

    generators = get_generators(conn)
    if generators:
        gens_el = ET.SubElement(schema_el, "generators")
        for gen in generators:
            ET.SubElement(gens_el, "generator", name=gen["name"], value=str(gen["value"]))

    for table_name in data:
        columns = col_meta[table_name]
        pk_cols = get_primary_key(conn, table_name)
        fks = get_foreign_keys(conn, table_name)

        table_schema = ET.SubElement(schema_el, "table", name=table_name)

        for col in columns:
            attrs = {"name": col["name"], "type": col["fb_type"]}
            if col["not_null"]:
                attrs["notnull"] = "true"
            if col["name"] in pk_cols:
                attrs["pk"] = "true"
            ET.SubElement(table_schema, "column", **attrs)

        for fk in fks:
            ET.SubElement(table_schema, "fk",
                          name=fk["name"],
                          column=fk["column"],
                          references=f'{fk["ref_table"]}({fk["ref_column"]})')

    # Data
    data_el = ET.SubElement(root, "data")

    for table_name, rows in data.items():
        columns = col_meta[table_name]
        table_el = ET.SubElement(data_el, "table", name=table_name, count=str(len(rows)))
        for row in rows:
            row_el = ET.SubElement(table_el, "row")
            for col in columns:
                xml_col(row_el, col["name"], col["fb_type"], row.get(col["name"]))

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    with open(output_path, "wb") as f:
        tree.write(f, encoding="UTF-8", xml_declaration=True)

    total = sum(1 for _ in root.iter()) - 1
    print(f"  {total} elements -> {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generic Firebird .FDB to flat XML")
    parser.add_argument("fdb_path", help="Path to the .FDB file")
    parser.add_argument("-o", "--outdir", default=None, help="Output directory (default: same as FDB)")

    args = parser.parse_args()

    if not os.path.exists(args.fdb_path):
        print(f"ERROR: not found: {args.fdb_path}")
        sys.exit(1)

    fdb_path = os.path.abspath(args.fdb_path)
    base_name = os.path.splitext(os.path.basename(fdb_path))[0]

    if args.outdir:
        outdir = os.path.abspath(args.outdir)
        os.makedirs(outdir, exist_ok=True)
    else:
        outdir = os.path.dirname(fdb_path)

    xml_path = os.path.join(outdir, f"{base_name}.xml")

    print(f"fdb2xml - Firebird -> XML")
    print(f"  Input: {fdb_path}")
    print(f"  XML:   {xml_path}")
    print()

    print("Connecting...")
    conn = connect_embedded(fdb_path)
    print("Connected.\n")

    print("Reading tables...")
    data = read_all_tables(conn)
    print()

    print("Generating XML...")
    generate_xml(conn, data, fdb_path, xml_path)
    print()

    conn.close()

    size = os.path.getsize(xml_path)
    fmt = f"{size/1024:.1f} KB" if size < 1048576 else f"{size/1048576:.1f} MB"
    print(f"Done! {xml_path} ({fmt})")


if __name__ == "__main__":
    main()
