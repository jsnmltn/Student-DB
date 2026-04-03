#!/usr/bin/env python3
import argparse
import sqlite3
import csv
import sys
import re
from pathlib import Path

DB_PATH = Path("students.db")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER,
    grade TEXT,
    email TEXT
);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    return conn

def init_db():
    conn = get_conn()
    conn.execute(CREATE_SQL)
    conn.commit()
    conn.close()
    print(f"Initialized database at {DB_PATH}")

def add_student(name, age=None, grade=None, email=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO students (name, age, grade, email) VALUES (?, ?, ?, ?)",
        (name, age, grade, email),
    )
    conn.commit()
    sid = cur.lastrowid
    conn.close()
    print(f"Added student id={sid} name={name}")

def list_students(limit=None):
    conn = get_conn()
    cur = conn.cursor()
    q = "SELECT id, name, age, grade, email FROM students ORDER BY id"
    if limit:
        q += f" LIMIT {int(limit)}"
    cur.execute(q)
    rows = cur.fetchall()
    conn.close()
    if not rows:
        print("No students found.")
        return
    print("ID\tName\tAge\tGrade\tEmail")
    for r in rows:
        print(f"{r[0]}\t{r[1]}\t{r[2] or ''}\t{r[3] or ''}\t{r[4] or ''}")

def find_students(query):
    conn = get_conn()
    cur = conn.cursor()
    like = f"%{query}%"
    cur.execute(
        "SELECT id, name, age, grade, email FROM students WHERE name LIKE ? OR email LIKE ? ORDER BY id",
        (like, like),
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        print("No matches.")
        return
    for r in rows:
        print(f"{r[0]}\t{r[1]}\t{r[2] or ''}\t{r[3] or ''}\t{r[4] or ''}")

def delete_student(student_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM students WHERE id = ?", (student_id,))
    conn.commit()
    changed = cur.rowcount
    conn.close()
    if changed:
        print(f"Deleted student id={student_id}")
    else:
        print(f"No student with id={student_id}")

def export_csv(path):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, age, grade, email FROM students ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    with open(path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "grade", "email"])
        writer.writerows(rows)
    print(f"Exported {len(rows)} students to {path}")

def import_csv(path, delimiter=',', encoding='utf-8', skip_invalid=False, strict=False, update_existing=False, mapping=None):
    """Import students from CSV.

    Options:
      - `delimiter`: CSV delimiter (default ',')
      - `encoding`: file encoding (default 'utf-8')
      - `skip_invalid`: continue past invalid rows
      - `strict`: require fields and enforce stronger checks
      - `update_existing`: update row when email already exists
    Expects header with columns: name, age, grade, email
    """
    path = Path(path)
    if not path.exists():
        print(f"File not found: {path}")
        return
    added = 0
    updated = 0
    errors = []
    email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    conn = get_conn()
    cur = conn.cursor()
    with open(path, newline='', encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for i, row in enumerate(reader, start=2):
            # support mapping from CSV headers to fields
            def col(field):
                if mapping and field in mapping:
                    return (row.get(mapping[field]) or '').strip()
                return (row.get(field) or '').strip()

            name = col('name')
            age_raw = col('age')
            grade = col('grade') or None
            email = col('email') or None

            # Name validation
            if not name or len(name) < 2 or name.isdigit():
                msg = 'missing or invalid name'
                errors.append((i, msg))
                if skip_invalid:
                    continue
                else:
                    break

            # Age validation
            age = None
            if age_raw:
                try:
                    age = int(age_raw)
                except ValueError:
                    errors.append((i, f'invalid age: {age_raw}'))
                    if skip_invalid:
                        continue
                    else:
                        break
                if not (3 <= age <= 120):
                    errors.append((i, f'age out of range: {age}'))
                    if skip_invalid:
                        continue
                    else:
                        break
            else:
                if strict:
                    errors.append((i, 'missing age (strict mode)'))
                    if skip_invalid:
                        continue
                    else:
                        break

            # Grade validation
            if grade and len(grade) > 20:
                errors.append((i, f'grade too long: {grade}'))
                if skip_invalid:
                    continue
                else:
                    break
            if strict and not grade:
                errors.append((i, 'missing grade (strict mode)'))
                if skip_invalid:
                    continue
                else:
                    break

            # Email validation and uniqueness
            if email:
                if not email_re.match(email):
                    errors.append((i, f'invalid email: {email}'))
                    if skip_invalid:
                        continue
                    else:
                        break
                cur.execute("SELECT id FROM students WHERE email = ?", (email,))
                existing = cur.fetchone()
                if existing:
                    if update_existing:
                        cur.execute(
                            "UPDATE students SET name = ?, age = ?, grade = ? WHERE id = ?",
                            (name, age, grade, existing[0]),
                        )
                        updated += 1
                        continue
                    else:
                        errors.append((i, f'duplicate email: {email}'))
                        if skip_invalid:
                            continue
                        else:
                            break
            else:
                if strict:
                    errors.append((i, 'missing email (strict mode)'))
                    if skip_invalid:
                        continue
                    else:
                        break

            cur.execute(
                "INSERT INTO students (name, age, grade, email) VALUES (?, ?, ?, ?)",
                (name, age, grade, email),
            )
            added += 1
    conn.commit()
    conn.close()
    print(f"Imported {added} rows from {path}; updated {updated} existing rows")
    if errors:
        print("Errors:")
        for ln, msg in errors:
            print(f"  line {ln}: {msg}")

def preview_csv(path, delimiter=',', encoding='utf-8', rows=10, strict=False, mapping=None):
    """Preview CSV rows and validation without inserting."""
    path = Path(path)
    if not path.exists():
        print(f"File not found: {path}")
        return
    email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    seen_emails = set()
    errors = []
    previewed = 0
    with open(path, newline='', encoding=encoding) as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for i, row in enumerate(reader, start=2):
            if previewed >= rows:
                break
            def col(field):
                if mapping and field in mapping:
                    return (row.get(mapping[field]) or '').strip()
                return (row.get(field) or '').strip()

            name = col('name')
            age_raw = col('age')
            grade = col('grade')
            email = col('email')

            previewed += 1
            issues = []
            if not name or len(name) < 2 or name.isdigit():
                issues.append('missing or invalid name')
            if age_raw:
                try:
                    a = int(age_raw)
                    if not (3 <= a <= 120):
                        issues.append('age out of range')
                except ValueError:
                    issues.append('invalid age')
            else:
                if strict:
                    issues.append('missing age (strict)')
            if grade and len(grade) > 20:
                issues.append('grade too long')
            if email:
                if not email_re.match(email):
                    issues.append('invalid email')
                if email in seen_emails:
                    issues.append('duplicate email in preview')
                seen_emails.add(email)
            else:
                if strict:
                    issues.append('missing email (strict)')

            print(f"Row {i}: name='{name}' age='{age_raw}' grade='{grade}' email='{email}'")
            if issues:
                print("  Issues:")
                for it in issues:
                    print(f"   - {it}")
    print(f"Previewed {previewed} rows from {path}")

def interactive_import_map(path, delimiter=',', encoding='utf-8', skip_invalid=False, strict=False, update_existing=False):
    path = Path(path)
    if not path.exists():
        print(f"File not found: {path}")
        return
    with open(path, newline='', encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)
        try:
            headers = next(reader)
        except StopIteration:
            print("Empty CSV")
            return
    print("CSV headers:")
    for idx, h in enumerate(headers, start=1):
        print(f"  {idx}. {h}")
    print("Enter the header name to map for each field, or leave blank to skip.")
    mapping = {}
    for field in ('name', 'age', 'grade', 'email'):
        val = input(f"Map CSV column for '{field}' (suggested '{field}'): ").strip()
        if val:
            if val not in headers:
                print(f"Warning: '{val}' not found in headers; mapping will still be used.")
            mapping[field] = val
    print("Mapping:")
    for k, v in mapping.items():
        print(f"  {k} <- {v}")
    confirm = input("Proceed with import? [y/N]: ").strip().lower()
    if confirm != 'y':
        print("Cancelled")
        return
    import_csv(path, delimiter=delimiter, encoding=encoding, skip_invalid=skip_invalid, strict=strict, update_existing=update_existing, mapping=mapping)

def update_student_record(by_id=None, by_email=None, name=None, age=None, grade=None, email=None):
    if by_id is None and by_email is None:
        print("Provide --id or --email to select a student to update")
        return
    fields = []
    vals = []
    if name is not None:
        fields.append('name = ?')
        vals.append(name)
    if age is not None:
        fields.append('age = ?')
        vals.append(age)
    if grade is not None:
        fields.append('grade = ?')
        vals.append(grade)
    if email is not None:
        fields.append('email = ?')
        vals.append(email)
    if not fields:
        print('No fields to update')
        return
    q = 'UPDATE students SET ' + ', '.join(fields)
    if by_id is not None:
        q += ' WHERE id = ?'
        vals.append(by_id)
    else:
        q += ' WHERE email = ?'
        vals.append(by_email)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(q, tuple(vals))
    conn.commit()
    changed = cur.rowcount
    conn.close()
    if changed:
        print(f'Updated {changed} record(s)')
    else:
        print('No matching record found to update')

def interactive_menu():
    print("Student DB interactive mode. Type 'help' for commands.")
    init_db()
    while True:
        try:
            cmd = input("db> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not cmd:
            continue
        parts = cmd.split()
        cmd0 = parts[0].lower()
        args = parts[1:]
        if cmd0 in ("q", "quit", "exit"):
            break
        if cmd0 in ("h", "help"):
            print("Commands: add, list, find, delete, export, quit")
            continue
        if cmd0 == "add":
            name = input("Name: ")
            age = input("Age (optional): ") or None
            grade = input("Grade (optional): ") or None
            email = input("Email (optional): ") or None
            add_student(name, int(age) if age else None, grade, email)
            continue
        if cmd0 == "list":
            n = args[0] if args else None
            list_students(n)
            continue
        if cmd0 == "find":
            if not args:
                print("Usage: find <query>")
                continue
            find_students(" ".join(args))
            continue
        if cmd0 == "delete":
            if not args:
                print("Usage: delete <id>")
                continue
            delete_student(int(args[0]))
            continue
        if cmd0 == "export":
            path = args[0] if args else "students_export.csv"
            export_csv(path)
            continue
        print("Unknown command. Type 'help' for commands.")

def build_parser():
    p = argparse.ArgumentParser(description="Student DB manager")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("init", help="Initialize the database")

    p_add = sub.add_parser("add", help="Add a student")
    p_add.add_argument("--name", required=True)
    p_add.add_argument("--age", type=int)
    p_add.add_argument("--grade")
    p_add.add_argument("--email")

    sub.add_parser("list", help="List students").add_argument("--limit", type=int, default=None)

    p_find = sub.add_parser("find", help="Find students by name/email")
    p_find.add_argument("query")

    p_del = sub.add_parser("delete", help="Delete student by id")
    p_del.add_argument("id", type=int)

    p_exp = sub.add_parser("export", help="Export students to CSV")
    p_exp.add_argument("path", nargs="?", default="students_export.csv")

    p_imp = sub.add_parser("import", help="Import students from CSV")
    p_imp.add_argument("path")
    p_imp.add_argument("--skip-invalid", action="store_true", help="Skip invalid rows and continue")
    p_imp.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    p_imp.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    p_imp.add_argument("--strict", action="store_true", help="Require all fields and enforce stricter validation")
    p_imp.add_argument("--update-existing", action="store_true", help="Update existing rows when email matches")
    p_imp.add_argument("--map", help="Provide mapping as comma-separated pairs field=column (e.g. name=FullName,email=Email)")

    sub.add_parser("interactive", help="Run interactive prompt")
    p_preview = sub.add_parser("preview", help="Preview CSV import without inserting")
    p_preview.add_argument("path")
    p_preview.add_argument("--rows", type=int, default=10)
    p_preview.add_argument("--delimiter", default=",")
    p_preview.add_argument("--encoding", default="utf-8")
    p_preview.add_argument("--strict", action="store_true")

    p_map = sub.add_parser("import-map", help="Interactive CSV-to-DB mapping helper and import")
    p_map.add_argument("path")
    p_map.add_argument("--delimiter", default=",")
    p_map.add_argument("--encoding", default="utf-8")
    p_map.add_argument("--skip-invalid", action="store_true")
    p_map.add_argument("--strict", action="store_true")
    p_map.add_argument("--update-existing", action="store_true")

    p_update = sub.add_parser("update", help="Update a student record by id or email")
    p_update.add_argument("--id", type=int)
    p_update.add_argument("--email", help="Select student by email")
    p_update.add_argument("--name")
    p_update.add_argument("--age", type=int)
    p_update.add_argument("--grade")
    p_update.add_argument("--new-email", help="Set a new email for the student")

    return p

def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args or args.cmd is None:
        interactive_menu()
        return

    if args.cmd == "init":
        init_db()
    elif args.cmd == "add":
        add_student(args.name, args.age, args.grade, args.email)
    elif args.cmd == "list":
        list_students(args.limit)
    elif args.cmd == "find":
        find_students(args.query)
    elif args.cmd == "delete":
        delete_student(args.id)
    elif args.cmd == "export":
        mapping = None
        if getattr(args, 'map', None):
            # parse mapping string like name=FullName,email=Email
            mapping = {}
            for pair in args.map.split(','):
                if '=' in pair:
                    k, v = pair.split('=', 1)
                    mapping[k.strip()] = v.strip()
        import_csv(args.path, delimiter=args.delimiter, encoding=args.encoding, skip_invalid=args.skip_invalid, strict=args.strict, update_existing=args.update_existing, mapping=mapping)
    elif args.cmd == "preview":
        preview_csv(args.path, delimiter=args.delimiter, encoding=args.encoding, rows=args.rows, strict=args.strict)
    elif args.cmd == "import-map":
        interactive_import_map(args.path, delimiter=args.delimiter, encoding=args.encoding, skip_invalid=args.skip_invalid, strict=args.strict, update_existing=args.update_existing)
    elif args.cmd == "update":
        new_email = getattr(args, 'new_email', None)
        update_student_record(by_id=args.id, by_email=args.email, name=args.name, age=args.age, grade=args.grade, email=new_email)
    elif args.cmd == "import":
        import_csv(args.path, skip_invalid=args.skip_invalid)

if __name__ == "__main__":
    main()
