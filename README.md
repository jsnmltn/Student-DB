# Student DB

Simple CLI program to manage a student database using SQLite.

Usage:

Initialize database:

```bash
python students_db.py --init
```

Add a student:

```bash
python students_db.py add --name "Alice" --age 14 --grade "9" --email alice@example.com
```

List students:

```bash
python students_db.py list
```

Interactive mode:

```bash
python students_db.py
```

Importing from CSV
------------------

CSV import expects a header row with these columns: `name`, `age`, `grade`, `email`.

Basic import (skip invalid rows):

```bash
python students_db.py import test_students.csv --skip-invalid
```

Import with stricter validation (requires all fields and enforces age range/format):

```bash
python students_db.py import test_students.csv --strict
```

Import with custom delimiter and encoding:

```bash
python students_db.py import myfile.tsv --delimiter "\t" --encoding latin-1 --skip-invalid
```

If the CSV contains rows with emails that already exist in the database, use `--update-existing` to update those records instead of treating them as duplicate errors:

```bash
python students_db.py import test_students.csv --update-existing
```

The `--skip-invalid` flag tells the importer to continue past invalid rows and report errors at the end. Without `--skip-invalid` the importer will stop on the first validation error.

If you want me to add an `update` command or an import preview mode, tell me which behavior you prefer.

New commands added:

- **Preview CSV**: show parsed rows and validation without inserting.

```bash
python students_db.py preview test_students.csv --rows 5
```

- **Interactive mapping helper**: run a prompt to map CSV headers to DB fields and import.

```bash
python students_db.py import-map myfile.csv
```

- **Non-interactive column mapping**: provide a mapping string to `import`.

```bash
python students_db.py import myfile.csv --map "name=FullName,email=EmailAddr" --skip-invalid
```

- **Update student**: update a record by id or email.

```bash
python students_db.py update --id 1 --name "New Name" --grade "11" --new-email new@example.com
```

All previous import flags (`--delimiter`, `--encoding`, `--strict`, `--skip-invalid`, `--update-existing`) apply.