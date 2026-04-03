import unittest
import tempfile
import sqlite3
import importlib
import sys
from pathlib import Path

# Add parent directory to path so we can import students_db
sys.path.insert(0, str(Path(__file__).parent.parent))

import students_db

class TestStudentsDB(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.dbpath = Path(self.tmpdir.name) / "test.db"
        students_db.DB_PATH = self.dbpath
        if self.dbpath.exists():
            self.dbpath.unlink()
        students_db.init_db()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_add_and_query(self):
        students_db.add_student("Unit Test", 20, "12", "unit@test.com")
        conn = sqlite3.connect(students_db.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT name, age, grade, email FROM students WHERE email = ?", ("unit@test.com",))
        row = cur.fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "Unit Test")
        self.assertEqual(row[1], 20)

    def test_import_csv_basic(self):
        csvpath = Path(self.tmpdir.name) / "import.csv"
        with open(csvpath, 'w', newline='', encoding='utf-8') as f:
            f.write("name,age,grade,email\n")
            f.write("Imp One,10,5,imp1@example.com\n")
            f.write("Imp Two,11,6,imp2@example.com\n")
        students_db.import_csv(csvpath)
        conn = sqlite3.connect(students_db.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM students")
        cnt = cur.fetchone()[0]
        conn.close()
        self.assertEqual(cnt, 2)

    def test_preview_and_strict(self):
        csvpath = Path(self.tmpdir.name) / "import2.csv"
        with open(csvpath, 'w', newline='', encoding='utf-8') as f:
            f.write("name,age,grade,email\n")
            f.write("Good,12,7,good@example.com\n")
            f.write(",13,8,bad@example.com\n")
        # preview should not raise
        students_db.preview_csv(csvpath, rows=5, strict=True)
        # import in strict mode should stop on missing name (without skip)
        students_db.import_csv(csvpath, strict=True, skip_invalid=True)
        conn = sqlite3.connect(students_db.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM students")
        cnt = cur.fetchone()[0]
        conn.close()
        # only the valid one imported
        self.assertEqual(cnt, 1)

if __name__ == '__main__':
    unittest.main()
