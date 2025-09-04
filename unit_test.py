class TestETLFlow(unittest.TestCase):

    def setUp(self):
        self.test_csv = "data/FactTransactions.csv"
        self.test_db = "data/test_database.db"
        self.test_table = "test_table"

        # Create a small sample CSV if not exists
        if not os.path.exists("data"):
            os.makedirs("data")
        if not os.path.exists(self.test_csv):
            pd.DataFrame({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"]
            }).to_csv(self.test_csv, index=False)

    def test_load_csv(self):
        df = load_csv(self.test_csv)
        self.assertFalse(df.empty)
        self.assertIn("id", df.columns)

    def test_write_to_sql(self):
        df = pd.read_csv(self.test_csv)
        write_to_sql(df, self.test_db, self.test_table)

        conn = sqlite3.connect(self.test_db)
        result = pd.read_sql_query(f"SELECT * FROM {self.test_table}", conn)
        conn.close()

        self.assertEqual(len(result), 3)

if __name__ == "__main__":
    unittest.main()
