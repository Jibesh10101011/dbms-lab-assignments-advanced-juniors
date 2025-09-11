import mysql.connector
import csv
import sys

def create_mysql_client():
    """Create and return a MySQL connection."""
    try:
        conn = mysql.connector.connect(
            host="192.168.56.1",
            user="linux-user",
            password="123",
            database="testdb"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Connection failed: {err}")
        sys.exit(1)

def execute_query(conn, query):
    """Execute a single SQL query."""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        print("Query executed successfully")
    except mysql.connector.Error as err:
        print(f"Query failed: {err}")
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()

def insert_into_table_from_csv(conn, path, columns, table_name):
    """Read CSV file and insert into table using a transaction."""
    cursor = conn.cursor()
    rows = []

    try:
        with open(path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                values = [row[col] for col in columns]
                if len(values) == len(columns):
                    rows.append(values)

        if not rows:
            print("No data found in CSV")
            return

        placeholders = ",".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"

        # Transaction
        try:
            cursor.executemany(query, rows)  # bulk insert
            conn.commit()
            print(f"Inserted {cursor.rowcount} rows successfully")
        except mysql.connector.Error as err:
            conn.rollback()
            print(f"Insert failed: {err}")
            sys.exit(1)

    finally:
        cursor.close()

def main():
    conn = create_mysql_client()

    columns = ["id", "first_name", "last_name", "email", "gender", "ip_address"]
    table_name = "user"
    csv_path = r"C:\Users\Jibesh\OneDrive\Desktop\dbms\MOCK_DATA.csv"

    create_table = """
    CREATE TABLE IF NOT EXISTS user (
      id INT PRIMARY KEY,
      first_name VARCHAR(50),
      last_name VARCHAR(50),
      email VARCHAR(100) UNIQUE,
      gender VARCHAR(50),
      ip_address VARCHAR(45)
    );
    """

    execute_query(conn, "DROP TABLE IF EXISTS user;")
    execute_query(conn, create_table)
    insert_into_table_from_csv(conn, csv_path, columns, table_name)

    conn.close()

if __name__ == "__main__":
    main()
