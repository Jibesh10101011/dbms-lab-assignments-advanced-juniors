import mysql from "mysql2/promise";
import fs from "fs";
import csv from "csv-parser";

async function createMySQLPool() {
  const pool = mysql.createPool({
    host: "192.168.56.1",
    user: "linux-user",
    password: "123",
    database: "testdb",
    waitForConnections: true,
    connectionLimit: 10, 
    queueLimit: 0,
  });
  return pool;
}

async function executeQuery(pool, query) {
  try {
    await pool.query(query);
    console.log("Query executed successfully");
  } catch (err) {
    console.error("Query failed:", err.message);
    process.exit(1);
  }
}

async function insertIntoTableFromCSVFile(pool, path, columns, tableName) {
  const rows = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(path)
      .pipe(csv())
      .on("data", (data) => {
        const row = columns.map((col) => data[col]);
        if (row.length === columns.length) {
          rows.push(row);
        }
      })
      .on("end", async () => {
        if (rows.length === 0) {
          console.log("No data found in CSV");
          return resolve();
        }

        const placeholders = `(${columns.map(() => "?").join(",")})`;
        const query = `INSERT INTO ${tableName} (${columns.join(",")}) VALUES ${placeholders}`;

        const conn = await pool.getConnection();
        try {
          await conn.beginTransaction();

          const stmt = await conn.prepare(query);
          for (const row of rows) {
            await stmt.execute(row);
          }
          await stmt.close();

          await conn.commit();
          console.log("All rows inserted successfully");
          resolve();
        } catch (err) {
          await conn.rollback();
          console.error("Insert failed:", err.message);
          reject(err);
        } finally {
          conn.release();
        }
      });
  });
}

// Main entry point
async function main() {
  const pool = await createMySQLPool();

  const columns = ["id", "first_name", "last_name", "email", "gender", "ip_address"];
  const tableName = "user";
  const csvPath = "C:\\Users\\Jibesh\\OneDrive\\Desktop\\dbms\\MOCK_DATA.csv";

  const createTable = `
    CREATE TABLE IF NOT EXISTS user (
      id INT PRIMARY KEY,
      first_name VARCHAR(50),
      last_name VARCHAR(50),
      email VARCHAR(100) UNIQUE,
      gender VARCHAR(50),
      ip_address VARCHAR(45)
    );
  `;

  await executeQuery(pool, "DROP TABLE IF EXISTS user;");
  await executeQuery(pool, createTable);
  await insertIntoTableFromCSVFile(pool, csvPath, columns, tableName);

  await pool.end();
}

main().catch((err) => console.error("Unexpected Error:", err));
