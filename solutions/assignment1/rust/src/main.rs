use mysql::*;
use mysql::prelude::*;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use csv::ReaderBuilder;

fn create_mysql_pool() -> Result<Pool, Box<dyn Error>> {
    let url = "mysql://linux-user:123@192.168.56.1:3306/testdb";
    let pool = Pool::new(url)?;
    Ok(pool)
}

fn execute_query(conn: &mut PooledConn, query: &str) -> Result<(), Box<dyn Error>> {
    match conn.query_drop(query) {
        Ok(_) => {
            println!("Query executed successfully");
            Ok(())
        }
        Err(err) => {
            eprintln!("Query failed: {}", err);
            Err(Box::new(err))
        }
    }
}

fn insert_into_table_from_csv(
    conn: &mut PooledConn,
    path: &str,
    columns: &[&str],
    table_name: &str,
) -> Result<(), Box<dyn Error>> {
    if !Path::new(path).exists() {
        return Err(format!("CSV file not found: {}", path).into());
    }

    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    let mut rows: Vec<Vec<String>> = Vec::new();

    for result in rdr.records() {
        let record = result?;
        let mut row: Vec<String> = Vec::new();

        for col in columns {
            match record.get(*col) {
                Some(val) => row.push(val.to_string()),
                None => {
                    eprintln!("Missing column {}, skipping row", col);
                    row.clear();
                    break;
                }
            }
        }

        if row.len() == columns.len() {
            rows.push(row);
        }
    }

    if rows.is_empty() {
        println!("No data found in CSV");
        return Ok(());
    }

    let placeholders = vec!["?"; columns.len()].join(",");
    let query = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        columns.join(","),
        placeholders
    );

    let mut tx = conn.start_transaction(TxOpts::default())?;

    for row in rows {
        let params: Vec<Value> = row.into_iter().map(Value::from).collect();
        tx.exec_drop(&query, params)?;
    }

    tx.commit()?;
    println!("All rows inserted successfully");

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let pool = create_mysql_pool()?;
    let mut conn = pool.get_conn()?;

    let columns = ["id", "first_name", "last_name", "email", "gender", "ip_address"];
    let table_name = "user";
    let csv_path = r"C:\Users\Jibesh\OneDrive\Desktop\dbms\MOCK_DATA.csv";

    let create_table = r#"
        CREATE TABLE IF NOT EXISTS user (
          id INT PRIMARY KEY,
          first_name VARCHAR(50),
          last_name VARCHAR(50),
          email VARCHAR(100) UNIQUE,
          gender VARCHAR(50),
          ip_address VARCHAR(45)
        );
    "#;

    execute_query(&mut conn, "DROP TABLE IF EXISTS user;")?;
    execute_query(&mut conn, create_table)?;
    insert_into_table_from_csv(&mut conn, csv_path, &columns, table_name)?;

    Ok(())
}
