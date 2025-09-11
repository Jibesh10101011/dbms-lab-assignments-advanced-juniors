package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// GenerateInsertQuery builds an INSERT query dynamically
func GenerateInsertQuery(tableName string, columns []string, tokens []string) string {
	if len(columns) != len(tokens) {
		return "" // invalid row
	}

	query := fmt.Sprintf("INSERT INTO %s(", tableName)
	query += strings.Join(columns, ",")
	query += ") VALUES("

	values := make([]string, len(tokens))
	for i, token := range tokens {
		// escape single quotes
		values[i] = "'" + strings.ReplaceAll(token, "'", "''") + "'"
	}
	query += strings.Join(values, ",")
	query += ");"

	return query
}

type MySQLClient struct {
	db *sql.DB
}

func NewMySQLClient(server, user, password, database string) *MySQLClient {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, server, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("sql.Open() failed: %v", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("mysql_real_connect() failed: %v", err)
	}
	return &MySQLClient{db: db}
}

func (c *MySQLClient) ExecuteQuery(query string) {
	_, err := c.db.Exec(query)
	if err != nil {
		log.Fatalf("Query failed: %v\nQuery: %s", err, query)
	} else {
		fmt.Println("QUERY executed successfully")
	}
}

func (c *MySQLClient) ExecuteSelectQuery(query string) {
	rows, err := c.db.Query(query)
	if err != nil {
		log.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Fatal(err)
		}
		for _, col := range values {
			if col == nil {
				fmt.Print("NULL ")
			} else {
				fmt.Print(string(col) + " ")
			}
		}
		fmt.Println()
	}
}

func (c *MySQLClient) InsertIntoTableFromCSVFile(pathName string, columns []string, tableName string) {
	file, err := os.Open(pathName)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// skip header
	if scanner.Scan() {
		// ignore first line
	}

	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, ",")

		if len(tokens) != len(columns) {
			fmt.Println("Row skipped due to invalid format:", line)
			continue
		}

		insertQuery := GenerateInsertQuery(tableName, columns, tokens)
		if insertQuery != "" {
			fmt.Println(insertQuery)
			c.ExecuteQuery(insertQuery)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
}

func (c *MySQLClient) Close() {
	c.db.Close()
}

func main() {
	server := "192.168.56.1:3306"
	user := "linux-user"
	password := "123"
	database := "testdb"

	client := NewMySQLClient(server, user, password, database)

	tableName := "user"
	columns := []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}
	pathName := "/mnt/c/Users/Jibesh/OneDrive/Desktop/dbms/MOCK_DATA.csv"

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS user (
		id INT AUTO_INCREMENT PRIMARY KEY,
		first_name VARCHAR(50),
		last_name VARCHAR(50),
		email VARCHAR(100) UNIQUE,
		gender VARCHAR(50),
		ip_address VARCHAR(45)
	);`

	client.ExecuteQuery("DROP TABLE IF EXISTS user;")
	client.ExecuteQuery(createTableQuery)
	client.InsertIntoTableFromCSVFile(pathName, columns, tableName)
	client.Close()
}
