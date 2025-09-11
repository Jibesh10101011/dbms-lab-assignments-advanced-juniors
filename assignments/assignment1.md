# Assignment: Insert CSV Data into MySQL Using C++

## Objective

You are provided with a CSV file containing user data:

```
id,first_name,last_name,email,gender,ip_address
```

Your task is to **read the CSV file** and **insert its data into a MySQL table** using C++.


**⚠️ Caution:** Do not directly move to the solutions provided. Try to solve it yourself first.


---

## MySQL Connection Parameters

In the skeleton code, the MySQL server connection parameters are:

```cpp
const char *server = "192.168.56.1";
const char *user = "linux-user";
const char *password = "123";
const char *database = "testdb";
```

* **server** → IP address of the MySQL server
* **user** → MySQL username
* **password** → Password for the MySQL user
* **database** → Database to connect and insert data

Make sure this database exists before running your program.

---

## Workflow

1. **Connect to MySQL** using the `MySQLClient` class.
2. **Create a table** if it does not exist, using a `CREATE TABLE` query.
3. **Optionally drop the table** if you want to reset data: `DROP TABLE IF EXISTS user;`
4. **Read the CSV file** row by row.
5. **Generate an INSERT query** for each row.
6. **Insert rows into the table** using `executeQuery()`.
7. **Close the connection** after all operations.

---

## Skeleton Code

```cpp
#include <mysql/mysql.h>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <vector>
#include <sstream>

using namespace std;

const char* generateInsertQuery(string &, vector<string> &, vector<string> &);

class MySQLClient {
    MYSQL *conn;
public:

    MySQLClient() {
        // TODO: initialize connection pointer
    }

    MySQLClient(MYSQL *conn) {
        // TODO: initialize connection
    }

    MySQLClient(const char *server, const char *user, const char *password, const char *database) {
        // TODO: connect to MySQL server
    }

    void initConnection(const char *server, const char *user, const char *password, const char *database) {
        // TODO: re-initialize connection
    }

    void executeQuery(const char* query) {
        // TODO: execute CREATE/DROP/INSERT query
    }

    void executeSELECTQuery(const char* select_query) {
        // TODO: execute SELECT query and print results
    }

    void insertIntoTableFromCSVFile(string &pathName, vector<string> &columns, string &tableName) {
        // TODO: read CSV, generate INSERT queries, and insert into table
    }

    void closeConnection() {
        // TODO: close MySQL connection
    }
};

const char* generateInsertQuery(string &tableName, vector<string> &columns, vector<string> &tokens) {
    // TODO: implement function to return SQL INSERT query as const char*
}

int main() {
    const char *server = "192.168.56.1";
    const char *user = "linux-user";
    const char *password = "123";
    const char *database = "testdb";

    MySQLClient *client = new MySQLClient(server, user, password, database);

    string pathName="MOCK_DATA.csv";
    vector<string> columns= {"id", "first_name", "last_name", "email", "gender", "ip_address"};
    string tableName = "user";

    const char *create_table_query = 
        "CREATE TABLE IF NOT EXISTS user ("
        "id INT AUTO_INCREMENT PRIMARY KEY,"
        "first_name VARCHAR(50),"
        "last_name VARCHAR(50),"
        "email VARCHAR(100) UNIQUE,"
        "gender VARCHAR(50),"
        "ip_address VARCHAR(45)"
        ");";

    client->executeQuery("DROP TABLE IF EXISTS user;"); // Delete all row data
    client->executeQuery(create_table_query);           // create table
    client->insertIntoTableFromCSVFile(pathName, columns, tableName); // insert CSV data
    client->closeConnection();

    return 0;
}
```

---

## Questions for Students after solving the assignment and viewing the solutions

1. Why in the `generateInsertQuery` function we use:

    ```cpp
    static string query;
    ```

What is the benefit of `static` here?

2. Why do we use `const char*` for queries when passing to `mysql_query()`?

3. Explain the workflow of reading CSV and inserting into MySQL.

4. How would you modify the program to **handle invalid rows** or **duplicate emails**?

5. **Advanced Assignment:** Modify the program to use threads to achieve parallelism when inserting rows. Compare the time taken for inserting all data sequentially vs using threads.

---

## Notes

* Make sure MySQL server is running and credentials are correct.
* CSV file should be in the same folder or provide the full path.
* Compile using:

### Compile
```bash
g++ main.cpp -o mysql_test -lmysqlclient
```

## Run
```bash
./mysql_test
```








