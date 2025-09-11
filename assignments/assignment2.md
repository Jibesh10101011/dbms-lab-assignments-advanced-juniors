# Assignment 2: Parallel CSV Data Insertion into MySQL Using C++

## Objective

In this assignment, you will **modify the CSV-to-MySQL program** to use **threads** in C++ to insert data into MySQL in parallel. You will also **compare the time taken** for inserting all data sequentially versus using threads.

**⚠️ Caution:** Do not directly copy solutions. Try to implement it yourself first.

---

## Requirements

* Read data from a CSV file with the following columns:

```
id,first_name,last_name,email,gender,ip_address
```

* Create a MySQL table to store this data.
* Insert the data into MySQL **using multiple threads**.
* Compare the total time taken with **sequential insertion**.

---

## Libraries to Use in C++

To implement this assignment, you will need the following libraries:

* `<mysql/mysql.h>` → For MySQL connectivity
* `<iostream>` → For standard input/output
* `<fstream>` → For reading CSV files
* `<sstream>` → For parsing CSV rows
* `<vector>` → For storing CSV data
* `<thread>` → For multi-threading
* `<chrono>` → For measuring time elapsed
* `<mutex>` → For thread-safe operations (if needed)

---

## MySQL Connection Parameters

```cpp
const char *server = "192.168.56.1";
const char *user = "linux-user";
const char *password = "123";
const char *database = "testdb";
```

Make sure this database exists and the credentials are correct.

---

## Workflow

1. Connect to MySQL using `MySQLClient` class.
2. Create a table if it does not exist.
3. Read the CSV file and store rows in memory.
4. Insert data sequentially and record time taken.
5. Insert data using multiple threads and record time taken.
6. Compare the performance.
7. Close MySQL connection.

---

## Skeleton Code

You can start from the previous assignment's skeleton and add threading logic using `<thread>`.

```cpp
#include <mysql/mysql.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <thread>
#include <chrono>
#include <mutex>

using namespace std;

// MySQLClient class and generateInsertQuery function
// TODO: Implement threading logic in insertIntoTableFromCSVFile
```

---

## Questions for Students

1. How did using threads affect the insertion speed?
2. What precautions do you need to take when multiple threads interact with MySQL?
3. How would you handle race conditions in this program?
4. Compare the pros and cons of sequential vs parallel insertion.

---

## Compilation

Compile the program using:

```bash
g++ main.cpp -o mysql_parallel -lmysqlclient -pthread
./mysql_parallel
```
