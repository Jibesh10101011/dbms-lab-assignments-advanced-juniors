#include <mysql/mysql.h>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>

using namespace std;

const char* generateInsertQuery(string &, vector<string> &, vector<string> &);

class MySQLClient {
    MYSQL *conn;
    public:
    
    MySQLClient() {
        this->conn=NULL;
    }

    MySQLClient(MYSQL *conn) {
        this->conn=conn;
    }

    MySQLClient(const char *server, const char *user, const char *password, const char *database) {
        this->conn=mysql_init(NULL);
        if(this->conn == NULL) {
            cerr << "mysql_init() failed" << endl;
            exit(1);
        }

        if(mysql_real_connect(this->conn, server, user, password, database, 0, NULL, 0) == NULL) {
            cerr << "mysql_real_connect() failed, maybe for invalid credentails";
            mysql_close(this->conn);
            exit(1);
        }
    }

    void initConnection(const char *server, const char *user, const char *password, const char *database) {
        this->conn=mysql_init(NULL);
        if(this->conn == NULL) {
            cerr << "mysql_init() failed" << endl;
            exit(1);
        }

        if(mysql_real_connect(this->conn, server, user, password, database, 0, NULL, 0) == NULL) {
            cerr << "mysql_real_connect() failed, maybe for invalid credentails";
            mysql_close(this->conn);
            exit(1);
        }
    }

    void executeQuery(const char* query) { // use for CREATE, DELETE Table don't use for SELECT 
        if (mysql_query(this->conn, query)) {
            cerr << "Some error occured, Error: "<< mysql_error(this->conn) << endl;
            mysql_close(conn);
            exit(1);
        } else {
            cout << "QUERY is successfully executed" << endl;
        }
    }

    void executeSELECTQuery(const char* select_query) {
        if (mysql_query(this->conn, select_query)) {
            cerr << "SELECT failed. Error: " << mysql_error(this->conn) << "\n";
            mysql_close(this->conn);
            exit(1);
        }

        MYSQL_RES *res = mysql_store_result(this->conn);
        if (res == NULL) {
            cerr << "mysql_store_result() failed\n";
            mysql_close(conn);
            exit(1);
        }

        MYSQL_ROW row;
        int num_fields = mysql_num_fields(res);
        while ((row = mysql_fetch_row(res))) {
            for (int i = 0; i < num_fields; i++) {
                cout << (row[i] ? row[i] : "NULL") << " ";
            }
            cout << endl;
        }
        mysql_free_result(res);
    }


    void insertIntoTableFromCSVFile(string &pathName, vector<string> &columns, string &tableName) {
        ifstream in(pathName);
        if(!in.is_open()) {
            cerr << "Error opening files" <<endl;
            exit(1);
        }

        string line;
        getline(in, line);
        while(getline(in, line)) {
            stringstream ss(line);
            vector<string>tokens;
            string token;
            
            while (getline(ss, token, ',')) {
                tokens.push_back(token);
            }

           
            if(tokens.size() != columns.size()) { // Skip Row
                cout << "Row is skipped for invalid data format" << endl;
                continue;
            }

            // const char *insert_query =
            //     "INSERT INTO user (id, first_name, last_name, email, gender, ip_address)"
            //     "VALUES ('Alice', 'Smith', 'alice@example.com', 'Female', '192.168.1.10');";
            
            const char *insert_query = generateInsertQuery(tableName, columns, tokens);
            cout<<insert_query<<endl;
            this->executeQuery(insert_query);
            // cout<<insert_query<<endl;
        }

    }


    void closeConnection() {
        mysql_close(this->conn);
    }


};


const char* generateInsertQuery(string &tableName, vector<string> &columns, vector<string> &tokens) { // be sure that columns.size() == tokens.size()
    static string query;
    query.clear();

    query = "INSERT INTO " + tableName + "(";
    for (size_t i = 0; i < columns.size(); i++) {
        query += columns[i];
        if (i != columns.size() - 1) query += ",";
    }
    query += ") VALUES(";

    for (size_t i = 0; i < tokens.size(); i++) {
        query += "'" + tokens[i] + "'";
        if (i != tokens.size() - 1) query += ",";
    }
    query += ");";

    return query.c_str();
}


int main() {
    const char *server = "192.168.56.1";
    const char *user = "linux-user";
    const char *password = "123";
    const char *database = "testdb";

    MySQLClient *client = new MySQLClient(server, user, password, database);
    
    string pathName="/mnt/c/Users/Jibesh/OneDrive/Desktop/dbms/MOCK_DATA.csv";
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
    client->executeQuery(create_table_query); // create table
    client->insertIntoTableFromCSVFile(pathName, columns, tableName); // insert all from CSV file
    client->closeConnection();
    
    return 0;
}
