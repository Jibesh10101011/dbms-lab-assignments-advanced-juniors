# DBMS Lab Assignment

## Prerequisites (Ubuntu/Linux via WSL)

### Installing Ubuntu via WSL (Windows Subsystem for Linux)

1. Open PowerShell as Administrator and enable WSL:

```powershell
wsl --install
```

2. Install Ubuntu from Microsoft Store:

   * Open Microsoft Store.
   * Search for "Ubuntu" and select the latest LTS version.
   * Click "Install".
3. Launch Ubuntu from the Start Menu and create a Linux user.
4. Update your Ubuntu environment:

```bash
sudo apt update && sudo apt upgrade -y
```

Before starting the assignments, ensure you have the following libraries installed:

1. **MySQL Development Libraries**

```bash
sudo apt install libmysqlclient-dev
```

2. **C++ Compiler (g++)**

```bash
sudo apt install g++
```

3. **Build Essential Tools**

```bash
sudo apt install build-essential
```

4. **Optional: Threads support (usually comes with g++)**

* Ensure you compile with `-pthread` flag when using threads.

---

## Assignments

* [Insert CSV Data into MySQL Using C++](assignments/assignment1.md)

  * [Solutions](solutions/assignment1/)

* [Modify the previous program to use threads to achieve parallelism when inserting rows. Compare the time taken for inserting all data sequentially vs using threads.](assignments/assignment2.md)
