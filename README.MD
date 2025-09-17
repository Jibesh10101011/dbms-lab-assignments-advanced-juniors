## Status

**Releases:**  
[![GitHub release](https://img.shields.io/github/v/release/Jibesh10101011/dbms-lab-assignments-advanced-juniors)](https://github.com/Jibesh10101011/dbms-lab-assignments-advanced-juniors/releases)  
[![GitHub commits](https://img.shields.io/github/commits-since/Jibesh10101011/dbms-lab-assignments-advanced-juniors/latest)](https://github.com/Jibesh10101011/dbms-lab-assignments-advanced-juniors/commits/main)

**Code:**  
[![Build Status](https://github.com/Jibesh10101011/dbms-lab-assignments-advanced-juniors/actions/workflows/ci.yml/badge.svg)](https://github.com/Jibesh10101011/dbms-lab-assignments-advanced-juniors/actions)  
[![codecov](https://codecov.io/gh/Jibesh10101011/dbms-lab-assignments-advanced-juniors/branch/main/graph/badge.svg)](https://codecov.io/gh/Jibesh10101011/dbms-lab-assignments-advanced-juniors)  
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/0000/badge)](https://bestpractices.coreinfrastructure.org/)  
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/Jibesh10101011/dbms-lab-assignments-advanced-juniors/badge)](https://securityscorecards.dev/viewer/?uri=github.com/Jibesh10101011/dbms-lab-assignments-advanced-juniors)  
[![License Scan](https://app.fossa.com/api/projects/git%2Bgithub.com%2FJibesh10101011%2Fdbms-lab-assignments-advanced-juniors.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FJibesh10101011%2Fdbms-lab-assignments-advanced-juniors?ref=badge_shield)

**Social:**  
[![Follow on Twitter](https://img.shields.io/twitter/follow/Jibesh10101011?style=social)](https://twitter.com/Jibesh10101011)  
[![GitHub followers](https://img.shields.io/github/followers/Jibesh10101011?style=social)](https://github.com/Jibesh10101011)


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

---

## Contributing to DBMS Lab Assignments

Thank you for your interest in contributing! This repository contains advanced DBMS lab assignments for juniors, with algorithm-focused exercises in C++, Python, Go, Rust, and Node.js. By contributing, you help create a high-quality learning resource for everyone.

## How to Contribute

### 1. Fork the Repository
- Click the "Fork" button at the top-right corner of the repo to create your own copy.

### 2. Clone Your Fork
```bash
git clone https://github.com/<your-username>/dbms-lab-assignments-advanced-juniors.git
cd dbms-lab-assignments-advanced-juniors

