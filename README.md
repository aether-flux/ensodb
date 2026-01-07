# EnsoDB
**EnsoDB** is a lightweight, embedded-first database written in Rust, designed for learning, hacking, and building developer-friendly systems from the ground up.

It focuses on:
- simple internals
- explicit abstractions
- clean APIs
- and a gradual path from local usage → networked database

> ⚠️ EnsoDB is **experimental** and currently in early development.

---

## Features

- File-backed storage engine
- Schema-based tables
- Primary key indexing
- Insert / Select / Delete support
- Custom SQL-like DSL (in progress)
- Interactive REPL
- TCP server for remote connections
- Rust-first API design

---

## Installation

### Linux
```bash
curl -fsSL https://raw.githubusercontent.com/aether-flux/ensodb/main/install/linux/install.sh | sh
```

### Windows
Powershell:
```bash
iwr -useb https://raw.githubusercontent.com/aether-flux/ensodb/main/install/windows/install.bat | iex
```

This starts:
- the EnsoDB REPL
- a TCP server (default: `127.0.0.1:5432`)

---

## Example (REPL)
```bash
[test_db] > INSERT INTO users VALUES(1, "user_name");
1 row(s) affected

[test_db] > SELECT * FROM users;
| id | name |
|----|------|
| 1  | user_name |
```

---

## TCP Server
EnsoDB exposes a simple text-based TCP protocol.
- One query per line
- Results returned as text
- Responses terminated with a protocol EOF marker

This allows external clients (CLI tools, SDKs) to connect.

---

## Motivation
EnsoDB exists to:
- understand database internals
- explore Rust system design
- and build a database feels good to use

---

## License
MIT

