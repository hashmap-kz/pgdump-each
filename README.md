# pgdump-each

`pgdump-each` is a CLI tool designed to simplify **PostgreSQL major version upgrades** by performing safe and concurrent
logical backups and restores of all databases in a cluster.

[![License](https://img.shields.io/github/license/hashmap-kz/pgdump-each)](https://github.com/hashmap-kz/pgdump-each/blob/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/hashmap-kz/pgdump-each)](https://goreportcard.com/report/github.com/hashmap-kz/pgdump-each)
[![Workflow Status](https://img.shields.io/github/actions/workflow/status/hashmap-kz/pgdump-each/ci.yml?branch=master)](https://github.com/hashmap-kz/pgdump-each/actions/workflows/ci.yml?query=branch:master)
[![GitHub Issues](https://img.shields.io/github/issues/hashmap-kz/pgdump-each)](https://github.com/hashmap-kz/pgdump-each/issues)
[![Go Version](https://img.shields.io/github/go-mod/go-version/hashmap-kz/pgdump-each)](https://github.com/hashmap-kz/pgdump-each/blob/master/go.mod#L3)
[![Latest Release](https://img.shields.io/github/v/release/hashmap-kz/pgdump-each)](https://github.com/hashmap-kz/pgdump-each/releases/latest)

---

## ✨ Features

- Concurrent `pg_dump` of every non-template database in the cluster
- Dumps are stored in `--format=directory` with compression and parallelism
- Dumps global objects (roles, tablespaces, etc.) via `pg_dumpall --globals-only`
- Planned: Concurrent restore via `pg_restore`
- Safety: Refuses to restore if the target cluster is not empty

---

## 🔧 Use Case

Designed for **major version upgrades** of PostgreSQL where logical backups are preferred. Typically used in the
following workflow:

1. Backup all databases from an old cluster (e.g., PostgreSQL 16)
2. Create a new clean cluster (e.g., PostgreSQL 17)
3. Restore all databases into the new cluster

---

## 🧪 Backup Example

```bash
pgdump-each dump \
  --connstr "postgres://postgres:secret@old-cluster:5432/postgres?sslmode=disable" \
  --output ./backups
```

This will:

- Create a timestamped directory in `./backups`
- Dump every user database concurrently using `pg_dump`
- Dump global objects using `pg_dumpall --globals-only`
- Ensure all dump logs are captured per-database
- Perform all jobs in a staging directory; mark status as OK only if all succeed.

---

## ♻️ Restore Example

```bash
pgdump-each restore \
  --connstr "postgres://postgres:newpass@new-cluster:5432/postgres?sslmode=disable" \
  --input ./backups/20250328154501.dmp
```

- Validates that the target cluster is empty (no user databases)
- Restores globals and all database dumps concurrently using `pg_restore`
- Logs progress and errors per database

---

## ✅ Requirements

- PostgreSQL client binaries in your `$PATH` (`pg_dump`, `pg_dumpall`, `pg_restore`, `psql`)
- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` — auto-inferred from `--connstr`

---

## Installation

### Manual Installation

1. Download the latest binary for your platform from
   the [Releases page](https://github.com/hashmap-kz/pgdump-each/releases).
2. Place the binary in your system's `PATH` (e.g., `/usr/local/bin`).

### Homebrew installation

```bash
brew tap hashmap-kz/pgdump-each
brew install pgdump-each
```

---

## 🚀 Roadmap

- [x] Concurrent logical backup
- [x] Dump global objects (`pg_dumpall --globals-only`)
- [x] Concurrent logical restore using `pg_restore`
- [x] Restore safety check (refuse to restore if cluster has databases)
- [ ] Configurable parallelism (`--jobs`, `--max-concurrency`)

---

## 📂 Backup Directory Structure

```
./backups/20250328154501.dmp/
├── globals.sql
├── mydb1.dmp/
│   ├── data/
│   ├── checksums.txt
│   └── dump.log
├── mydb2.dmp/
│   ├── data/
│   ├── checksums.txt
│   └── dump.log
...
```

---

## 📘 License

MIT License. Use freely at your own risk. Contributions welcome!
