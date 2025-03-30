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

## 🎬 Demo: dump cluster (version 16, port 5432) and restore on cluster (version 17, port 5433).

![pgdump-each demo GIF](assets/pgdump-each-usage.gif)

---

## ✨ Features

- Concurrent `pg_dump` of every non-template database in the cluster
- Dumps are stored in `--format=directory` with compression and parallelism
- Dumps global objects (roles, tablespaces, etc.) via `pg_dumpall --globals-only`
- Concurrent restore via `pg_restore`
- Safety: Refuses to restore if the target cluster is not empty
- KISS: It's not reinventing the wheel - just a handy wrapper around reliable PostgreSQL tools

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
- Perform all jobs in a staging directory; mark status as OK only if all succeed
- Record checksums for all files in the output directory

---

## ♻️ Restore Example

```bash
pgdump-each restore \
  --connstr "postgres://postgres:newpass@new-cluster:5432/postgres?sslmode=disable" \
  --input ./backups/20250328154501.dmp
```

- Validates that the target cluster is empty (no user databases)
- Verify all files in the input directory against `checksums.txt` before restore
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

#### Example installation script for Unix-Based OS _(requirements: tar, curl, jq)_:

```bash
(
set -euo pipefail

OS="$(uname | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$/arm64/')"
TAG="$(curl -s https://api.github.com/repos/hashmap-kz/pgdump-each/releases/latest | jq -r .tag_name)"

curl -L "https://github.com/hashmap-kz/pgdump-each/releases/download/${TAG}/pgdump-each_${TAG}_${OS}_${ARCH}.tar.gz" |
tar -xzf - -C /usr/local/bin && \
chmod +x /usr/local/bin/pgdump-each
)
```

### Homebrew installation

```bash
brew tap hashmap-kz/pgdump-each
brew install pgdump-each
```

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
