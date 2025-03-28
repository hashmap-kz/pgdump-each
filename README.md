# pgdump-each

`pgdump-each` is a CLI tool designed to simplify **PostgreSQL major version upgrades** by performing safe and concurrent
logical backups and restores of all databases in a cluster.

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
pgdump-each backup \
  --connstr "postgres://postgres:secret@old-cluster:5432/postgres?sslmode=disable" \
  --output ./backups
```

This will:

- Create a timestamped directory in `./backups`
- Dump every user database concurrently using `pg_dump`
- Dump global objects using `pg_dumpall --globals-only`
- Ensure all dump logs are captured per-database

---

## ♻️ Restore Example (Coming Soon)

```bash
pgdump-each restore \
  --connstr "postgres://postgres:newpass@new-cluster:5432/postgres?sslmode=disable" \
  --input ./backups/20250328154501.dmp
```

- Validates that the target cluster is empty (no user databases)
- Restores globals and all database dumps concurrently using `pg_restore`
- Logs progress and errors per database

> ✅ *This command is stubbed but not yet implemented.*

---

## ✅ Requirements

- PostgreSQL client binaries in your `$PATH` (`pg_dump`, `pg_dumpall`, and soon `pg_restore`)
- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` — auto-inferred from `--connstr`
- Go 1.21+ (to build from source)

---

## 🚀 Roadmap

- [x] Concurrent logical backup
- [x] Dump global objects (`pg_dumpall --globals-only`)
- [ ] Concurrent logical restore using `pg_restore`
- [ ] Configurable parallelism (`--jobs`, `--max-concurrency`)
- [ ] Restore safety check (refuse to restore if cluster has databases)

---

## 🛠 Build from Source

```bash
go build -o pgdump-each ./cmd/pgdump-each
```

---

## 👀 Logs

All logs are printed to stderr and saved as `dump.log` files inside each database’s dump directory.

---

## 📂 Backup Directory Structure

```
./backups/20250328154501.dmp/
├── globals.sql
├── mydb1.dmp/
│   ├── data/
│   └── dump.log
├── mydb2.dmp/
│   ├── data/
│   └── dump.log
...
```

---

## 📘 License

MIT License. Use freely at your own risk. Contributions welcome!
