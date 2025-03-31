### Connect

```
psql "postgres://postgres:postgres@localhost:15432/postgres"
psql "postgres://postgres:postgres@localhost:15433/postgres"
```

### Run/Cleanup

```
docker compose up -d
docker compose down -v
```

### Check/Stat

```
docker logs pg16 -f
docker system df -v | grep pg
```
