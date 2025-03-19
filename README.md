# gopgdumps

### Настройка

- Исполняемый файл собирается в артефакты
- Переместить на нужный хост, распаковать, поместить в /usr/local/bin

### Создать файл конфигурации и запись cron, например так.

```
0 5 * * *   barman [ -x /usr/bin/barman ] && gopgdump -config /etc/gopgdump/config.yml
```

