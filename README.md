# gopgdumps

### Настройка

- Исполняемый файл собирается в артефакты
- Переместить на нужный хост, распаковать, поместить в /usr/local/bin

### Создать файл конфигурации и запись cron, например так.

```
0 5 * * * barman /usr/local/bin/gopgdump -config /etc/gopgdump/config.yml 1>/tmp/gopgdump.out.log 2>/tmp/gopgdump.err.log
```

