select (select rolsuper
        from pg_roles
        where rolname = current_user)      as connected_as_superuser,

       (select setting::int
        from pg_settings
        where name = 'server_version_num') as server_version_num,

       (select pg_read_file(setting, 0, (pg_stat_file(setting)).size)
        from pg_settings
        where name = 'config_file')        as postgresql_conf,

       (select pg_read_file(setting, 0, (pg_stat_file(setting)).size)
        from pg_settings
        where name = 'hba_file')           as pg_hba_conf
;