select (select rolsuper
        from pg_roles
        where rolname = current_user)     as connected_as_superuser,
       jsonb_build_object(
               'server_version_num',
               (select setting::int
                from pg_settings
                where name = 'server_version_num'),
               'postgresql.conf',
               (select pg_read_file(setting, 0, (pg_stat_file(setting)).size)
                from pg_settings
                where name = 'config_file'),
               'pg_hba.conf',
               (select pg_read_file(setting, 0, (pg_stat_file(setting)).size)
                from pg_settings
                where name = 'hba_file')) as configs
;