drop table if exists in_oltp_logs;
drop table if exists in_olap_logs;


create table in_oltp_logs (
    id serial primary key,
    game_code varchar,
    field_name varchar,
    original_value text,
    level int,
    status int
);
create table in_olap_logs (
    id serial primary key,
    database_name varchar,
    table_name varchar,
    field_name varchar,
    original_value text,
    level int,
    status int
);