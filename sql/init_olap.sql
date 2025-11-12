drop table if exists bridge_game_category;
drop table if exists bridge_game_genre;
drop table if exists bridge_game_language;

-- drop table if exists bridge_game_developer;
-- drop table if exists bridge_game_publisher;
-- drop table if exists bridge_game_platform;


drop table if exists fact_revenue;
drop table if exists dim_game;
drop table if exists dim_date;
drop table if exists dim_category;
drop table if exists dim_genre;
drop table if exists dim_language;

-- drop table if exists dim_developer;
-- drop table if exists dim_publisher;
-- drop table if exists dim_platform;




create table dim_date (
    id serial primary key,
    full_date date unique,
    year int,
    month int,
    day int
);

create table dim_language (
    id serial primary key,
    language_id int unique,
    name varchar unique
);

create table dim_genre (
    id serial primary key,
    genre_id int unique,
    name varchar unique
);

create table dim_category (
    id serial primary key,
    category_id int unique,
    name varchar unique
);

-- create table dim_publisher (
--     id serial primary key,
--     publisher_id int unique,
--     name varchar unique
-- );

-- create table dim_developer (
--     id serial primary key,
--     developer_id int unique,
--     name varchar unique
-- );
--
-- create table dim_platform (
--     id serial primary key,
--     platform_id int unique,
--     name varchar unique
-- );

create table dim_game (
    id serial primary key,
    game_id int unique,
    game_code varchar unique,
    game_name varchar,
    required_age int,
    price decimal(10, 2),
    support_url varchar,
    support_email varchar,
    user_score int,
    positive int,
    negative int,
    average_playtime_forever int,
    median_playtime_forever int,
    estimated_owners_min int,
    estimated_owners_max int
);

create table bridge_game_language (
    id serial primary key,
    game_sk int not null references dim_game(id),
    language_sk int not null references dim_language(id),
    unique (game_sk, language_sk)
);
create table bridge_game_genre (
    id serial primary key,
    game_sk int not null references dim_game(id),
    genre_sk int not null references dim_genre(id),
    unique (game_sk, genre_sk)
);
create table bridge_game_category (
    id serial primary key,
    game_sk int not null references dim_game(id),
    category_sk int not null references dim_category(id),
    unique (game_sk, category_sk)
);
-- create table bridge_game_developer (
--     id serial primary key,
--     game_sk int not null references dim_game(id),
--     developer_sk int not null references dim_developer(id),
--     unique (game_sk, developer_sk)
-- );
-- create table bridge_game_publisher (
--     id serial primary key,
--     game_sk int not null references dim_game(id),
--     publisher_sk int not null references dim_publisher(id),
--     unique (game_sk, publisher_sk)
-- );
-- create table bridge_game_platform (
--     id serial primary key,
--     game_sk int not null references dim_game(id),
--     platform_sk int not null references dim_platform(id),
--     unique (game_sk, platform_sk)
-- );

create table fact_revenue (
    id serial primary key,
    game_sk int references dim_game(id),
    date_sk int references dim_date(id),
    estimated_revenue_min decimal(20, 2),
    estimated_revenue_max decimal(20, 2)
);
