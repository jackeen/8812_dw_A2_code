drop table if exists reviews;
drop table if exists game_platforms;
drop table if exists platforms;
drop table if exists game_languages;
drop table if exists languages;
drop table if exists game_developers;
drop table if exists developers;
drop table if exists game_publishers;
drop table if exists publishers;
drop table if exists package_items;
drop table if exists packages;
drop table if exists game_categories;
drop table if exists categories;
drop table if exists game_genres;
drop table if exists genres;
drop table if exists games;



create table games (
  id serial primary key,
  game_code varchar unique,
  game_name varchar,
  release_date date,
  required_age int,
  price decimal(10, 2),
  dlc_count int, -- Downloadable Content Count
  detailed_description text,
  short_description text,
  about_the_game text,
  header_image varchar,
  website varchar,
  support_url varchar,
  support_email varchar,
  metacritic_score int,
  metacritic_url varchar,
  achievements int,
  recommendations int,
  user_score int,
  positive int,
  negative int,
  average_playtime_forever int,
  median_playtime_forever int,
  estimated_owners_min int,
  estimated_owners_max int
);


create table reviews (
  id serial primary key,
  game_id int not null,
  original_content text,
  quote text,
  source varchar,
  rating decimal,
  out_of decimal,
  foreign key (game_id) references games(id) on delete cascade
);


create table platforms (
  id serial primary key,
  name varchar unique,
  code varchar
);
create table game_platforms (
  game_id int not null,
  platform_id int not null,
  primary key (game_id, platform_id),
  foreign key (game_id) references games(id) on delete cascade,
  foreign key (platform_id) references platforms(id) on delete cascade
);


create table languages (
  id serial primary key,
  name varchar unique,
  code varchar unique
);
create table game_languages (
  game_id int not null,
  language_id int not null,
  primary key (game_id, language_id),
  foreign key (game_id) references games(id) on delete cascade,
  foreign key (language_id) references languages(id) on delete cascade
);


create table developers (
  id serial primary key,
  name varchar unique
);
create table game_developers (
  game_id int not null,
  developer_id int not null,
  primary key (game_id, developer_id),
  foreign key (game_id) references games(id) on delete cascade,
  foreign key (developer_id) references developers(id) on delete cascade
);


create table publishers (
  id serial primary key,
  name varchar unique
);
create table game_publishers (
  game_id int not null,
  publisher_id int not null,
  primary key (game_id, publisher_id),
  foreign key (game_id) references games(id) on delete cascade,
  foreign key (publisher_id) references publishers(id) on delete cascade
);


create table categories (
  id serial primary key,
  name varchar unique
);
create table game_categories (
  game_id int not null,
  category_id int not null,
  primary key (game_id, category_id),
  foreign key (game_id) references games(id),
  foreign key (category_id) references categories(id)
);


create table genres (
  id serial primary key,
  name varchar unique
);
create table game_genres (
  game_id int not null,
  genres_id int not null,
  primary key (game_id, genres_id),
  foreign key (game_id) references games(id),
  foreign key (genres_id) references genres(id)
);





-- create table packages (
--   id serial primary key,
--   title varchar,
--   description text
-- );
-- create table package_items (
--   package_id int,
--   game_id int,
--   item_text varchar,
--   item_description text,
--   item_price decimal(10, 2),
--   primary key (package_id, item_text),
--   foreign key (package_id) references packages(id),
--   foreign key (game_id) references games(id)
-- );