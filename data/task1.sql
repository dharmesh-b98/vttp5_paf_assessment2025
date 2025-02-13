drop database if exists movies;

-- create database
-- create database if not exists movies;
create database movies;

-- select database
use movies;

-- create the table
create table imdb(
    imdb_id varchar(16) not null,
    vote_average float default 0,
    vote_count int default 0,
    release_date date not null,
    revenue decimal(15,2) default 1000000,
    budget decimal(15,2) default 1000000,
    runtime int default 90,
           
    constraint pk_imdb_id primary key (imdb_id)
);

-- grant test fella access to the database
grant all privileges on movies.* to 'testuser'@'%';
flush privileges;