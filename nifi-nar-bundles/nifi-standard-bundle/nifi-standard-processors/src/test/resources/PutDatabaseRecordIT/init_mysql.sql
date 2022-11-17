create database putdatabaserecordtest;

create table putdatabaserecordtest.json_test (
    id int primary key auto_increment,
    msg json,
    msgb jsonb
);