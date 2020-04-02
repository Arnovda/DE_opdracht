CREATE DATABASE IF NOT EXISTS forum;

use forum;

drop table IF EXISTS mail;

create table IF NOT EXISTS mail(
    email varchar (255),
    creationDate int,
    counts int
)



