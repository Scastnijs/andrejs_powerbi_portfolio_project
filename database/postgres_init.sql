
create schema if not exists staging;

create table if not exists staging.drinks
(
id int generated always as identity primary key,
country varchar(60) not null,
beer int,
spirit int,
wine int,
total_litres decimal(10,2),
continent char(2)
);

create table if not exists staging.expectancy
(
id int generated always as identity primary key,
country varchar(60) not null,
both_g decimal(10,2),
female decimal(10,2),
male decimal(10,2)
);

create table if not exists staging.worldcities 
(
id int generated always as identity primary key,
city varchar(60),
city_ascii varchar(60),
lat decimal(7,4),
lng decimal(7,4),
country varchar(50),
iso2 char(2),
iso3 char(3),
admin_name varchar(100),
capital varchar(10),
population int
);

create table if not exists staging.vehicles_country
(
id int generated always as identity primary key,
country varchar(60) not null,
per1kpeople int,
total int,
year int
);

create table if not exists staging.happiness
(
id int generated always as identity primary key,
year int,
country varchar(60),
region varchar(60),
rank int,
score decimal(10,5),
gdp_per_capita decimal(7,5),
family decimal(7,5),
health decimal(7,5),
freedom decimal(7,5),
trust decimal(7,5),
generosity decimal(7,5),
dystopia decimal(7,5)
);
