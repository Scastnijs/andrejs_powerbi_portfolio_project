create schema if not exists dw;

SET autocommit = 1;

create table dw.dim_geo
(
	geo_key int auto_increment primary key,
	geo_code varchar(3) not null unique,
    geo_name varchar(100) not null,
    geo_type varchar(30) not null,
	iso2 char(2),
    iso3 char(3),
    continent_code char(2),
    region varchar(60),
    is_eu_member_current boolean default false,
    eu_join_year int default null,
    eu_leave_year int default null
);

insert into dw.dim_geo 
(geo_code,geo_name,geo_type,iso2,iso3,continent_code,
region,is_eu_member_current,eu_join_year,eu_leave_year)
values ('EU','European Union','aggregate',null,null,null,
'Europe',null,null,null);

create table dw.dim_unit (
    unit_key int auto_increment primary key,
    unit_code varchar(30) not null unique,
    unit_name varchar(100) not null
);

insert into dw.dim_unit (unit_code, unit_name)
values
    ('NR', 'Number'),
    ('PC', 'Percent'),
    ('THS_PER', 'Thousand persons'),
    ('LT', 'Litres');

create table dw.dim_time
(
	time_key int auto_increment primary key,
	time_period varchar(20),
	year int,
	quarter int,
	month int,
	frequency varchar(10)
);

insert into dw.dim_time (year) values (year(CURRENT_DATE()));
insert into dw.dim_time (year) 
values (2025),(2024),(2023),(2022),(2021),(2020),(2019),(2018),(2017),(2016),(2015);

create table dw.dim_indicator
(
	indicator_key int auto_increment primary key,
	indicator_code varchar(50),
	indicator_name varchar(200),
	domain varchar(100)
);

insert ignore into dw.dim_indicator (indicator_code, indicator_name, domain)
values
    ('beer', 'beer', 'drinks'),
    ('spirit', 'spirit', 'drinks'),
    ('wine', 'wine', 'drinks'),
    ('total_litres', 'total_litres', 'drinks'),
    ('car_density', 'car_density', 'cars'),
    ('cars', 'cars', 'cars'),
    ('h_rank', 'happiness_rank', 'happiness'),
    ('h_score', 'happiness_score', 'happiness'),
    ('gdp_pc', 'gdp_per_capita', 'happiness'),
    ('family', 'family', 'happiness'),
    ('health', 'health', 'happiness'),
    ('freedom', 'freedom', 'happiness'),
    ('trust', 'trust', 'happiness'),
    ('generosity', 'generosity', 'happiness'),
    ('dystopia', 'dystopia', 'happiness'),
    ('country_pop', 'country_population', 'people'),
    ('city_pop', 'city_population', 'people');


create table dw.fact_observation_country
(
	geo_key int references dw.dim_geo(geo_key),
    indicator_key int references dw.dim_indicator(indicator_key),
    time_key int references dw.dim_time(time_key),
    unit_key int references dw.dim_unit(unit_key),
    value numeric(20,6),
    loaded_at timestamp default current_timestamp,
    primary key (geo_key, indicator_key, time_key, unit_key)	
);


create table dw.dim_city
(
	city_key int auto_increment primary key,
	geo_key int references dw.dim_geo(geo_key),
    city_name varchar(100) not null,
    city_ascii_name varchar(100) default null,
    subdivision_code varchar(6) default null,
    subdivision_name varchar(100) default null,
    subdivision_type varchar(100) default null,
    lat numeric(7,4),
    lng numeric(7,4),
    capital varchar(100)	
);

create table dw.fact_observation_city
(
	geo_key int references dw.dim_geo(geo_key),
	city_key int references dw.dim_city(city_key),
    indicator_key int references dw.dim_indicator(indicator_key),
    time_key int references dw.dim_time(time_key),
    unit_key int references dw.dim_unit(unit_key),
    value numeric(20,6),
    loaded_at timestamp default current_timestamp,
    primary key (geo_key, city_key, indicator_key, time_key, unit_key)	
);

CREATE OR REPLACE VIEW dw.v_country_alcohol_summary AS
SELECT
	geo_key,
    geo_code,
    alcohol_type,
    litres
FROM (
	SELECT
	b.geo_name,
	b.geo_code,
	b.geo_key,
	(
	 case when (b.beer > s.spirit) and (b.beer > w.wine) then 'beer'
			when (s.spirit > b.beer) and (s.spirit> w.wine) then 'spirit'
			else 'wine'
	 end
	) alcohol_type,
	(
	 case when (b.beer > s.spirit) and (b.beer > w.wine) then b.beer
			when (s.spirit > b.beer) and (s.spirit> w.wine) then s.spirit
			else w.wine
	 end
	) litres
	FROM
	(			
	select  
				dg.geo_name,
				dg.geo_code,
				dg.geo_key,
				fc.value as beer
			from dw.fact_observation_country fc
			left join dw.dim_geo dg 
				on fc.geo_key = dg.geo_key 
			left join dw.dim_indicator di  
				on fc.indicator_key = di.indicator_key
			left join dw.dim_time dt 
				on fc.time_key = dt.time_key
			where 
				dg.continent_code = 'EU'
				and di.indicator_name = 'beer') b
	LEFT JOIN
	(			
	select  
				dg.geo_name,
				fc.value as spirit
			from dw.fact_observation_country fc
			left join dw.dim_geo dg 
				on fc.geo_key = dg.geo_key 
			left join dw.dim_indicator di  
				on fc.indicator_key = di.indicator_key
			left join dw.dim_time dt 
				on fc.time_key = dt.time_key
			where 
				dg.continent_code = 'EU'
				and di.indicator_name = 'spirit') s
	ON b.geo_name = s.geo_name 
	LEFT JOIN
	(			
	select  
				dg.geo_name,
				fc.value as wine
			from dw.fact_observation_country fc
			left join dw.dim_geo dg 
				on fc.geo_key = dg.geo_key 
			left join dw.dim_indicator di  
				on fc.indicator_key = di.indicator_key
			left join dw.dim_time dt 
				on fc.time_key = dt.time_key
			where 
				dg.continent_code = 'EU'
				and di.indicator_name = 'wine') w
	ON b.geo_name = w.geo_name 
) alcohol_summary;

SET autocommit = 0;