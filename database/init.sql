-- root 
CREATE DATABASE maps; 
commit;

CREATE TABLE maps.customer_location
(
CUST_LOC_ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
CUST_FIRST_NAME varchar(20) NOT NULL,
CUST_LAST_NAME varchar(20) NULL,
CUST_COUNTRY varchar(50) NULL,
CUST_COUNTRY_CODE char(2) NULL,
CUST_CITY varchar(50) NULL,
CUST_STATE varchar(50) NULL,
CUST_STATE_CODE char(2) NULL
);

--drop table maps.customer_location;

--select * from maps.customer_location ;
/*
select 
city,
admin_name
from maps.worldcities w 
where iso3 = 'USA' and capital != ''
order by admin_name;
*/
CREATE VIEW v_customer_loc AS
select 
	COALESCE(w.city, w2.city) as city,
	COALESCE(w.admin_name, w2.admin_name) as state,
	COALESCE(w.lat, w2.lat) as lat,
	COALESCE(w.lng, w2.lng) as lng
from maps.customer_location cl
left join 
	(select 
	city,
	admin_name,
	lat,
	lng
	from maps.worldcities) w
on cl.CUST_CITY = w.city
	and cl.CUST_STATE = UPPER(w.admin_name)
left join 
	(select 
	city,
	admin_name,
	lat,
	lng
	from maps.worldcities
	where iso3 = 'USA' and capital != '') w2
on cl.CUST_STATE = UPPER(w2.admin_name)
;
