select 
	universities as university,
	careers as career,
	inscription_dates as inscription_date,
	split_part(names,'-',1) as first_name,
	split_part(names,'-',2) as last_name,
	sexo as gender,
	to_char(age(to_date(inscription_dates,'DD-MM-YYYY'),to_date(birth_dates,'DD-MM-YYYY')), 'YYY') as age,
	'' as postal_code,
	locations as location,
	emails as email 
from lat_sociales_cine lsc 
where( 
	to_date(inscription_dates,'DD-MM-YYYY') between '01-09-2020' and '01-02-2021' 
	and universities  like '%LATINOAMERICANA%'
	)
