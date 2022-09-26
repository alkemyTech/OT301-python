select 
	universities as university,
	careers as career,
	inscription_dates as inscription_date,
	'' as first_name,
	names as last_name,
	sexo as gender,
	'0' as age,
	'0' as postal_code,
	locations as location,
	emails as email 
from lat_sociales_cine lsc 
where( 
	to_date(inscription_dates,'DD-MM-YYYY') between to_date('01-09-2020','DD-MM-YYYY') and TO_DATE('01-02-2021','DD-MM-YYYY') 
	and universities  like '%LATINOAMERICANA%'
	)