select
	university,
	career,
	inscription_date,
	null as first_name,
	nombre as last_name,
	sexo as gender,
	0 as age,
	null as postal_code,
	location,
	email

from
	training.public.jujuy_utn ju 

where
	university = 'universidad tecnológica nacional' and
	cast (inscription_date as date) between '2020-09-01' and '2021-02-01';