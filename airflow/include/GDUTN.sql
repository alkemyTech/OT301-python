select
	university,
	career,
	inscription_date,
	nombre as last_name,
	null as first_name,
	sexo as gender,
	0 as age,
	null as postal_code,
	location,
	email,
	birth_date

from
	training.public.jujuy_utn ju 

where
	university = 'universidad tecnológica nacional' and
	cast (inscription_date as date) between '2020-09-01' and '2021-02-01'