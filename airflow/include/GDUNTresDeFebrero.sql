select
	universidad as university,
	careers as career,
	fecha_de_inscripcion as inscription_date,
	names as last_name,
	null  as first_name,
	sexo as gender,
	0 as age,
	codigo_postal as postal_code,
	null as location,
	correos_electronicos as email,
	birth_dates

from
	training.public.palermo_tres_de_febrero ptdf

where
	universidad = 'universidad_nacional_de_tres_de_febrero' and
	cast (fecha_de_inscripcion as date) between '2020-09-01' and '2021-02-01'