select
	universidad as university,
	careers as career,
	fecha_de_inscripcion as inscription_date,
	null  as first_name,
	names as last_name,
	sexo as gender,
	0 as age,
	codigo_postal as postal_code,
	null as location,
	correos_electronicos as email

from
	training.public.palermo_tres_de_febrero ptdf

where
	universidad = 'universidad_nacional_de_tres_de_febrero' and
	cast (fecha_de_inscripcion as date) between '2020-09-01' and '2021-02-01';