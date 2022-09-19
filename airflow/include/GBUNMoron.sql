-- Código de consulta para la universidad de Morón
-- Alumnos inscriptos entre el 1/09/2020 y 1/02/2021

SELECT 
	fechaiscripccion as inscription_date,
	universidad as university,
	carrerra as career,
	null as first_name,
	nombrre as last_name,
	sexo as gender,
	null as age,
	codgoposstal as postal_code,
	null as location,
	eemail as email 
FROM 
	public.moron_nacional_pampa
WHERE
	universidad like '%morón%' and
	to_DATE(fechaiscripccion,'DD/MM/YYYY') >= to_DATE('01/09/2020','DD/MM/YYYY') and
	to_DATE(fechaiscripccion,'DD/MM/YYYY') < to_DATE('01/02/2021','DD/MM/YYYY');