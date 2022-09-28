/*Sentencia de SQL para Universidad Nacional de La Pampa
OBJETIVO: Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021.*/

SELECT 
	fechaiscripccion,
	universidad as university,
	carrerra,
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
	universidad like '%pampa%' and
	to_DATE(fechaiscripccion,'DD/MM/YYYY') >= to_DATE('01/09/2020','DD/MM/YYYY') and
	to_DATE(fechaiscripccion,'DD/MM/YYYY') < to_DATE('01/02/2021','DD/MM/YYYY');