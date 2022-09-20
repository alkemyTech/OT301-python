-- Código de consulta para la universidad de Morón
-- Alumnos inscriptos entre el 1/09/2020 y 1/02/2021

SELECT 
	inscription_dates,
	univiersities as university,
	carrera as career,
	null as first_name,
	names as last_name,
	sexo as gender,
	null as age,
	null as postal_code,
	localidad as location,
	email
FROM 
	public.rio_cuarto_interamericana
WHERE
	univiersities like '%cuarto%' and
	to_DATE(inscription_dates,'YY/Mon/DD') >= to_DATE('20/Sep/01','YY/Mon/DD') and
	to_DATE(inscription_dates,'YY/Mon/DD') < to_DATE('21/Feb/01','YY/Mon/DD');