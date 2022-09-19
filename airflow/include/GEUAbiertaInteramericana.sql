/*Sentencia de SQL para Universidad Abierta Interamericana
OBJETIVO: Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021.*/

SELECT 
	inscription_dates,
	univiersities as university,
	carrera,
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
	univiersities like '%nteramericana%' and
	to_DATE(inscription_dates,'YY/Mon/DD') >= to_DATE('20/Sep/01','YY/Mon/DD') and
	to_DATE(inscription_dates,'YY/Mon/DD') < to_DATE('21/Feb/01','YY/Mon/DD');