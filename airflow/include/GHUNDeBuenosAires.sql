SELECT
	universidades AS university,
	carrerAS AS career,
	0 as first_name,
	nombres as last_name,
	to_date(fechas_de_inscripcion, 'yy-mon-dd') AS inscription_date,
	sexo AS gender,
	--DATE_PART('year', AGE(current_date ,TO_DATE(fechas_nacimiento, 'YY-Mon-DD'))) AS age,
	0 as age,
	codigos_postales AS postal_code,
	direcciones AS location,
	emails AS email
FROM
	uba_kenedy
WHERE
	universidades = 'universidad-de-buenos-aires' AND
	TO_DATE(fechas_de_inscripcion, 'yy-mon-dd') BETWEEN '01-09-2020' AND '01-02-2021';