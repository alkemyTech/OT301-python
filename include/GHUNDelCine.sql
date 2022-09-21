SELECT
	universities AS university,
	careers AS career,
	names AS first_name,
	inscription_dates AS inscription_date,
	sexo AS gender,
	DATE_PART('year', AGE(current_date ,TO_DATE(birth_dates, 'DD-MM-YYYY'))) AS age,
	emails AS email,
	locations AS location
FROM
	lat_sociales_cine
WHERE
	universities  = 'UNIVERSIDAD-DEL-CINE' AND
	TO_DATE(inscription_dates, 'DD-MM-YY') BETWEEN '01-09-2020' AND '01-02-2021';