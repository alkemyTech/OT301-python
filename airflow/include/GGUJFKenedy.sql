select
	universidades as university,
	carreras as career,
	fechas_de_inscripcion as inscription_date,
	split_part(nombres ,'-',1) as first_name,
	split_part(nombres ,'-',2) as last_name,
	sexo as gender,
	(
		split_part(to_char(to_date(fechas_de_inscripcion ,'YY-Mon-DD'),'YY'),'-',1)::INT+'100' - 
		split_part(to_char(to_date(fechas_nacimiento ,'YY-Mon-DD'),'YY'),'-',1)::INT
	) as age,
	codigos_postales as postal_code,
	'' as location,
	emails as email
	from uba_kenedy uk  
	where fechas_de_inscripcion between '20-Sep-01' and '21-Feb-01'
