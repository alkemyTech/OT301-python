select
	universidades as university,
	carreras as career,
	fechas_de_inscripcion as inscription_date,
	'' as first_name,
	nombres as last_name,
	sexo as gender,
	'0' as age,
	codigos_postales as postal_code,
	direcciones as location,
	emails as email
from uba_kenedy uk  
where(
	to_date(fechas_de_inscripcion,'YY-Mon-DD') between to_date('20-Sep-01','YY-Mon-DD') and to_date('21-Feb-01','YY-Mon-DD')
	and universidades like '%kennedy%'
	)