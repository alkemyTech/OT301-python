SELECT 
	nombre as nombre_y_apellido,
	sexo,
	0 as age, 
	email, 
	to_date(birth_date, 'YYYY-MM-DD')as fecha_nacimiento, 
	university as universidad, 
	to_date(inscription_date, 'YYYY-MM-DD') as fecha_inscripcion,
	career as carrera, 
	"location" as locacion,
	null as codigo_postal 
FROM 
	public.jujuy_utn
where 
	inscription_date  >='2020-09-01'
	and inscription_date  <='2021-09-01'