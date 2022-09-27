#Consulta SQL Universidad Palermo

select
	names as nombre_y_apellido,
	sexo,
	0 as age,
	correos_electronicos as email,
	birth_dates as fecha_nacimiento,
	universidad,
	to_date(fecha_de_inscripcion , 'YY-Mon-DD')as fecha_de_inscripcion ,
	careers as carrera,
	codigo_postal,
	null as locacion
FROM 
	public.palermo_tres_de_febrero
where 
	fecha_de_inscripcion  >='20-Sep-01'
	and fecha_de_inscripcion  <='21-Feb-01'
