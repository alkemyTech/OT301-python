--Consulta SQL Universidad Jujuy

	SELECT 
nombre as nombre_y_apellido,
sexo,
0 as age, 
email, 
to_date(birth_date, 'YYYY-MM-DD')as fecha_nacimiento, 
university as universidad, 
inscription_date as fecha_inscripcion,
career as carrera, 
"location" as locacion,
-1 as codigo_postal 
	FROM 
public.jujuy_utn
	where  
to_DATE(inscription_date,'YYYY/MM/DD') >= to_DATE('01/09/2020','DD/MM/YYYY') and
to_DATE(inscription_date,'YYYY/MM/DD') < to_DATE('01/02/2021','DD/MM/YYYY')
