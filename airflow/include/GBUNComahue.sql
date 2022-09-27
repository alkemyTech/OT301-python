	SELECT 
universidad as university, 
carrera as career, 
fecha_de_inscripcion as inscription_date, 
' ' as first_name, 
name as last_name, 
sexo as gender, 
-1 as age, 
codigo_postal as postal_code, 
' ' as location, 
correo_electronico as email
	FROM 
public.flores_comahue
	WHERE 
universidad='UNIV. NACIONAL DEL COMAHUE' and 
TO_DATE('2021-02-21','YYYY-MM-DD') >= TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') and 
TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') >= TO_DATE('2020-09-01','YYYY-MM-DD');
