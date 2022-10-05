#Consulta SQL Universidad Palermo

	select
names as last_name,
null as first_name,
sexo as gender,
null as age,
birth_dates,
correos_electronicos as email,
universidad as university,
to_date(fecha_de_inscripcion  , 'YY-Mon-DD')as inscription_date,
careers as career,
codigo_postal as postal_code,
null as location
	FROM 
public.palermo_tres_de_febrero
	where 
to_DATE(fecha_de_inscripcion ,'YY/Mon/DD') >= to_DATE('01/09/2020','DD/MM/YYYY') and
to_DATE(fecha_de_inscripcion ,'YY/Mon/DD') < to_DATE('01/02/2021','DD/MM/YYYY')
