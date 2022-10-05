--Consulta SQL Universidad Jujuy

	SELECT 
nombre as last_name,
sexo as gender,
null as age, 
email,
birth_date, 
university, 
inscription_date,
career, 
null as first_name,
location,
-1 as postal_code
	FROM 
public.jujuy_utn
	where  
to_DATE(inscription_date,'YYYY/MM/DD') >= to_DATE('01/09/2020','DD/MM/YYYY') and
to_DATE(inscription_date,'YYYY/MM/DD') < to_DATE('01/02/2021','DD/MM/YYYY')
