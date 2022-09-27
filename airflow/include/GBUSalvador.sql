    SELECT 
universidad as university, 
carrera as career, 
fecha_de_inscripcion as inscription_date, 
' ' as first_name, 
nombre as last_name, 
sexo as gender, 
-1 as age, 
-1 as postal_code, 
localidad as location, 
email 
    FROM 
public.salvador_villa_maria 
    WHERE 
universidad='UNIVERSIDAD_DEL_SALVADOR' and 
TO_DATE('01-Feb-21','DD-MON-YY') >= TO_DATE(fecha_de_inscripcion,'DD-MON-YY') and 
TO_DATE(fecha_de_inscripcion,'DD-MON-YY') >= TO_DATE('01-Sep-20','DD-MON-YY');