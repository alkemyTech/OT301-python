Mejoras del MapReduce utilizando Hadoop

Aquí se contiene los mapper y reducer necesarios para obtener un output de información mas ligero y procesado desde nuestros datos iniciales en posts.xml. Los archivos van de acuerdo a los mismos requisitos solicitados anteriormente y, de la misma forma, quedan listos para que a posterior se los termine de procesar y obtener las conjeturas o conclusiones pertinentes.

Para empezar, comenzaremos cargando en nuestro conteiner el archivo base posts.xml dentro de la carpeta input, y luego lo subiremos a Hadoop, ya que será desde el cual iremos trabajando. Cada requisito debe ejecutarse y procesarse de forma separa e independiente, reemplazando los mapper.py y reducer.py de acuerdo a los pasos indicados a continuación.
Ej.: docker cp LOCAL_PATH/mapperR1.py namenode:mapper.py

	1º Requisito: Top 10 tags sin respuestas aceptadas
Utilizaremos el mapper "mapperR1.py" y el reducer "reducerR1.py" para el procesamiento. El output que retornaremos seran todos los tags con esta condición, para que el próximo desarrollador eliga como llegar al top 10 y como mostrar la información.
key:values = tag:count

	2º Requisito: Relación entre cantidad de palabras en un post y su cantidad de visitas
En este caso, obtendremos estos atributos de cada post en nuestro output para poder ser procesados con mas detalles y llegar a la conclusión de la posible relación que tienen entre ellos.En este caso, solo ejecutaremos el mapper "mapperR2.py".
key:values = ViewCount:len_palabras

	3º Requisito: Puntaje promedio de las repuestas de las preguntas con más favoritos
Este requisito tendra dos output, que deben recolectarse por separado tambien.
El primero sera con el mapper "mapperR3A.py" que nos devolvera las preguntas junto a sus favoritos.
key:values = question_id:favoriteCount
Y segundo, con el "mapperR3B.py" y el "reducerR3B.py" que nos permitirá tener de cada pregunta sus puntajes.
key:values = current_id:mean
Esta información servirá para ser procesada nuevamente en busca de los promedios pudiendo seleccionar con mas criterio "las mas favoritas".
