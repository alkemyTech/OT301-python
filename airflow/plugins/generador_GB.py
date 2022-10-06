#Este .py será el encargado de generar todos los dags en sus propios .py desde la platilla
#y los datos de configuración de las universidades almacenados en los .yaml
from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))  
env = Environment(loader=FileSystemLoader(file_dir))   
template = env.get_template('template_GB.jinja2')
dags_dir = file_dir.replace('/plugins','/dags')   #Guardamos el path de los dags
for filename in os.listdir(file_dir):             #Recorremos los archivos en el directorio
    if filename.endswith('.yaml'):                #Si es un .yaml, le extraemos los datos
        with open(f'{file_dir}/{filename}','r') as configfile:
            config = yaml.safe_load(configfile)
            id = config['id'].replace("'","")
            with open(f'{dags_dir}/DD_{id}_dag_etl.py','w') as f:
                f.write(template.render(config))  #Generamos cada dag con los datos previos