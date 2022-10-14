''' OT301-89
COMO: Analista de datos
QUIERO: Arreglar un Dag dinamico
PARA: Poder ejecutarlos normalmente
Criterios de aceptación: 
- El DAG a arreglar es el que procesa las siguientes universidades:
- Universidad Nacional De La Pampa
- Universidad Abierta Interamericana- Se debe arreglar el DAG factory
- No se debe tocar la lógica de procesamiento o negocio'''

from jinja2 import Environment, FileSystemLoader

import yaml

import os

from pathlib import Path

# list made to run only the universities of group E
list = ['config_GEUAbiertaInteramericana.yaml', 'config_GEUNDeLaPampa.yaml']

# folder where this file is located
file_dir = os.path.dirname(os.path.abspath(__file__))

# folder where the airflow project is located
airflow_folder = Path(__file__).resolve().parent.parent

# the parameters are set to execute the ".jinja2" file
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template_GE.jinja2')

# The files to be executed are traversed to build the dynamic DAG
for filename in os.listdir(file_dir):

    if filename in list: # to run only the universities of the list (Group E)
    # if filename.endswith('.yaml'):
        with open(f'{file_dir}/{filename}','r') as configfile:
            config = yaml.safe_load(configfile)
            with open(f"{airflow_folder}/dags/DD_{config['dag_id']}.py", "w") as file:
                file.write(template.render(config))