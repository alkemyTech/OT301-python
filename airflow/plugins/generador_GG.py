from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir=os.path.dirname(os.path.abspath(__file__))
airflow_path=os.path.abspath(os.path.join(file_dir, os.pardir))
dags_path=airflow_path+'/dags/'
env=Environment(loader=FileSystemLoader(file_dir))
template=env.get_template('template_GG.jinja2')

for filename in os.listdir(file_dir):
    if filename.endswith('.yaml'):
        with open (f'{file_dir}/{filename}','r') as configfile:
            config=yaml.safe_load(configfile)
            with open(f"{dags_path}/DD_{config['dag_id']}_dag_etl.py","w") as f:
                f.write(template.render(config))