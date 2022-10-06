from re import template
from jinja2 import Environment, FileSystemLoader
import yaml
import os
from pathlib import Path

airflow = Path(__file__).resolve().parent.parent
list = ['config_GDUTN.yaml', 'config_GDUNTresDeFebrero.yaml']

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template_GD.jinja2')

for filename in os.listdir(file_dir):
    if filename in list:
        with open(f"{file_dir}/{filename}", "r") as configfile:
            config = yaml.safe_load(configfile)
            with open(f"{airflow}/dags/DD_{config['dag_name']}.py", "w") as file:
                file.write(template.render(config))