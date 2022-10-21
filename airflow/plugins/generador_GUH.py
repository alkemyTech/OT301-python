from jinja2 import Environment, FileSystemLoader
import yaml
import os


file_dir = os.path.dirname(os.path.abspath(__file__))
dags_dir = file_dir.replace('/plugins', '/dags')
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template_GUH.jinja2')

for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"):
        with open(f'{file_dir}/{filename}', 'r') as configfile:
            config = yaml.safe_load(configfile)
            print(config)
            with open(f"{dags_dir}/DD_{config['dag_id']}.py", "w") as f:
                f.write(template.render(config))
