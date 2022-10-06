from jinja2 import Environment, FileSystemLoader
import yaml
import os
from pathlib import Path

file_dir= os.path.dirname(os.path.abspath(__file__))
sql_path= Path(__file__).resolve().parents[1]

print(file_dir)
print(sql_path)

env= Environment(loader= FileSystemLoader(file_dir))
template= env.get_template('template_GC.jinja2')

for filename in os.listdir(file_dir):
    if filename.endswith('.yaml'):

       
        
        with open (f'{file_dir}/{filename}','r') as configfile:
            config= yaml.safe_load(configfile)

            
            with open(f"{sql_path}/dags/DD_{config['sql_name']}.py","w") as f:
                f.write(template.render(config))