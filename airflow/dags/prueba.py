import os
#print(os.getcwd())
#print(os.path.dirname(os.path.realpath(__file__)).replace('/dags',''))

print(os.path.isfile(os.path.dirname(os.path.realpath(__file__)).replace('/dags','/datasets/GBUSalvador.csv')))