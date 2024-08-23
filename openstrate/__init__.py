import os
from openstrate import *


dir_path = os.path.dirname(f'{os.path.realpath(__file__)}') + '/'
if not os.path.exists(f'{dir_path}.env'):
    print('No environment file provided, let\'s generate a new environment file.')
    with open(file=f'{dir_path}.env', mode='w', encoding='utf8') as env:
        env.write(f'API_HOST = "{input("Domain of Host: ")}" \n')
        env.write(f'ACCESS_TOKEN = "{input("API Token: ")}" \n')
    env.close()

assert os.path.exists(f'{dir_path}.env')
