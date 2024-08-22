import os
from openstrate import *

if not os.path.exists('.env'):
    print('No environment file provided, let\'s generate a new environment file.')
    with open(file='.env', mode='w', encoding='utf8') as env:
        env.write(f'API_PROTOCOL = "{input("Protocol: ")}" \n')
        env.write(f'API_HOST = "{input("Domain of Host: ")}" \n')
        env.write(f'API_PORT = "{input("Service Port: ")}" \n')
        env.write(f'ACCESS_TOKEN = "{input("API Token: ")}" \n')
    env.close()
