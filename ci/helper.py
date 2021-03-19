import os
import time
import json
import random
from pathlib import Path
from requests import get
from tomlkit import loads
from tomlkit import dumps


def main():
    config_file = Path('/etc/MASQ.toml')
    config = loads(config_file.read_text())

    print('\n>>> Preparing MASQ Node')
    time.sleep(1)
    

    # Get IP
    print('\n==>  Checking your external IP address ...')
    old_ip = config['ip']
    new_ip = get('https://api.ipify.org').text
    if not new_ip:
        if not old_ip:
            print('Failed! Please try again or set IP address manually!')
        else:
            print('Failed! Old IP address will be used!')
    elif new_ip != old_ip:
        config['ip'] = new_ip
        print(f'External IP updated: {old_ip} -> {new_ip}')
    else:
        print(f'External IP unchanged: {old_ip}')
    time.sleep(2)
    
    # Get neighbors
    print('\n==>  Getting the latest neighbors ...')
    old_neighbors = sorted(config['neighbors'].split(','))
    new_neighbors = json.loads(get('https://nodes.masq.ai/api/v0/nodes').text)
    new_neighbors = sorted([i['descriptor'] for i in new_neighbors])
    if not new_neighbors:
        if not old_neighbors:
            print('Failed! Please try again or set neighbors manually!')
        else:
            print('Failed! Old neighbors will be used!')
    elif new_neighbors != old_neighbors:
        config['neighbors'] = ','.join(new_neighbors)
        print(f'Saved {len(new_neighbors)} neighbors!')
    else:
        print(f'The original {len(old_neighbors)} neighbors unchanged')
    
    time.sleep(2)

    # Set ports
    clandestine_port = config['clandestine-port']
    print('\n==>  Setting clandestine port ...')
    print(clandestine_port)
    time.sleep(2)

    # Generate db password
    if not config['db-password']:
        print('\n==>  Generating password for db')
        password = ''.join([random.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%&amp;*1234567890") for i in range(32)])
        config['db-password'] = password
        print('*'*32)
        time.sleep(2)

    print('\n>>> Starting MASQ Node ...\n')
    time.sleep(4)

    # Save config
    config_file.write_text(dumps(config))

if __name__ == '__main__':
    main()
