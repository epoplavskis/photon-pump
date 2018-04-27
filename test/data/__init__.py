import json
import os

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
GOSSIP_PATH = os.path.join(DIR_PATH, 'gossip.json')
CHAIR_PATH = os.path.join(DIR_PATH, 'chair.json')

with open(GOSSIP_PATH, 'r') as fgossip:
    GOSSIP = json.loads(fgossip.read())

with open(CHAIR_PATH, 'r') as fchair:
    CHAIR = fchair.read()


def get_state(idx):
    if idx == 0:
        return 'Master'

    if idx < 3:
        return 'Slave'

    return 'Clone'


def make_gossip(*args):
    return {
        'members': [
            {
                'state': get_state(idx),
                'isAlive': True,
                'externalTcpIp': addr,
                'internalTcpIp': addr,
                'internalHttpIp': addr,
                'externalHttpIp': addr,
                'externalTcpPort': 1113,
                'internalTcpPort': 1112,
                'internalHttpPort': 2112,
                'externalHttpPort': 2113
            } for idx, addr in enumerate(args)
        ]
    }
