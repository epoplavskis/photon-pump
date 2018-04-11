import json
import os


DIR_PATH = os.path.dirname(os.path.realpath(__file__))
GOSSIP_PATH = os.path.join(DIR_PATH, 'gossip.json')
CHAIR_PATH = os.path.join(DIR_PATH, 'chair.json')

with open(GOSSIP_PATH, 'r') as fgossip:
    GOSSIP = json.loads(fgossip.read())


with open(CHAIR_PATH, 'r') as fgossip:
    CHAIR = fgossip.read()
