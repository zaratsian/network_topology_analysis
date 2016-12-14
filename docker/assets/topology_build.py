import sys, json

fn = sys.argv[1]
with open(fn, 'r') as f: lines = f.read().split('\n')

# headers
fields = lines[0].strip().split(',')

# build list of pois
pois = {}
for line in lines[1:]:
  tokens = line.strip().split(',')
  poi = {}
  for idx, field in enumerate(fields):  poi[field] = tokens[idx]
  poi['NODE_PATH'] = []
  poi['CHILDREN'] = {}
  pois[poi['ID']] = poi

with open('out', 'w') as f:
  i = 0
  for poi_id in pois:
    base = pois[poi_id]
    poi = base
    while(poi['UPSTREAM_DEVICE_ID'] in pois):
      base['NODE_PATH'].append(poi['UPSTREAM_DEVICE_ID'])
      if (i == 0): print('Appended to ' + base['ID'] + ': ' + str(base)+'\n')
      poi = pois[poi['UPSTREAM_DEVICE_ID']]
    base['NODE_PATH'] = '|'.join(base['NODE_PATH'])
    if (i == 0): print(str(base))
    i += 1
    f.write(json.dumps(base)+'\n')
