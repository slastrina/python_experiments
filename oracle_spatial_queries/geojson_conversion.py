import json
from geojson import FeatureCollection

elms_projection = 3112
sas_projection = 4283

with open('input.json') as f:
    input_json = json.load(f)

features = input_json['polygonDetails']

geojson_obj = geojson.loads(json.dumps(features))

geojson_obj.