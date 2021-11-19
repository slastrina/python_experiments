import csv
import json
from shapely import wkb
import geojson
from shapely.geometry import shape, GeometryCollection

import re
regex_ = r".+::(\d+$)"

elms_projection = 3112
sas_projection = 4283

with open('input.json') as f:
    input_json = json.load(f)

polygon_details = input_json['polygonDetails']
# geojson_obj = geojson.loads(json.dumps(polygon_details))

# Note: geojson doesnt support crs anymore, deprecated as of 2016 geojson rfc https://datatracker.ietf.org/doc/html/rfc7946
# See sections B.1: https://datatracker.ietf.org/doc/html/rfc7946#appendix-B.1
#              4: https://datatracker.ietf.org/doc/html/rfc7946#section-4
# typically geojson imposes wgs84 for geojson, we will accept input crs manually
input_crs = re.search(regex_, polygon_details['crs']['properties']['name']).group(1)

print(input_crs)
print(polygon_details)

# features = GeometryCollection([shape(feature["geometry"]).buffer(0) for feature in geojson_obj['features']])
# print(features)

with open('test_ewkb.csv', "w") as fl:
    writer = csv.writer(fl, delimiter=",", lineterminator="\n")
    firstRow = True
    for f in polygon_details['features']:
        try:
            if firstRow:
                writer.writerow(['geom', 'type', 'id'])
                firstRow = False

            if f["geometry"]:
                o = shape(f["geometry"])
                g = o
                r = 0.0000001
                m = 1
                while g.wkb.__sizeof__() > 1048440: # allow for size of epsg - Redshift max geomtry size is 1048447
                    t = r*m
                    g = o.simplify(t)
                    m = m * 10
                writer.writerow([wkb.dumps(g, hex=True, srid=int(input_crs)), f['geometry']['type'], f['id']])
        except Exception:
            print(f"Error processing feature {f['id']}")
            raise