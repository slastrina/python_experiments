import os
import json
import geojson
from shapely.geometry import shape

from timeit import default_timer as timer

import cx_Oracle
import geopandas as gpd
from dotenv import load_dotenv
load_dotenv()

def chunks(cur): # 256
    while True:
        rows=cur.fetchmany()
        if not rows: break;
        yield rows

elms_projection = 3112
sas_projection = 4283

with open('input.json') as f:
    input_json = json.load(f)

dsn = cx_Oracle.makedsn(os.getenv('DB_HOSTNAME', None), os.getenv('DB_PORT', None), sid=os.getenv('DB_SID', None))
connection = cx_Oracle.connect(user=os.getenv('DB_USER', None), password=os.getenv('DB_PASS', None), dsn=dsn, encoding="UTF-8")

cursor = connection.cursor()

# using a pandas dataframe
cursor.execute("""select NBN_LOCATION_PID, lat, lng 
                    from (select l.nbn_location_pid NBN_LOCATION_PID, SDO_CS.TRANSFORM(l.search_point,4283).sdo_point.y lat, SDO_CS.TRANSFORM(l.search_point,4283).sdo_point.x lng from location l 
                    where rownum < 100000)""")
start = timer()
gdf = gpd.GeoDataFrame(cursor.fetchall(), columns=['NBN_LOCATION_PID', 'lat', 'lng'])
end = timer()
print(end - start, len(gdf))

# using fetchmany on the cursor and managing the chunks in python
locations = []
cursor.execute("""select NBN_LOCATION_PID, lat, lng 
                    from (select l.nbn_location_pid NBN_LOCATION_PID, SDO_CS.TRANSFORM(l.search_point,4283).sdo_point.y lat, SDO_CS.TRANSFORM(l.search_point,4283).sdo_point.x lng from location l 
                    where rownum < 100000)""")
start = timer()
for i, chunk in enumerate(chunks(cursor)):
    for row in chunk:
        locations.append(row)
end = timer()
print(end - start, len(locations))
