import os

import cx_Oracle
from dotenv import load_dotenv

from shapely.wkb import loads
import geopandas as gpd

import cx_Oracle as oracledb

load_dotenv()

elms_projection = 3112
sas_projection = 4283

dsn = cx_Oracle.makedsn(os.getenv('DB_HOSTNAME', None), os.getenv('DB_PORT', None), sid=os.getenv('DB_SID', None))
connection = cx_Oracle.connect(user=os.getenv('DB_USER', None), password=os.getenv('DB_PASS', None), dsn=dsn, encoding="UTF-8")

cursor = connection.cursor()

print(connection)
print(cursor)

def output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type == oracledb.BLOB:
        return cursor.var(oracledb.LONG_BINARY, arraysize=cursor.arraysize)
connection.outputtypehandler = output_type_handler

# acquire types used for creating SDO_GEOMETRY objects
type_obj = connection.gettype("MDSYS.SDO_GEOMETRY")
element_info_type_obj = connection.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
ordinate_type_obj = connection.gettype("MDSYS.SDO_ORDINATE_ARRAY")

cursor.execute("""select NBN_LOCATION_PID, lat, lng from (select l.nbn_location_pid NBN_LOCATION_PID, SDO_CS.TRANSFORM(l.search_point,4283).sdo_point.y lat, SDO_CS.TRANSFORM(l.search_point,4283).sdo_point.x lng from location l where rownum < 50)""")
gdf = gpd.GeoDataFrame(cursor.fetchall(), columns=['NBN_LOCATION_PID', 'lat', 'lng'])

print(gdf)