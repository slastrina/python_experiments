import itertools
import json
import tempfile, os
import zipfile

import osr
import pyproj
import shapely
from shapely import wkt
from osgeo import ogr, gdal
from shapely.geometry import MultiPolygon
from shapely.ops import transform

ogr.UseExceptions()

VALID_GDAL_FORMATS = {
    extension
    for index in range(ogr.GetDriverCount())
    for extension in str(ogr.GetDriver(index).GetMetadataItem(gdal.DMD_EXTENSIONS) or '').upper().split()
}

class DataSourceObj:
    """
    An ogr wrapper to simplify metadata extraction
    """

    def __init__(self, path: str, filename: str, file_extension: str):
        self.map = None
        self.driver = None
        self._feature_count = None
        self._fields = None
        self.path = path
        self.filename = filename
        self.file_extension = file_extension
        self.open()

    def __repr__(self):
        return f"{self.filename}, {str(self.map)}"

    def open(self):
        source = ogr.Open(self.path, 0)
        if source is None:
            raise IOError(f'Invalid {self.path}')
        source_name = 'memData'
        outdriver = ogr.GetDriverByName('MEMORY')
        self.map = outdriver.CreateDataSource(source_name)
        outdriver.Open(source_name, 1)
        self.map.CopyLayer(source.GetLayer(0), 'layer', ['OVERWRITE=YES'])
        self.driver = source.GetDriver()

    @property
    def fields(self):
        """
        Returns a list of field objects for the given input
        :return:
        :rtype:
        """
        if self._fields:
            return self._fields
        else:
            layer = self.map.GetLayer(0)
            schema = []
            ldefn = layer.GetLayerDefn()
            for n in range(ldefn.GetFieldCount()):
                fdefn = ldefn.GetFieldDefn(n)
                schema.append(fdefn)
            self._fields = {field.name: field.GetFieldTypeName(field.type) for field in schema}
        return self._fields

    @property
    def driver_name(self):
        """
        Retrieves the driver name used to load the file
        :return: The name of the driver
        :rtype: str
        """
        return self.driver.name

    @property
    def features(self):
        """
        Returns all rows in geojson format via a generator function
        :return: generator outputting geojson dicts
        :rtype: generator
        """
        layer = self.map.GetLayer(0)
        for feature in layer:
            yield feature

    def get_sample_data(self, rows: int = 1):
        """
        Returns a generator sliced with x number of records
        :param rows:
        :type rows:
        :return:
        :rtype:
        """
        return itertools.islice(self.features, rows)

    @property
    def metadata(self):
        meta = {
            "filename": self.filename,
            "file_extension": self.file_extension,
            "driver": self.driver_name,
            "path": self.path,
            "fields": self.fields,
            "rows": self.feature_count,
            "srid": self.srid,
            "sample": [sample.ExportToJson(as_object=True)['properties'] for sample in list(self.get_sample_data(2))],
            "field_values": self.get_distinct_values(),
        }

        return meta

    @property
    def feature_count(self):
        if self._feature_count:
            return self._feature_count
        else:
            layer = self.map.GetLayer(0)
            self._feature_count = layer.GetFeatureCount()
        return self._feature_count

    def get_distinct_values(self, all=False, cutoff=40):
        fields = {}

        layer = self.map.GetLayer(0)
        for name, type in self.fields.items():
            if type in ['String', 'Integer64', 'Real'] or all:
                sql = f'SELECT DISTINCT {name} FROM {layer.GetName()} LIMIT {cutoff + 1}'
                result = self.map.ExecuteSQL(sql)
                values = [feature.GetField(0) for feature in result if feature.GetField(0)]
                if len(values) > cutoff:
                    print(f'skipping {name!r}, exceeds cutoff of ({cutoff})')
                    continue
                if not values:
                    print(f'skipping {name!r}, no values retrieved')
                    continue
                print(f'values for {name!r}: {values}')
                fields[name] = values
            else:
                print(f'skipping {name!r}, wrong data type ({type})')

        return fields

    @property
    def srid(self):

        #print(dir(self.map.GetLayer()))
        #print(self.map.GetLayer().GetSpatialRef())

        # proj_source = pyproj.CRS.from_string(str(self.map.GetLayer().GetSpatialRef()))
        # print(proj_source)
        # print(pyproj.Proj(str(self.map.GetLayer().GetSpatialRef())))
        #proj = osr.SpatialReference(self.map)

        #res = proj.GetAttrValue('AUTHORITY', 1)
        return str(self.map.GetLayer().GetSpatialRef())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.map = None

def get_map_objects(paths: list):
    """
    Given a list of valid paths, attempt to create list of valid ogr objects
    :param paths:
    :type paths:
    :return: list of ogr.DataSource
    :rtype: list
    """
    objects = []

    for path in paths:
        try:
            objects.append(DataSourceObj(*path))
        except IOError as ex:
            pass
        except Exception as ex:
            print(ex)

    return objects


def discover_source(path: str):
    """
    Given a path to a zip or valid map file will return a list of paths to be used with ogr/gdal
    :param path: path to input file
    :type path: str
    :return: list of tuples [(path, filename, file_extension)]
    :rtype:
    """
    paths = []

    try:
        with zipfile.ZipFile(path, 'r') as zf:
            paths = [(f"/vsizip/{path}/{name}", os.path.splitext(name)[0], os.path.splitext(name)[1]) for name in
                     zf.namelist() if
                     any(name.upper().endswith(ext.upper()) for ext in VALID_GDAL_FORMATS)]
    except:
        name, ext = os.path.splitext(path)
        if ext.upper() in VALID_GDAL_FORMATS:
            paths.append((path, name, ext))

    return paths

#path = discover_source(os.path.join(os.path.expanduser('~'),'Desktop/ELMS_Map_Files/Australia World Heritage Areas.zip'))
path = discover_source(os.path.join(os.path.expanduser('~'),'Desktop/ELMS_Map_Files/vhi.zip'))
for map in get_map_objects(path):

    #print(json.dumps(map.metadata, indent=4))
    #print(map.srid)
    projector = pyproj.Transformer.from_proj(
        pyproj.Proj(map.map.GetLayer(0).GetSpatialRef().ExportToWkt()), # source coordinate system
        pyproj.Proj(f'epsg:3112') # destination coordinate system
    )

    for feature in map.features:
        #print(feature.autoidentifyEPSG())
        try:
            geom = shapely.wkt.loads(feature.GetGeometryRef().ExportToWkt())
            transformed = transform(projector.transform, geom)

            geom_multipolygon = transformed.simplify(0.5)

            print(shapely.wkt.dumps(geom_multipolygon))
        except Exception as ex:
            print('exception:', ex)


    #print(json.dumps(map.metadata, indent=4))



    # data = list(map.get_sample_data(1))[0]
    # for x in data:
    #     print(x)