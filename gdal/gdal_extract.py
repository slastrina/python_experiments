import itertools
import json
import tempfile, os
import zipfile

from osgeo import ogr, gdal

ogr.UseExceptions()

valid_formats = " ".join(
    [x for x in [ogr.GetDriver(i).GetMetadataItem(gdal.DMD_EXTENSIONS) for i in range(ogr.GetDriverCount())] if
     x]).split(" ")


class MapObject():
    map = None
    driver = None

    def __init__(self, path: str, filename: str, file_extension: str):
        self.path = path
        self.filename = filename
        self.file_extension = file_extension
        self.open()

    def __repr__(self):
        return f"{self.filename}, {str(self.map)}"

    def open(self):
        self.map = ogr.Open(self.path, 0)
        if self.map:
            self.driver = self.map.GetDriver()
            self.source_name = self.map.GetName()
        else:
            raise IOError(f'Invalid {self.path}')

    @property
    def fields(self):
        """
        Returns the field names for the given input
        :return:
        :rtype:
        """
        layer = self.map.GetLayer(0)
        schema = []
        ldefn = layer.GetLayerDefn()
        for n in range(ldefn.GetFieldCount()):
            fdefn = ldefn.GetFieldDefn(n)
            schema.append(fdefn.name)
        return(schema)

    @property
    def driver_name(self):
        """
        Retrieves the driver name used to load the file
        :return: The name of the driver
        :rtype: str
        """
        return self.driver.name

    @property
    def records(self):
        """
        Returns all rows in geojson format via a generator function
        :return: generator outputting geojson dicts
        :rtype: generator
        """
        layer = self.map.GetLayer(0)
        for i in range(layer.GetFeatureCount()):
            feature = layer.GetNextFeature()
            yield json.loads(feature.ExportToJson())

    def get_sample_data(self, rows: int = 1):
        """
        Returns a generator sliced with x number of records
        :param rows:
        :type rows:
        :return:
        :rtype:
        """
        return itertools.islice(self.records, rows)

    @property
    def metadata(self):
        return {
            "filename": self.filename,
            "file_extension": self.file_extension,
            "driver": self.driver_name,
            "path": self.source_name,
            "fields": self.fields,
            "rows": self.feature_count,
            "sample": [sample['properties'] for sample in list(self.get_sample_data(2))]
        }

    @property
    def feature_count(self):
        layer = self.map.GetLayer(0)
        return layer.GetFeatureCount()

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
            objects.append(MapObject(*path))
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
                     any(name.upper().endswith(ext.upper()) for ext in valid_formats)]
    except:
        name, ext = os.path.splitext(path)
        if ext.upper() in valid_formats:
            paths.append((path, name, ext))

    return paths

path = discover_source(os.path.join(os.path.expanduser('~'),'Desktop/ELMS_Map_Files/Australia World Heritage Areas.zip'))
#path = discover_source(os.path.join(os.path.expanduser('~'),'Desktop/ELMS_Map_Files/SAHeritagePlacesPoly_GDA94_State.zip'))
for map in get_map_objects(path):
    print(json.dumps(map.metadata, indent=4))