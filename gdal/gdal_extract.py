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

    def close(self):
        self.map = None

    @property
    def fields(self):
        layer = self.map.GetLayer()
        schema = []
        ldefn = layer.GetLayerDefn()
        for n in range(ldefn.GetFieldCount()):
            fdefn = ldefn.GetFieldDefn(n)
            schema.append(fdefn.name)
        return(schema)

    @property
    def driver_name(self):
        return self.driver.name

    def get_sample_data(self):
        pass

    def generate_metadata(self):
        pass

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


paths = discover_source('/Users/samuel.275320/Desktop/ELMS_Map_Files/Australia World Heritage Areas.zip')
for x in get_map_objects(paths):
    print((x.fields))

print(valid_formats)

