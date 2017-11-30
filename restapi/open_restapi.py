#-------------------------------------------------------------------------------
# Open source version
# special thanks to geospatial python for shapefile module
#-------------------------------------------------------------------------------
from __future__ import print_function
import urllib
import shapefile
import shp_helper
import os
import json
import sys
from collections import OrderedDict
from rest_utils import *
from shapefile import shapefile

if sys.version_info[0] > 2:
    basestring = str

__opensource__ = True

# field types for shapefile module
SHP_FTYPES = munch.munchify({
          DATE_FIELD:'D',
          TEXT_FIELD:'C',
          FLOAT_FIELD:'F',
          DOUBLE_FIELD:'F',
          SHORT_FIELD:'N',
          LONG_FIELD:'N',
          GUID_FIELD:'C',
          RASTER_FIELD:'B',
          BLOB_FIELD: 'B',
          GLOBALID: 'C'
          })

def project(SHAPEFILE, wkid):
    """creates .prj for shapefile

    Required:
        SHAPEFILE -- full path to shapefile
        wkid -- well known ID for spatial reference
    """
    # write .prj file
    prj_file = os.path.splitext(SHAPEFILE)[0] + '.prj'
    with open(prj_file, 'w') as f:
        f.write(PROJECTIONS.get(str(wkid), '').replace("'", '"'))
    return prj_file


def exportReplica(replica, out_folder):
    """converts a restapi.Replica() to a Shapefiles

    replica -- input restapi.Replica() object, must be generated from restapi.FeatureService.createReplica()
    out_folder -- full path to folder location where new files will be stored.
    """
    if not hasattr(replica, 'replicaName'):
        print('Not a valid input!  Must be generated from restapi.FeatureService.createReplica() method!')
        return

    # attachment directory and gdb set up
    att_loc = os.path.join(out_folder, 'Attachments')
    if not os.path.exists(att_loc):
        os.makedirs(att_loc)

    # set schema and create feature classes
    for layer in replica.layers:

        # download attachments
        att_dict = {}
        for attInfo in layer.attachments:
            out_file = assign_unique_name(os.path.join(att_loc, attInfo[NAME]))
            with open(out_file, 'wb') as f:
                f.write(urllib.urlopen(attInfo['url']).read())
            att_dict[attInfo['parentGlobalId']] = out_file.strip()

        if layer.features:

            # make new feature class
            sr = layer.spatialReference

            out_fc = validate_name(os.path.join(out_folder, layer.name + '.shp'))
            g_type = G_DICT[layer.geometryType]

            # add all fields
            layer_fields = [f for f in layer.fields if f.type not in (SHAPE, OID)]
            w = shp_helper.ShpWriter(g_type, out_fc)
            guid = None
            field_map = []
            for fld in layer_fields:
                field_name = fld.name.split('.')[-1][:10]
                field_type = SHP_FTYPES[fld.type]
                if fld.type == GLOBALID:
                    guid = fld.name
                field_length = str(fld.length) if fld.length else "50"
                w.add_field(field_name, field_type, field_length)
                field_map.append((fld.name, field_name))

            w.add_field('ATTCH_PATH', 'C', '254')

            # search cursor to write rows
            s_fields = [f[0] for f in field_map]
            date_indices = [i for i,f in enumerate(layer_fields) if f.type == DATE_FIELD]

            for feature in layer.features:
                row = [feature[ATTRIBUTES][f] for f in s_fields]
                if guid:
                    row += [att_dict[feature[ATTRIBUTES][guid]]]
                for i in date_indices:
                    row[i] = mil_to_date(row[i])

                g_type = G_DICT[layer.geometryType]
                if g_type == 'Polygon':
                    geom = feature[GEOMETRY][RINGS]

                elif g_type == 'Polyline':
                     geom = feature[GEOMETRY][PATHS]

                elif g_type == 'Point':
                     geom = [feature[GEOMETRY][X], feature[GEOMETRY][Y]]

                else:
                    # multipoint - to do
                    pass

                w.add_row(geom, [v if v else ' ' for v in row])

            w.save()
            print('Created: "{0}"'.format(out_fc))

            # write projection file
            project(out_fc, sr)

    return out_folder

def partHandler(shape):
    """builds multipart features if necessary, returns parts
    as a list.

    Required:
        shape -- shapefile._Shape() object
    """
    parts = []
    if isinstance(shape, shapefile._Shape):
        if hasattr(shape, 'parts'):
            # add parts
            part_indices = shape.parts
            if len(part_indices) >= 2:
                parts = []
                st = 0
                for pi in part_indices[1:]:
                    parts.append(shape.points[st:pi])
                    st += pi
                    if pi == part_indices[-1]:
                        parts.append(shape.points[pi:])
                        break
            else:
                parts = [shape.points]
    elif isinstance(shape, list):
        # check if multipart
        if any(isinstance(i, list) for i in shape):
            part_indices = [0] + [len(i) for i in iter(shape)][:-1]
            if len(part_indices) >= 2:
                parts = []
                st = 0
                for pi in part_indices[1:]:
                    parts.extend(shape[st:pi])
                    st += pi
                    if pi == part_indices[1:]:
                        parts.extend(shape[pi:])
                        break
            else:
                parts = [shape]
        else:
            parts = [shape]
    else:
        raise IOError('Not a valid shapefile._Shape() input!')
    return parts

def find_ws_type(path):
    """gets a workspace for shapefile"""
    if os.path.isfile(path):
        find_ws(os.path.dirname(path))
    elif os.path.isdir(path):
        return (path, 'FileSystem')

class Geometry(BaseGeometry):
    """class to handle restapi.Geometry"""

    def __init__(self, geometry, **kwargs):
        """converts geometry input to restapi.Geometry object

        Required:
            geometry -- input geometry.  Can be arcpy.Geometry(), shapefile/feature
                class, or JSON
        """
        self._inputGeometry = geometry
        if isinstance(geometry, self.__class__):
            geometry = geometry.json
        spatialReference = None
        self.geometryType = None
        for k, v in kwargs.iteritems():
            if k == SPATIAL_REFERENCE:
                if isinstance(v, int):
                    spatialReference = v
                elif isinstance(v, basestring):
                    try:
                        # it's a json string?
                        v = json.loads(v)
                    except:
                        try:
                            v = int(v)
                            spatialReference = v
                        except:
                            pass

                if isinstance(v, dict):
                    spatialReference = v.get(LATEST_WKID) if v.get(LATEST_WKID) else v.get(WKID)

            elif k == GEOMETRY_TYPE and v.startswith('esri'):
                self.geometryType = v

        self.json = OrderedDict2()
        if isinstance(geometry, shapefile._Shape):
            if geometry.shapeType in (1, 11, 21):
                self.geometryType = ESRI_POINT
            elif geometry.shapeType in (3, 13, 23):
                self.geometryType = ESRI_POLYLINE
            elif geometry.shapeType in (5,15, 25):
                self.geometryType = ESRI_POLYGON
            elif self.geometryType in (8, 18, 28):
                self.geometryType = ESRI_MULTIPOINT
            if self.geometryType != ESRI_POINT:
                self.json[json_CODE[self.geometryType]] = partHandler(geometry.points)
            else:
                self.json = OrderedDict2(zip([X, Y], geometry.points[0]))

        elif isinstance(geometry, basestring):
            try:
                geometry = OrderedDict2(**json.loads(geometry))
            except:
                # maybe it's a shapefile/feature class?
                if arcpy.Exists(geometry):
                    desc = arcpy.Describe(geometry)
                    spatialReference = desc.spatialReference.factoryCode
                    self.geometryType = 'esriGeometry{}'.format(desc.shapeType.title())
                    with arcpy.da.SearchCursor(geometry, ['SHAPE@JSON']) as rows:
                        for row in rows:
                            esri_json = json.loads(row[0])
                            break

                    for k,v in sorted(esri_json.iteritems()):
                        if k != SPATIAL_REFERENCE:
                            self.json[k] = v
                    if SPATIAL_REFERENCE in esri_json:
                        self.json[SPATIAL_REFERENCE] = esri_json[SPATIAL_REFERENCE]
                else:
                    raise IOError('Not a valid geometry input!')

        if isinstance(geometry, dict):
            if SPATIAL_REFERENCE in geometry:
                sr_json = geometry[SPATIAL_REFERENCE]
                if LATEST_WKID in sr_json:
                    spatialReference = sr_json[LATEST_WKID]
                else:
                    try:
                        spatialReference = sr_json[WKID]
                    except:
                        raise IOError('No spatial reference found in JSON object!')
            if FEATURES in geometry:
                d = geometry[FEATURES][0]
                if GEOMETRY in d:
                    d = geometry[FEATURES][0][GEOMETRY]
                for k,v in d.iteritems():
                    self.json[k] = v
            elif GEOMETRY in geometry:
                for k,v in geometry[GEOMETRY].iteritems():
                    self.json[k] = v
            if not self.json:
                if RINGS in geometry:
                    self.json[RINGS] = geometry[RINGS]
                    self.geometryType = GEOM_DICT[RINGS]
                elif PATHS in geometry:
                    self.json[PATHS] = geometry[PATHS]
                    self.geometryType = GEOM_DICT[PATHS]
                elif POINTS in geometry:
                    self.json[POINTS] = geometry[POINTS]
                    self.geometryType = GEOM_DICT[POINTS]
                elif X in geometry and Y in geometry:
                    self.json[X] = geometry[X]
                    self.json[Y] = geometry[Y]
                    self.geometryType = ESRI_POINT
                elif all(map(lambda k: k in geometry, [XMIN, YMIN, XMAX, YMAX])):
                    for k in [XMIN, YMIN, XMAX, YMAX]:
                        self.json[k] = geometry[k]
                    self.geometryType = ESRI_ENVELOPE
                else:
                    raise IOError('Not a valid JSON object!')
            if not self.geometryType and GEOMETRY_TYPE in geometry:
                self.geometryType = geometry[GEOMETRY_TYPE]
        if not SPATIAL_REFERENCE in self.json and spatialReference is not None:
            self.spatialReference = spatialReference
        if not self.geometryType:
            if RINGS in self.json:
                self.geometryType = ESRI_POLYGON
            elif PATHS in self.json:
                self.geometryType = ESRI_POLYLINE
            elif POINTS in self.json:
                self.geometryType = ESRI_MULTIPOINT
            elif X in self.json and Y in self.json:
                self.geometryType = ESRI_POINT
            else:
                self.geometryType = NULL_GEOMETRY
        self.json = munch.munchify(self.json)

    @property
    def spatialReference(self):
        return self.getWKID()

    @spatialReference.setter
    def spatialReference(self, wkid):
        if isinstance(wkid, int):
            self.json[SPATIAL_REFERENCE] = {WKID: wkid}
        elif isinstance(wkid, dict):
            self.json[SPATIAL_REFERENCE] = wkid

    def envelope(self):
        """return an envelope from shape"""
        if self.geometryType != ESRI_POINT:
            coords = []
            for i in self.json[JSON_CODE[self.geometryType]]:
                coords.extend(i)
            XMin = min(g[0] for g in coords)
            YMin = min(g[1] for g in coords)
            XMax = max(g[0] for g in coords)
            YMax = max(g[1] for g in coords)
            return ','.join(map(str, [XMin, YMin, XMax, YMax]))
        else:
            return '{0},{1},{0},{1}'.format(self.json[X], self.json[Y])

    def envelopeAsJSON(self, roundCoordinates=False):
        """returns an envelope geometry object as JSON"""
        if self.geometryType != ESRI_ENVELOPE:
            flds = [XMIN, YMIN, XMAX, YMAX]
            if roundCoordinates:
                coords = map(int, [float(i) for i in self.envelope().split(',')])
            else:
                coords = self.envelope().split(',')
            d = dict(zip(flds, coords))
        else:
            d = self.json
        if self.json.get(SPATIAL_REFERENCE):
            d[SPATIAL_REFERENCE] = self.json[SPATIAL_REFERENCE]
        return d

    def asShape(self):
        """returns geometry as shapefile._Shape() object"""
        shp = shapefile._Shape(shp_helper.shp_dict[self.geometryType.split('Geometry')[1].upper()])
        if self.geometryType != ESRI_POINT:
            shp.points = self.json[JSON_CODE[self.geometryType]]
        else:
            shp.points = [[self.json[X], self.json[Y]]]

        # check if multipart, will need to fix if it is
        if any(isinstance(i, list) for i in shp.points):
            coords = []
            part_indices = [0] + [len(i) for i in iter(shp.points)][:-1]
##            for i in shp.points:
##                coords.extend(i)
##            shp.points = coords
            shp.parts = shapefile._Array('i', part_indices)
        else:
            shp.parts = shapefile._Array('i', [0])

        if shp.shapeType not in (0,1,8,18,28,31):
            XMin = min(coords[0] for coords in shp.points)
            YMin = min(coords[1] for coords in shp.points)
            XMax = max(coords[0] for coords in shp.points)
            YMax = max(coords[1] for coords in shp.points)
            shp.bbox = shapefile._Array('d', [XMin, YMin, XMax, YMax])

        return shp

    def __str__(self):
        """dumps JSON to string"""
        return self.dumps()

    def __repr__(self):
        """represntation"""
        return '<restapi.Geometry: {}>'.format(self.geometryType)


class GeometryCollection(object):
    """represents an array of restapi.Geometry objects"""
    geometries = []
    JSON = {GEOMETRIES: []}
    geometryType = None

    def __init__(self, geometries, use_envelopes=False):
        """represents an array of restapi.Geometry objects

        Required:
            geometries -- a single geometry or a list of geometries.  Valid inputs
                are a shapefile|feature class|Layer, geometry as JSON, or a restapi.Geometry or restapi.FeatureSet

        Optional:
            use_envelopes -- if set to true, will use the bounding box of each geometry passed in
                for the JSON attribute.
        """
        # it is a shapefile
        if os.path.exists(geometries) and geometries.endswith('.shp'):
            r = shapefile.Reader(geometries)
            self.geometries = [Geometry(s) for s in r.shapes]

        # it is already a list
        elif isinstance(geometries, list):

            # it is a list of restapi.Geometry() objects
            if all(map(lambda g: isinstance(g, Geometry), geometries)):
                self.geometries = geometries

            # it is a JSON structure either as dict or string
            elif all(map(lambda g: isinstance(g, (dict, basestring)), geometries)):

                # this *should* be JSON, right???
                try:
                    self.geometries = [Geometry(g) for g in geometries]
                except ValueError:
                    raise ValueError('Inputs are not valid ESRI JSON Geometries!!!')

        # it is a FeatureSet
        elif isinstance(geometries, FeatureSet):
            fs = geometries
            self.geometries = [Geometry(f.geometry, spatialReference=fs.getWKID(), geometryType=fs.geometryType) for f in fs.features]

        # it is a JSON struture of geometries already
        elif isinstance(geometries, dict) and GEOMETRIES in geometries:

            # it is already a GeometryCollection in ESRI JSON format?
            self.geometries = [Geometry(g) for g in geometries[GEOMETRIES]]

        # it is a single Geometry object
        elif isinstance(geometries, Geometry):
            self.geometries.append(geometries)

        # it is a single geometry as JSON
        elif isinstance(geometries, (dict, basestring)):

            # this *should* be JSON, right???
            try:
                self.geometries.append(Geometry(geometries))
            except ValueError:
                raise ValueError('Inputs are not valid ESRI JSON Geometries!!!')

        else:
            raise ValueError('Inputs are not valid ESRI JSON Geometries!!!')

        if self.geometries:
            self.JSON[GEOMETRIES] = [g.envelopeAsJSON() if use_envelopes else g.JSON for g in self.geometries]
            self.JSON[GEOMETRY_TYPE] = self.geometries[0].geometryType if not use_envelopes else ESRI_ENVELOPE
            self.geometryType = self.geometries[0].geometryType

    @property
    def count(self):
        return len(self)

    def __len__(self):
        return len(self.geometries)

    def __iter__(self):
        for geometry in self.geometries:
            yield geometry

    def __getitem__(self, index):
        return self.geometries[index]

    def __bool__(self):
        return bool(len(self.geometries))

    def __repr__(self):
        """represntation"""
        return '<restapi.GeometryCollection [{}]>'.format(self.geometryType)

class GeocodeHandler(object):
    """class to handle geocode results"""
    __slots__ = [SPATIAL_REFERENCE, 'results', FIELDS, 'formattedResults']

    def __init__(self, geocodeResult):
        """geocode response object handler

        Required:
            geocodeResult -- GeocodeResult object
        """
        self.results = geocodeResult.results
        self.spatialReference = geocodeResult.spatialReference[WKID]

    @property
    def fields(self):
        """returns collections.namedtuple with (name, type)"""
        res_sample = self.results[0]
        __fields = []
        for f, val in res_sample.attributes.iteritems():
            if isinstance(val, float):
                if val >= -3.4E38 and val <= 1.2E38:
                    __fields.append(FIELD_SCHEMA(name=f, type='F'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='D'))
            elif isinstance(val, (int, long)):
                __fields.append(FIELD_SCHEMA(name=f, type='I'))
            else:
                __fields.append(FIELD_SCHEMA(name=f, type='C'))
        return __fields

    @property
    def formattedResults(self):
        """returns a generator with formated results as Row objects"""
        for res in self.results:
            pt = (res.location[X], res.location[Y])
            yield (pt,) + tuple(res.attributes[f.name] for f in self.fields)

class Geocoder(GeocodeService):
    """class to handle Geocoding operations"""
    def __init__(self, url, usr='', pw='', token='', proxy=None):
        """Geocoder object, created from GeocodeService

        Required:
            url -- Geocode service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
        """
        super(Geocoder, self).__init__(url, usr, pw, token, proxy)

    @staticmethod
    def exportResults(geocodeResultObject, out_fc):
        """exports the geocode results to feature class

        Required:
            geocodeResultObject -- results from geocode operation, must be of type
                GeocodeResult.
            out_fc -- full path to output shapefile
        """
        if isinstance(geocodeResultObject, GeocodeResult):
            handler = GeocodeHandler(geocodeResultObject)
            if not handler.results:
                print('Geocoder returned 0 results! Did not create output')
                return None

            # create shapefile
            w = shp_helper.ShpWriter('POINT', out_fc)
            for field in handler.fields:
                w.add_field(field.name, field.type, 254)

            # add values
            for values in handler.formattedResults:
                w.add_row(values[0], values[1:])
            w.save()

            # project shapefile
            project(out_fc, handler.spatialReference)
            print('Created: "{}"'.format(out_fc))
            return out_fc

        else:
            raise TypeError('{} is not a {} object!'.format(geocodeResultObject, GeocodeResult))
