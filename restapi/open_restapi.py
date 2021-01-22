#-------------------------------------------------------------------------------
# Open source version
# special thanks to geospatial python for shapefile module
#-------------------------------------------------------------------------------
from __future__ import print_function
from . import shp_helper
import os
import json
import sys
from collections import OrderedDict
from .rest_utils import *
from .conversion import *
shapefile =  shp_helper.shapefile

from . import projections

import six
from six.moves import urllib

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
    """Creates .prj for shapefile.

    Args:
        SHAPEFILE: Full path to shapefile.
        wkid: Well known ID for spatial reference
    
    Returns:
        The project file.
    """

    # write .prj file
    prj_file = os.path.splitext(SHAPEFILE)[0] + '.prj'
    with open(prj_file, 'w') as f:
        f.write(projections.projections.get(str(wkid), '').replace("'", '"'))
    return prj_file


def exportReplica(replica, out_folder):
    """Converts a restapi.Replica() to a shapefile.

    Args:   
        replica: Input restapi.Replica() object, must be generated from restapi.FeatureService.createReplica()
        out_folder: Full path to folder location where new files will be stored.
    
    Returns:
        The output folder for the shapefile.
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
                f.write(urllib.request.urlopen(attInfo['url']).read())
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
    """Builds multipart features if necessary, returns parts
            as a list.

    Args:
        shape: shapefile.Shape() object.
    """

    parts = []
    if isinstance(shape, shapefile.Shape):
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
        raise IOError('Not a valid shapefile.Shape() input!')
    return parts

def find_ws_type(path):
    """Returns a workspace for shapefile.
    
    Args:
        path: The path for the workspace.
    """

    if os.path.exists(path) and os.path.isfile(path):
        return find_ws_type(os.path.dirname(path))
    elif os.path.isdir(path):
        return (path, 'FileSystem')
    else:
        return(os.path.dirname(path), 'FileSystem')

class Geometry(BaseGeometry):
    """Class to handle restapi.Geometry."""
    _native_format = GEOJSON_FORMAT

    def __init__(self, geometry, **kwargs):
        """Converts geometry input to restapi.Geometry object.

        Args:
            geometry: Input geometry.  Can be arcpy.Geometry(), shapefile/feature
                class, or JSON.
        """

        self._inputGeometry = geometry
        if isinstance(geometry, self.__class__):
            geometry = geometry.json
        spatialReference = None
        self.geometryType = None
        for k, v in six.iteritems(kwargs):
            if k == SPATIAL_REFERENCE:
                if isinstance(v, int):
                    spatialReference = v
                elif isinstance(v, six.string_types):
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
        if isinstance(geometry, shapefile.Shape):
            geometry = geometry.__geo_interface__

        elif isinstance(geometry, six.string_types):
            try:
                geometry = OrderedDict2(**json.loads(geometry))
            except:
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

            # first check for geojson
            if is_geojson(geometry):
                self.geometryType = GEOJSON_GEOMETRY_MAPPING.get(geometry.get(TYPE))
                self.json = geojson_to_arcgis(geometry)
                return
                
            if FEATURES in geometry:
                d = geometry[FEATURES][0]
                if GEOMETRY in d:
                    d = geometry[FEATURES][0][GEOMETRY]
                for k,v in six.iteritems(d):
                    self.json[k] = v
            elif GEOMETRY in geometry:
                for k,v in six.iteritems(geometry[GEOMETRY]):
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
    def _native_json(self):
        return arcgis_to_geojson(self.json) if is_arcgis(self.json) else self.json

    @property
    def _native_type(self):
        return self._native_json.get(TYPE)

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
        """Returns an envelope from shape."""
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
        """Returns an envelope geometry object as JSON.
        
        Args:
            roundCoordinates: Optional boolean that determines if the coordinates 
                are rounded, defaults to False.
        """

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
        """Returns geometry as shapefile.Shape() object."""
        return shapefile.Shape._from_geojson(self._native_json)

    def __str__(self):
        """Dumps JSON to string."""
        return self.dumps()

    def __repr__(self):
        """Representation."""
        return '<restapi.Geometry: {}>'.format(self.geometryType)


class GeometryCollection(object):
    """Represents an array of restapi.Geometry objects."""
    geometries = []
    JSON = {GEOMETRIES: []}
    geometryType = None

    def __init__(self, geometries, use_envelopes=False):
        """Represents an array of restapi.Geometry objects.
        
        Args:
            geometries: A single geometry or a list of geometries.  Valid inputs
                are a shapefile|feature class|Layer, geometry as JSON, or a 
                restapi.Geometry or restapi.FeatureSet.
            use_envelopes: Optional boolean, if set to true, will use the bounding 
                box of each geometry passed in for the JSON attribute. 
                Default is False.
        
        Raises:
            ValueError: 'Inputs are not valid ESRI JSON Geometries!!!'
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
            elif all(map(lambda g: isinstance(g, (dict, six.string_types)), geometries)):

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
        elif isinstance(geometries, (dict, six.string_types)):

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
        """Representation."""
        return '<restapi.GeometryCollection [{}]>'.format(self.geometryType)

class GeocodeHandler(object):
    """Class to handle geocode results."""
    # __slots__ = [SPATIAL_REFERENCE, 'results', 'formattedResults']

    def __init__(self, geocodeResult):
        """Geocode response object handler.

        Args:
            geocodeResult: GeocodeResult object.
        """

        self.results = geocodeResult.results
        self.spatialReference = geocodeResult.spatialReference[WKID]

    @property
    def fields(self):
        """Returns collections.namedtuple with (name, type)."""
        res_sample = self.results[0]
        __fields = []
        for f, val in six.iteritems(res_sample.attributes):
            if isinstance(val, float):
                if val >= -3.4E38 and val <= 1.2E38:
                    __fields.append(FIELD_SCHEMA(name=f, type='F'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='D'))
            elif isinstance(val, six.integer_types):
                __fields.append(FIELD_SCHEMA(name=f, type='I'))
            else:
                __fields.append(FIELD_SCHEMA(name=f, type='C'))
        return __fields

    @property
    def formattedResults(self):
        """Returns a generator with formated results as Row objects."""
        for res in self.results:
            pt = (res.location[X], res.location[Y])
            yield (pt,) + tuple(res.attributes[f.name] for f in self.fields)

class Geocoder(GeocodeService):
    """Class to handle Geocoding operations."""
    def __init__(self, url, usr='', pw='', token='', proxy=None):
        """Geocoder object, created from GeocodeService
        
        Args:
            url: Geocode service url.
        Below args only required if security is enabled:
            usr: Username credentials for ArcGIS Server. Defaults to ''.
            pw: Password credentials for ArcGIS Server. Defaults to ''.
            token: Token to handle security (alternative to usr and pw). 
                Defaults to ''.
            proxy: Optional boolean to use proxy page to handle security, need 
                to provide full path to proxy url. Defaults to None.
        """

        super(Geocoder, self).__init__(url, usr, pw, token, proxy)

    @staticmethod
    def exportResults(geocodeResultObject, out_fc):
        """Exports the geocode results to feature class.

        Args:
            geocodeResultObject: Results from geocode operation, must be of type
                GeocodeResult.
            out_fc: Full path to output shapefile.
        
        Raises:
            TypeError: '{} is not a {} object!'
        
        Returns:
            The path for the output shapefile.
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
