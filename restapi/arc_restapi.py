# proprietary version (uses arcpy)
from __future__ import print_function
import arcpy
import os
import time
import json
import sys
from .rest_utils import *

from . import six
from .six.moves import range
from .six.moves import urllib


arcpy.env.overwriteOutput = True
arcpy.env.addOutputsToMap = False

def find_ws_type(path):
    """determine output workspace (feature class if not FileSystem)
    returns a tuple of workspace path and type
    """
    # try original path first
    if not arcpy.Exists(path):
        path = os.path.dirname(path)

    desc = arcpy.Describe(path)
    if hasattr(desc, 'workspaceType'):
        return path, desc.workspaceType

    # search until finding a valid workspace
    split = filter(None, path.split(os.sep))
    if path.startswith('\\\\'):
        split[0] = r'\\{0}'.format(split[0])

    # find valid workspace
    for i in range(1, len(split)):
        sub_dir = os.sep.join(split[:-i])
        desc = arcpy.Describe(sub_dir)
        if hasattr(desc, 'workspaceType'):
            return sub_dir, desc.workspaceType

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
        spatialReference = self._find_wkid(geometry) if isinstance(geometry, dict) else None
        if spatialReference is None:
            spatialReference = self._find_wkid(kwargs)
        self.geometryType = kwargs.get(GEOMETRY_TYPE, '') if kwargs.get(GEOMETRY_TYPE, '').startswith('esri') else None
        self.json = munch.Munch()
        if isinstance(geometry, arcpy.mapping.Layer) and geometry.supports('DATASOURCE'):
            geometry = geometry.dataSource

        if isinstance(geometry, (arcpy.RecordSet, arcpy.FeatureSet)):
            fs = FeatureSet(geometry.JSON)
            geometry = fs.json
            spatialReference = fs.getSR()

        if isinstance(geometry, arcpy.Geometry):
            spatialReference = geometry.spatialReference.factoryCode
            self.geometryType = 'esriGeometry{}'.format(geometry.type.title())
            esri_json = json.loads(geometry.JSON)
            for k,v in sorted(six.iteritems(esri_json)):
                if k != SPATIAL_REFERENCE:
                    self.json[k] = v
            if SPATIAL_REFERENCE in esri_json:
                self.json[SPATIAL_REFERENCE] = spatialReference or self._find_wkid(esri_json)

        elif isinstance(geometry, six.string_types):
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

                    for k,v in sorted(six.iteritems(esri_json)):
                        if k != SPATIAL_REFERENCE:
                            self.json[k] = v
                    if SPATIAL_REFERENCE in esri_json:
                        self.json[SPATIAL_REFERENCE] = esri_json[SPATIAL_REFERENCE]
                else:
                    raise ValueError('Not a valid geometry input!')

        if isinstance(geometry, dict):
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
                if RINGS in geometry or CURVE_RINGS in geometry:
                    if RINGS in geometry:
                        self.json[RINGS] = geometry[RINGS]
                    else:
                        self.json[CURVE_RINGS] = geometry[CURVE_RINGS]
                    self.geometryType = GEOM_DICT[RINGS]
                elif PATHS in geometry or CURVE_PATHS in geometry:
                    if PATHS in geometry:
                        self.json[PATHS] = geometry[PATHS]
                    else:
                        self.json[CURVE_PATHS] = geometry[CURVE_PATHS]
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
                    raise ValueError('Not a valid JSON object!')
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
        self.hasCurves = CURVE_PATHS in self.json or CURVE_RINGS in self.json

    def envelope(self):
        """return an envelope from shape"""
        if self.geometryType != ESRI_ENVELOPE:
            e = arcpy.AsShape(self.json, True).extent
            return ','.join(map(str, [e.XMin, e.YMin, e.XMax, e.YMax]))
        else:
            return ','.join(map(str, [self.json[XMIN], self.json[YMIN], self.json[XMAX], self.json[YMAX]]))

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
        return munch.munchify(d)

    def asShape(self):
        """returns JSON as arcpy.Geometry() object"""
        if self.geometryType != ESRI_ENVELOPE:
            return arcpy.AsShape(self.json, True)
        else:
            ar = arcpy.Array([
                arcpy.Point(self.json[XMIN], self.json[YMAX]),
                arcpy.Point(self.json[XMAX], self.json[YMAX]),
                arcpy.Point(self.json[XMAX], self.json[YMIN]),
                arcpy.Point(self.json[XMIN], self.json[YMIN])
            ])
            return arcpy.Polygon(ar, arcpy.SpatialReference(self.spatialReference))

    def toPolygon(self):
        if hasattr(self, GEOMETRY_TYPE):
            if getattr(self, GEOMETRY_TYPE) == ESRI_ENVELOPE:
                ext = getattr(self, 'json')
                if ext:
                    rings = [[
                        [ext.get(XMIN, ext.get(YMIN))],
                        [ext.get(XMIN, ext.get(YMAX))],
                        [ext.get(XMAX, ext.get(YMAX))],
                        [ext.get(XMAX, ext.get(YMIN))],
                        [ext.get(XMIN, ext.get(YMIN))],
                    ]]

                    return Geometry({
                        SPATIAL_REFERENCE: self._spatialReference,
                        RINGS: rings
                    })


    def __str__(self):
        """dumps JSON to string"""
        return self.dumps()

    def __repr__(self):
        """represntation"""
        return '<{}.{}: {}>'.format(self.__module__, self.__class__.__name__, self.geometryType)

class GeometryCollection(BaseGeometryCollection):
    """represents an array of restapi.Geometry objects"""
    def __init__(self, geometries, use_envelopes=False, spatialReference=None):
        """represents an array of restapi.Geometry objects

        Required:
            geometries -- a single geometry or a list of geometries.  Valid inputs
                are a shapefile|feature class|Layer, geometry as JSON, or a restapi.Geometry or restapi.FeatureSet

        Optional:
            use_envelopes -- if set to true, will use the bounding box of each geometry passed in
                for the JSON attribute.
        """
        self.geometries = []
        if isinstance(geometries, self.__class__):
            self.geometries = geometries.geometries
            self.json = geometries.json
            self.geometryType = geometries.geometryType

        else:

            # if it is a dict, see if it is actually a feature set, then go through the rest of the filters
            if isinstance(geometries, dict)  and GEOMETRIES in geometries and not isinstance(geometries, FeatureSet):
                # it is already a GeometryCollection in ESRI JSON format?
                self.geometries = [Geometry(g) for g in geometries[GEOMETRIES]]

            # it is a layer or feature class/shapefile
            elif isinstance(geometries, arcpy.mapping.Layer):
                with arcpy.da.SearchCursor(geometries, ['SHAPE@']) as rows:
                    self.geometries = [Geometry(r[0]) for r in rows]

            elif isinstance(geometries, six.string_types):
                if (not geometries.startswith('{') or not geometries.startswith('[')) and arcpy.Exists(geometries):
                    with arcpy.da.SearchCursor(geometries, ['SHAPE@']) as rows:
                        self.geometries = [Geometry(r[0]) for r in rows]

                else:
                    gd = json.loads(geometries)
                    if isinstance(gd, (list, dict)):
                        self.geometries = self.__init__(gd)
                    else:
                        raise ValueError('Inputs are not valid ESRI JSON Geometries!!!')

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
                self.geometries.extend([Geometry(f.geometry, spatialReference=fs.getWKID(), geometryType=fs.geometryType) for f in fs.features])

            # it is a JSON struture of geometries already
            elif isinstance(geometries, dict):
                if GEOMETRIES in geometries:
                    # it is already a GeometryCollection in ESRI JSON format?
                    self.geometries = [Geometry(g) for g in geometries.get(GEOMETRIES, [])]
                elif FEATURES in geometries:
                    self.geometries = [Geometry(g) for g in geometries.get(FEATURES, [])]
                else:
                    raise ValueError('Inputs are not valid ESRI JSON Geometries!!!')

            # it is a single Geometry object
            elif isinstance(geometries, Geometry):
                self.geometries.append(geometries)

            if self.geometries:
                self.json[GEOMETRIES] = []
                for g in self.geometries:
                    if not g.spatialReference:
                        g.spatialReference = spatialReference
                    self.json[GEOMETRIES].append(g.envelopeAsJSON() if use_envelopes else g.json)

                self.json[GEOMETRY_TYPE] = self.geometries[0].geometryType if not use_envelopes else ESRI_ENVELOPE
                self.geometryType = self.geometries[0].geometryType
                if not spatialReference:
                    self.spatialReference = self.geometries[0].spatialReference

            elif not self.geometries:
                raise ValueError('Inputs are not valid ESRI JSON Geometries!!!')

            self.json = munch.munchify(self.json)

    @property
    def spatialReference(self):
        try:
            return self.geometries[0].spatialReference
        except IndexError:
            return None

    @spatialReference.setter
    def spatialReference(self, wkid):
        for g in self.geometries:
            g.spatialReference = wkid

class GeocodeHandler(object):
    """class to handle geocode results"""

    def __init__(self, geocodeResult):
        """geocode response object handler

        Required:
            geocodeResult -- GeocodeResult object
        """
        self.results = geocodeResult.results
        self.spatialReference = geocodeResult.spatialReference

    @property
    def fields(self):
        """returns collections.namedtuple with (name, type)"""
        res_sample = self.results[0]
        __fields = []
        for f, val in six.iteritems(res_sample.attributes):
            if isinstance(val, float):
                if val >= -3.4E38 and val <= 1.2E38:
                    __fields.append(FIELD_SCHEMA(name=f, type='FLOAT'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='DOUBLE'))
            elif isinstance(val, six.integer_types):
                if abs(val) < 32768:
                    __fields.append(FIELD_SCHEMA(name=f, type='SHORT'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='LONG'))
            else:
                __fields.append(FIELD_SCHEMA(name=f, type='TEXT'))
        return __fields


    @property
    def formattedResults(self):
        """returns a generator with formated results as tuple"""
        for res in self.results:
            pt = arcpy.PointGeometry(arcpy.Point(res.location[X],
                                                 res.location[Y]),
                                                 self.spatialReference)

            yield (pt,) + tuple(res.attributes[f.name] for f in self.fields)

class Geocoder(GeocodeService):

    @staticmethod
    def exportResults(geocodeResultObject, out_fc):
        """exports the geocode results (GeocodeResult object) to feature class

        Required:
            geocodeResultObject -- results from geocode operation, must be of type
                GeocodeResult.
            out_fc -- full path to output feature class
        """
        if isinstance(geocodeResultObject, GeocodeResult):
            handler = GeocodeHandler(geocodeResultObject)
            if not handler.results:
                print('Geocoder returned 0 results! Did not create output')
                return None

            # make feature class
            path, name = os.path.split(out_fc)
            arcpy.management.CreateFeatureclass(path, name, 'POINT', spatial_reference=handler.spatialReference)
            for field in handler.fields:
                arcpy.management.AddField(out_fc, field.name, field.type, field_length=254)

            # add records
            fields = ['SHAPE@'] + [f.name for f in handler.fields]
            with arcpy.da.InsertCursor(out_fc, fields) as irows:
                for values in handler.formattedResults:
                    irows.insertRow(values)
            print('Created: "{}"'.format(out_fc))
            return out_fc

        else:
            raise TypeError('{} is not a {} object!'.format(geocodeResultObject, GeocodeResult))

# ARCPY UTILITIES - only available here
def create_empty_schema(feature_set, out_fc):
    # make copy of feature set
    fs = feature_set.getEmptyCopy()
    try:
        try:
            # this tool has been very buggy in the past :(
            tmp = fs.dump(tmp_json_file(), indent=None)
            arcpy.conversion.JSONToFeatures(tmp, out_fc)
        except:
            # this isn't much better..
            gp = arcpy.geoprocessing._base.Geoprocessor()

            # create arcpy.FeatureSet from raw JSON string
            arcpy_fs = gp.fromEsriJson(fs.dumps(indent=None))
            arcpy_fs.save(out_fc)

    except:
        # manually add records with insert cursor, this is SLOW!
        print('arcpy conversion failed, manually writing features...')
        outSR = arcpy.SpatialReference(fs.getSR())
        path, fc_name = os.path.split(out_fc)
        g_type = G_DICT.get(fs.geometryType, '').upper()
        arcpy.management.CreateFeatureclass(path, fc_name, g_type,
                                        spatial_reference=outSR)

        # add all fields
        for field in fs.fields:
            if field.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
                if '.' in field.name:
                    if 'shape.' not in field.name.lower():
                        field_name = field.name.split('.')[-1] #for weird SDE fields with periods
                    else:
                        field_name = '_'.join([f.title() for f in field.name.split('.')]) #keep geometry calcs if shapefile
                else:
                    field_name = field.name


                # need to filter even more as SDE sometimes yields weird field names...sigh
                restricted = ('fid', 'shape', 'objectid')
                if (not any(['shape_' in field.name.lower(),
                            'shape.' in field.name.lower(),
                            '(shape)' in field.name.lower()]) \
                            or isShp) and field.name.lower() not in restricted:
                    field_length = field.length if hasattr(field, 'length') else None
                    arcpy.management.AddField(out_fc, field_name, FTYPES[field.type],
                                                field_length=field_length,
                                                field_alias=field.alias)
        return out_fc

def add_domains_from_feature_set(out_fc, fs):

    # find workspace type and path
    ws, wsType = find_ws_type(out_fc)
    isShp = wsType == 'FileSystem'
    if not isShp:
            gdb_domains = arcpy.Describe(ws).domains
            dom_map = {}
            for field in fs.fields:
                if field.get(DOMAIN):
                    field_name = field.name.split('.')[-1]
                    dom_map[field_name] = field.domain[NAME]
                    if field.domain[NAME] not in gdb_domains:
                        if CODED_VALUES in field.domain:
                            dType = CODED
                        else:
                            dType = RANGE_UPPER
                        arcpy.management.CreateDomain(ws, field.domain[NAME],
                                                          field.domain[NAME],
                                                          FTYPES[field.type],
                                                          dType)

                        try:

                            if dType == CODED:
                                for cv in field.domain[CODED_VALUES]:
                                    arcpy.management.AddCodedValueToDomain(ws, field.domain[NAME], cv[CODE], cv[NAME])

                            elif dType == RANGE_UPPER:
                                _min, _max = field.domain[RANGE]
                                arcpy.management.SetValueForRangeDomain(ws, field.domain[NAME], _min, _max)

                        except Exception as e:
                            warnings.warn(e)

                        gdb_domains.append(field.domain[NAME])
                        print('Added domain "{}" to database: "{}"'.format(field.domain[NAME], ws))

            # add domains
            field_list = [f.name.split('.')[-1] for f in arcpy.ListFields(out_fc)]
            for fld, dom_name in six.iteritems(dom_map):
                if fld in field_list:
                    arcpy.management.AssignDomainToField(out_fc, fld, dom_name)
                    print('Assigned domain "{}" to field "{}"'.format(dom_name, fld))

def append_feature_set(out_fc, feature_set):
    """appends features from a feature set to existing feature class manually with an insert cursor

    """
    fc_fields = arcpy.ListFields(out_fc)
    cur_fields = [f.name for f in fc_fields if f.type not in ('OID', 'Geometry') and not f.name.lower().startswith('shape')]
    # insert cursor to write rows manually
    with arcpy.da.InsertCursor(out_fc, cur_fields + ['SHAPE@']) as irows:
        for i, row in enumerate(Cursor(feature_set, cur_fields + ['SHAPE@'])):
            if not i % 100:
                print(row)
            irows.insertRow(row)

def export_attachments(out_fc, layer):
    fc_ws, fc_ws_type = find_ws_type(out_fc)

##    if all([include_attachments, self.hasAttachments, fs.OIDFieldName, fc_ws_type != 'FileSystem']):
##
##        # get attachments (OID will start at 1)
##        att_folder = os.path.join(arcpy.env.scratchFolder, '{}_Attachments'.format(os.path.basename(out_fc)))
##        if not os.path.exists(att_folder):
##            os.makedirs(att_folder)
##
##        att_dict, att_ids = {}, []
##        for i,row in enumerate(fs):
##            att_id = 'P-{}'.format(i + 1)
##            print('\nattId: {}, oid: {}'.format(att_id, row.get(fs.OIDFieldName)))
##            att_ids.append(att_id)
##            att_dict[att_id] = []
##            for att in self.attachments(row.get(fs.OIDFieldName)):
##                print('\tatt: ', att)
##                out_att = att.download(att_folder, verbose=False)
##                att_dict[att_id].append(os.path.join(out_att))
##
##        # photo field (hopefully this is a unique field name...)
##        print('att_dict is: ', att_dict)
##
##        PHOTO_ID = 'PHOTO_ID_X_Y_Z__'
##        arcpy.management.AddField(out_fc, PHOTO_ID, 'TEXT', field_length=255)
##        with arcpy.da.UpdateCursor(out_fc, PHOTO_ID) as rows:
##            for i,row in enumerate(rows):
##                rows.updateRow((att_ids[i],))
##
##        # create temp table
##        arcpy.management.EnableAttachments(out_fc)
##        tmp_tab = r'in_memory\temp_photo_points'
##        arcpy.management.CreateTable(*os.path.split(tmp_tab))
##        arcpy.management.AddField(tmp_tab, PHOTO_ID, 'TEXT')
##        arcpy.management.AddField(tmp_tab, 'PATH', 'TEXT', field_length=255)
##        arcpy.management.AddField(tmp_tab, 'PHOTO_NAME', 'TEXT', field_length=255)
##
##        with arcpy.da.InsertCursor(tmp_tab, [PHOTO_ID, 'PATH', 'PHOTO_NAME']) as irows:
##            for k, att_list in six.iteritems(att_dict):
##                for v in att_list:
##                    irows.insertRow((k,) + os.path.split(v))
##
##         # add attachments
##        arcpy.management.AddAttachments(out_fc, PHOTO_ID, tmp_tab, PHOTO_ID,
##                                        'PHOTO_NAME', in_working_folder=att_folder)
##        arcpy.management.Delete(tmp_tab)
##        arcpy.management.DeleteField(out_fc, PHOTO_ID)
##        try:
##            shutil.rmtree(att_folder)
##        except:
##            pass
##
##        print('added attachments to: "{}"'.format(out_fc))