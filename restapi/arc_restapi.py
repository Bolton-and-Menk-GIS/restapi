# proprietary version (uses arcpy)
from __future__ import print_function
import urllib
import arcpy
import os
import time
import json
from rest_utils import *
arcpy.env.overwriteOutput = True
arcpy.env.addOutputsToMap = False

def exportFeatureSet(feature_set, out_fc, include_domains=False):
    """export FeatureSet (JSON result)  to shapefile or feature class

    Required:
        feature_set -- JSON response obtained from a query or FeatureSet() object
        out_fc -- output feature class or shapefile

    Optional:
        include_domains -- if True, will manually create the feature class and add domains to GDB
            if output is in a geodatabase.

    at minimum, feature set must contain these keys:
        [u'features', u'fields', u'spatialReference', u'geometryType']
    """
    # validate features input (should be list or dict, preferably list)
    feature_set = FeatureSet(feature_set)

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
        for i in xrange(1, len(split)):
            sub_dir = os.sep.join(split[:-i])
            desc = arcpy.Describe(sub_dir)
            if hasattr(desc, 'workspaceType'):
                return sub_dir, desc.workspaceType

    # find workspace type and path
    ws, wsType = find_ws_type(out_fc)
    isShp = wsType == 'FileSystem'

    # do proper export routine
    tmp = feature_set.dump(tmp_json_file(), indent=None)
    arcpy.conversion.JSONToFeatures(tmp, out_fc)
    try:
        os.remove(tmp)
    except:
        pass

    if not isShp and include_domains in (True, 1, TRUE):
        gdb_domains = arcpy.Describe(ws).domains
        dom_map = {}
        for field in feature_set.fields:
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
                    if dType == CODED:
                        for cv in field.domain[CODED_VALUES]:
                            arcpy.management.AddCodedValueToDomain(ws, field.domain[NAME], cv[CODE], cv[NAME])
                    elif dType == RANGE_UPPER:
                        _min, _max = field.domain[RANGE]
                        arcpy.management.SetValueForRangeDomain(ws, field.domain[NAME], _min, _max)

                    gdb_domains.append(field.domain[NAME])
                    print('Added domain "{}" to database: "{}"'.format(field.domain[NAME], ws))

        # add domains
        if not isShp and include_domains:
            field_list = [f.name.split('.')[-1] for f in arcpy.ListFields(out_fc)]
            for fld, dom_name in dom_map.iteritems():
                if fld in field_list:
                    arcpy.management.AssignDomainToField(out_fc, fld, dom_name)
                    print('Assigned domain "{}" to field "{}"'.format(dom_name, fld))

    print('Created: "{0}"'.format(out_fc))
    return out_fc

def exportFeaturesWithAttachments(out_ws, lyr_url, fields='*', where='1=1', token='', max_recs=None, get_all=False, out_gdb_name='', **kwargs):
    """exports a map service layer with attachments.  Output is a geodatabase.

    Required:
        out_ws -- output location to put new file gdb
        lyr_url -- url to map service layer

    Optional:
        fields -- list of fields or comma separated list of desired fields. Default is all fields ('*')
        where -- where clause for query
        token -- token to handle security, only required if service is secured
        max_recs -- maximum number of records to return. Ignored if get_all is set to True.
        get_all -- option to exceed transfer limit
        out_gdb_name -- optional output geodatabase name, can also reference an existing gdb within the "out_ws" folder
        **kwargs -- key word arguments to further filter query (i.e. geometry or outSR)
    """
    lyr = MapServiceLayer(lyr_url, token=token)

    if not out_gdb_name:
        out_gdb_name = arcpy.ValidateTableName(lyr.url.split('/')[-3], out_ws) + '.gdb'
    if not arcpy.Exists(os.path.join(out_ws, out_gdb_name)):
        gdb = arcpy.management.CreateFileGDB(out_ws, out_gdb_name, 'CURRENT').getOutput(0)
    else:
        gdb = os.path.join(out_ws, out_gdb_name)
    out_fc = os.path.join(gdb, arcpy.ValidateTableName(lyr.name, gdb))

    # make sure there is an OID field
    oid_name = lyr.OID.name
    if isinstance(fields, basestring):
        if fields != '*':
            if OID_TOKEN not in fields or oid_name not in fields:
                fields += ',{}'.format(oid_name)

    elif isinstance(fields, list):
        if OID_TOKEN not in fields or oid_name not in fields:
            fields.append(oid_name)

    # get feature set
    kwargs[RETURN_GEOMETRY] = TRUE
    cursor = lyr.cursor(fields, where, records=max_recs, add_params=kwargs, get_all=get_all)
    oid_index = [i for i,f in enumerate(cursor.field_objects) if f.type == OID][0]

    # form feature set and call export feature set
    fs = {FEATURES: cursor.features,
          FIELDS: lyr.response[FIELDS],
          SPATIAL_REFERENCE: lyr.response[EXTENT][SPATIAL_REFERENCE],
          GEOMETRY_TYPE: lyr.geometryType}

    exportFeatureSet(fs, out_fc)

    # get attachments (OID will start at 1)
    att_folder = os.path.join(out_ws, '{}_Attachments'.format(os.path.basename(out_fc)))
    if not os.path.exists(att_folder):
        os.makedirs(att_folder)

    att_dict, att_ids = {}, []
    for i,row in enumerate(cursor.get_rows()):
        att_id = 'P-{}'.format(i + 1)
        att_ids.append(att_id)
        att_dict[att_id] = []
        for att in lyr.attachments(row.oid):
            out_att = att.download(att_folder, verbose=False)
            att_dict[att_id].append(os.path.join(out_att))

    # photo field (hopefully this is a unique field name...)
    PHOTO_ID = 'PHOTO_ID_X_Y_Z__'
    arcpy.management.AddField(out_fc, PHOTO_ID, 'TEXT')
    with arcpy.da.UpdateCursor(out_fc, PHOTO_ID) as rows:
        for i,row in enumerate(rows):
            rows.updateRow((att_ids[i],))

    # create temp table
    arcpy.management.EnableAttachments(out_fc)
    tmp_tab = r'in_memory\temp_photo_points'
    arcpy.management.CreateTable('in_memory', 'temp_photo_points')
    arcpy.management.AddField(tmp_tab, PHOTO_ID, 'TEXT')
    arcpy.management.AddField(tmp_tab, 'PATH', 'TEXT', field_length=255)
    arcpy.management.AddField(tmp_tab, 'PHOTO_NAME', 'TEXT', field_length=255)

    with arcpy.da.InsertCursor(tmp_tab, [PHOTO_ID, 'PATH', 'PHOTO_NAME']) as irows:
        for k, att_list in att_dict.iteritems():
            for v in att_list:
                irows.insertRow((k,) + os.path.split(v))

     # add attachments
    arcpy.management.AddAttachments(out_fc, PHOTO_ID, tmp_tab, PHOTO_ID,
                                    'PHOTO_NAME', in_working_folder=att_folder)
    arcpy.management.Delete(tmp_tab)
    arcpy.management.DeleteField(out_fc, PHOTO_ID)

    print('Created: "{}"'.format(gdb))
    return gdb

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
        if isinstance(geometry, arcpy.mapping.Layer) and geometry.supports('DATASOURCE'):
            geometry = geometry.dataSource

        if isinstance(geometry, (arcpy.RecordSet, arcpy.FeatureSet)):
            geometry = geometry.JSON

        if isinstance(geometry, arcpy.Geometry):
            spatialReference = geometry.spatialReference.factoryCode
            self.geometryType = 'esriGeometry{}'.format(geometry.type.title())
            esri_json = json.loads(geometry.JSON)
            for k,v in sorted(esri_json.iteritems()):
                if k != SPATIAL_REFERENCE:
                    self.json[k] = v
            if SPATIAL_REFERENCE in esri_json:
                self.json[SPATIAL_REFERENCE] = esri_json[SPATIAL_REFERENCE]

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
        return d

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

    def __str__(self):
        """dumps JSON to string"""
        return self.dumps()

    def __repr__(self):
        """represntation"""
        return '<restapi.Geometry: {}>'.format(self.geometryType)

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
        if isinstance(geometries, self.__class__):
            geometries = geometries.json
        if spatialReference:
            if isinstance(spatialReference, int):
                sr_dict = {SPATIAL_REFERENCE: {WKID}}
            elif isinstance(spatialReference, dict):
                sr_dict = spatialReference
        else:
            sr_dict = None
        # if it is a dict, see if it is actually a feature set, then go through the rest of the filters
        if isinstance(geometries, dict):
            try:
                geometries = FeatureSet(geometries)
            except:
                pass

        # it is a layer or feature class/shapefile
        if isinstance(geometries, (arcpy.mapping.Layer, basestring)):
            if arcpy.Exists(geometries):
                with arcpy.da.SearchCursor(geometries, ['SHAPE@']) as rows:
                    self.geometries = [Geometry(r[0]) for r in rows]

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
            self.json[GEOMETRIES] = []
            for g in self.geometries:
                if not g.spatialReference:
                    g.spatialReference = spatialReference
                self.json[GEOMETRIES].append(g.envelopeAsJSON() if use_envelopes else g.json)

            self.json[GEOMETRY_TYPE] = self.geometries[0].geometryType if not use_envelopes else ESRI_ENVELOPE
            self.geometryType = self.geometries[0].geometryType
            if not spatialReference:
                self.spatialReference = self.geometries[0].spatialReference

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
        if isinstance(wkid, int):
            self.json[SPATIAL_REFERENCE] = {WKID: wkid}
        elif isinstance(wkid, dict):
            self.json[SPATIAL_REFERENCE] = wkid

class GeocodeHandler(object):
    """class to handle geocode results"""
    __slots__ = [SPATIAL_REFERENCE, 'results', FIELDS, 'formattedResults']

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
        for f, val in res_sample.attributes.iteritems():
            if isinstance(val, float):
                if val >= -3.4E38 and val <= 1.2E38:
                    __fields.append(FIELD_SCHEMA(name=f, type='FLOAT'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='DOUBLE'))
            elif isinstance(val, (int, long)):
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
