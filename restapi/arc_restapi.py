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

def exportFeatureSet(out_fc, feature_set):
    """export features (JSON result) to shapefile or feature class

    Required:
        out_fc -- output feature class or shapefile
        feature_set -- JSON response (feature set) obtained from a query

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
        SPLIT = filter(None, path.split(os.sep))
        if path.startswith('\\\\'):
            SPLIT[0] = r'\\{0}'.format(SPLIT[0])

        # find valid workspace
        for i in xrange(1, len(SPLIT)):
            sub_dir = os.sep.join(SPLIT[:-i])
            desc = arcpy.Describe(sub_dir)
            if hasattr(desc, 'workspaceType'):
                return sub_dir, desc.workspaceType

    # find workspace type and path
    ws, wsType = find_ws_type(out_fc)
    if wsType == 'FileSystem':
        isShp = True
        shp_name = out_fc
        out_fc = r'in_memory\temp_xxx'
    else:
        isShp = False

    # make new feature class
    fields = [Field(f) for f in feature_set.fields]

    sr_dict = feature_set.spatialReference
    outSR = feature_set.getWKID()

    g_type = G_DICT[feature_set.geometryType]
    path, fc_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(path, fc_name, g_type,
                                        spatial_reference=outSR)

    # add all fields
    cur_fields, fMap = [], []
    if not isShp:
        gdb_domains = arcpy.Describe(ws).domains
    for field in fields:
        if field.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
            field_name = field.name.split('.')[-1]
            if field.domain and not isShp:
                if field.domain['name'] not in gdb_domains:
                    if 'codedValues' in field.domain:
                        dType = 'CODED'
                    else:
                        dType = 'RANGE'

                    arcpy.management.CreateDomain(ws, field.domain['name'],
                                                  field.domain['name'],
                                                  FTYPES[field.type],
                                                  dType)
                    if dType == 'CODED':
                        for cv in field.domain['codedValues']:
                            arcpy.management.AddCodedValueToDomain(ws, field.domain['name'], cv['code'], cv['name'])
                    else:
                        _min, _max = field.domain['range']
                        arcpy.management.SetValueForRangeDomain(ws, field.domain['name'], _min, _max)

                    gdb_domains.append(field.domain['name'])
                    print('added domain "{}" to geodatabase: "{}"'.format(field.domain['name'], ws))

                field_domain = field.domain['name']
            else:
                field_domain = ''

            # need to filter even more as SDE sometimes yields weird field names...sigh
            if not any(['shape_' in field.name.lower(),
                        'shape.' in field.name.lower(),
                        '(shape)' in field.name.lower(),
                        'objectid' in field.name.lower(),
                        field.name.lower() == 'fid']):

                arcpy.management.AddField(out_fc, field_name, FTYPES[field.type],
                                            field_length=field.length,
                                            field_alias=field.alias,
                                            field_domain=field_domain)
                cur_fields.append(field_name)
                fMap.append(field.name)

    # insert cursor to write rows (using arcpy.FeatureSet() is too buggy)
    with arcpy.da.InsertCursor(out_fc, cur_fields + [SHAPE_TOKEN]) as irows:
        for feat in feature_set:
            irows.insertRow([feat.attributes.get(f) for f in fMap] + [arcpy.AsShape(feat.geometry, True)])

    # if output is a shapefile
    if isShp:
        out_fc = arcpy.management.CopyFeatures(out_fc, shp_name)

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
    kwargs['returnGeometry'] = 'true'
    cursor = lyr.cursor(fields, where, records=max_recs, add_params=kwargs, get_all=get_all)
    oid_index = [i for i,f in enumerate(cursor.field_objects) if f.type == OID][0]

    # form feature set and call export feature set
    fs = {'features': cursor.features,
          'fields': lyr.response['fields'],
          SPATIAL_REFERENCE: lyr.response['extent'][SPATIAL_REFERENCE],
          GEOMETRY_TYPE: lyr.geometryType}

    exportFeatureSet(out_fc, fs)

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

def exportReplica(replica, out_folder):
    """converts a restapi.Replica() to a File Geodatabase

    replica -- input restapi.Replica() object, must be generated from restapi.FeatureService.createReplica()
    out_folder -- full path to folder location where new geodatabase will be stored.
                The geodatabase will be named the same as the replica
    """
    if not hasattr(replica, 'replicaName'):
        print('Not a valid input!  Must be generated from restapi.FeatureService.createReplica() method!')
        return

    # attachment directory and gdb set up
    att_loc = os.path.join(out_folder, 'Attachments')
    if not os.path.exists(att_loc):
        os.makedirs(att_loc)

    out_gdb_name = arcpy.ValidateTableName(replica.replicaName, out_folder).split('.')[0] + '.gdb'
    gdb = arcpy.management.CreateFileGDB(out_folder, out_gdb_name, 'CURRENT').getOutput(0)

    # set schema and create feature classes
    for layer in replica.layers:

        # download attachments
        att_dict = {}
        for attInfo in layer.attachments:
            out_file = assignUniqueName(os.path.join(att_loc, attInfo['name']))
            with open(out_file, 'wb') as f:
                f.write(urllib.urlopen(attInfo['url']).read())
            att_dict[attInfo['parentGlobalId']] = out_file

        fc = os.path.join(gdb, arcpy.ValidateTableName(layer.name, gdb))

        if layer.features:
            arcpy.management.CreateFeatureclass(gdb, os.path.basename(fc), G_DICT[layer.geometryType],
                                                spatial_reference=layer.spatialReference)

            # set up schema
            guid, guidFieldName = None, None
            layer_fields = [f for f in layer.fields if f.type not in (SHAPE, OID)]
            for i, field in enumerate(layer_fields):

                if field.type == GLOBALID:
                    field_name = 'ORIG_GlobalID'
                    guid = i
                    guidFieldName = field.name
                else:
                    field_name = field.name

                # set up domain if necessary
                gdb_domains = []
                if field.domain:
                    if field.domain['name'] not in gdb_domains:
                        if 'codedValues' in field.domain:
                            dType = 'CODED'
                        else:
                            dType = 'RANGE'

                        arcpy.management.CreateDomain(gdb, field.domain['name'],
                                                      field.domain['name'],
                                                      FTYPES[field.type],
                                                      dType)
                        if dType == 'CODED':
                            for cv in field.domain['codedValues']:
                                arcpy.management.AddCodedValueToDomain(gdb, field.domain['name'], cv['code'], cv['name'])
                        else:
                            _min, _max = field.domain['range']
                            arcpy.management.SetValueForRangeDomain(gdb, field.domain['name'], _min, _max)

                        gdb_domains.append(field.domain['name'])

                    field_domain = field.domain['name']
                else:
                    field_domain = ''

                arcpy.management.AddField(fc, field_name, FTYPES[field.type],
                                            field_length=field.length,
                                            field_alias=field.alias,
                                            field_domain=field_domain)

            # set up field values
            fld_names = [SHAPE_TOKEN] + [f.name for f in layer_fields]
            if guid != None:
                fld_names[guid + 1] = 'ORIG_GlobalID'
            date_indices = [i for i,f in enumerate(layer_fields) if f.type == DATE_FIELD]

            with arcpy.da.InsertCursor(fc, fld_names) as irows:
                for rowD in layer.features:
                    row = [rowD['attributes'][f] if f in rowD['attributes']
                           else rowD['attributes'][guidFieldName]
                           for f in fld_names[1:]]

                    for i in date_indices:
                        row[i] = mil_to_date(row[i])

                    shape = arcpy.AsShape(rowD['geometry'], True)
                    irows.insertRow([shape] + row)

        # Enable Attachments
        if layer.attachments and layer.features:
            arcpy.management.AddGlobalIDs(fc)
            arcpy.management.EnableAttachments(fc)

            # create temp table
            tmp_tab = r'in_memory\temp_photo_points'
            arcpy.management.CreateTable('in_memory', 'temp_photo_points')
            arcpy.management.AddField(tmp_tab, 'ORIG_GlobalID', 'TEXT')
            arcpy.management.AddField(tmp_tab, 'PATH', 'TEXT', field_length=255)
            arcpy.management.AddField(tmp_tab, 'PHOTO_NAME', 'TEXT', field_length=254)
            with arcpy.da.InsertCursor(tmp_tab, ['ORIG_GlobalID', 'PATH', 'PHOTO_NAME']) as irows:
                for k,v in att_dict.iteritems():
                    irows.insertRow((k,) + os.path.split(v))

            # add attachments
            arcpy.management.AddAttachments(fc, 'ORIG_GlobalID', tmp_tab, 'ORIG_GlobalID', 'PHOTO_NAME', in_working_folder=att_loc)
            arcpy.management.Delete(tmp_tab)

            print('Created: "{}"'.format(gdb))
            return gdb

    return out_folder

class Geometry(object):
    """class to handle restapi.Geometry"""
    def __init__(self, geometry, **kwargs):
        """converts geometry input to restapi.Geometry object

        Required:
            geometry -- input geometry.  Can be arcpy.Geometry(), shapefile/feature
                class, or JSON
        """
        self._inputGeometry = geometry
        self.spatialReference = None
        self.geometryType = None
        for k, v in kwargs.iteritems():
            if k == SPATIAL_REFERENCE:
                if isinstance(v, int):
                    self.spatialReference = v
                elif isinstance(v, basestring):
                    try:
                        # it's a json string?
                        v = json.loads(v)
                    except:
                        try:
                            v = int(v)
                            self.spatialReference = v
                        except:
                            pass

                if isinstance(v, dict):
                    self.spatialReference = v.get(LATEST_WKID) if v.get(LATEST_WKID) else v.get(WKID)

            elif k == GEOMETRY_TYPE and v.startswith('esri'):
                self.geometryType = v

        self.JSON = OrderedDict2()
        if isinstance(geometry, arcpy.mapping.Layer) and geometry.supports('DATASOURCE'):
            geometry = geometry.dataSource

        if isinstance(geometry, (arcpy.RecordSet, arcpy.FeatureSet)):
            geometry = geometry.JSON

        if isinstance(geometry, arcpy.Geometry):
            self.spatialReference = geometry.spatialReference.factoryCode
            self.geometryType = 'esriGeometry{}'.format(geometry.type.title())
            esri_json = json.loads(geometry.JSON)
            for k,v in sorted(esri_json.iteritems()):
                if k != SPATIAL_REFERENCE:
                    self.JSON[k] = v
            if SPATIAL_REFERENCE in esri_json:
                self.JSON[SPATIAL_REFERENCE] = esri_json[SPATIAL_REFERENCE]

        elif isinstance(geometry, basestring):
            try:
                geometry = OrderedDict2(**json.loads(geometry))
            except:
                # maybe it's a shapefile/feature class?
                if arcpy.Exists(geometry):
                    desc = arcpy.Describe(geometry)
                    self.spatialReference = desc.spatialReference.factoryCode
                    self.geometryType = 'esriGeometry{}'.format(desc.shapeType.title())
                    with arcpy.da.SearchCursor(geometry, ['SHAPE@JSON']) as rows:
                        for row in rows:
                            esri_json = json.loads(row[0])
                            break

                    for k,v in sorted(esri_json.iteritems()):
                        if k != SPATIAL_REFERENCE:
                            self.JSON[k] = v
                    if SPATIAL_REFERENCE in esri_json:
                        self.JSON[SPATIAL_REFERENCE] = esri_json[SPATIAL_REFERENCE]
                else:
                    raise IOError('Not a valid geometry input!')

        if isinstance(geometry, dict):
            if SPATIAL_REFERENCE in geometry:
                sr_json = geometry[SPATIAL_REFERENCE]
                if LATEST_WKID in sr_json:
                    self.spatialReference = sr_json[LATEST_WKID]
                else:
                    try:
                        self.spatialReference = sr_json[WKID]
                    except:
                        raise IOError('No spatial reference found in JSON object!')
            if 'features' in geometry:
                d = geometry['features'][0]
                if 'geometry' in d:
                    d = geometry['features'][0]['geometry']
                for k,v in d.iteritems():
                    self.JSON[k] = v
            elif 'geometry' in geometry:
                for k,v in geometry['geometry']:
                    self.JSON[k] = v
            if not self.JSON:
                if 'rings' in geometry:
                    self.JSON['rings'] = geometry['rings']
                    self.geometryType = GEOM_DICT['rings']
                elif 'paths' in geometry:
                    self.JSON['paths'] = geometry['paths']
                    self.geometryType = GEOM_DICT['paths']
                elif 'points' in geometry:
                    self.JSON['points'] = geometry['points']
                    self.geometryType = GEOM_DICT['points']
                elif 'x' in geometry and 'y' in geometry:
                    self.JSON['x'] = geometry['x']
                    self.JSON['y'] = geometry['y']
                    self.geometryType = ESRI_POINT
                else:
                    raise IOError('Not a valid JSON object!')
            if not self.geometryType and GEOMETRY_TYPE in geometry:
                self.geometryType = geometry[GEOMETRY_TYPE]
        if not SPATIAL_REFERENCE in self.JSON and self.spatialReference:
            self.JSON[SPATIAL_REFERENCE] = {WKID: self.spatialReference}

    def envelope(self):
        """return an envelope from shape"""
        e = arcpy.AsShape(self.JSON, True).extent
        return ','.join(map(str, [e.XMin, e.YMin, e.XMax, e.YMax]))

    def envelopeAsJSON(self, roundCoordinates=False):
        """returns an envelope geometry object as JSON"""
        flds = ['xmin','ymin','xmax','ymax']
        if roundCoordinates:
            coords = map(int, [float(i) for i in self.envelope().split(',')])
        else:
            coords = self.envelope().split(',')
        d = dict(zip(flds, coords))
        d[SPATIAL_REFERENCE] = self.JSON[SPATIAL_REFERENCE]
        return d

    def dumps(self):
        """retuns JSON as a string"""
        return json.dumps(self.JSON)

    def asShape(self):
        """returns JSON as arcpy.Geometry() object"""
        return arcpy.AsShape(self.JSON, True)

    def buffer(self, distance, units='', geometry_service='', **kwargs):
        """buffer the geometry, returns a new Geometry object.

        Required:
            distance -- distance integer or float
            units -- name or WKID of units (will pass through GeometryService.getLinearUnitWKID() )

        Optional:
            geometry_service -- url or GeometryService object to use.  If none specified, will default
                to esri sample server geometry service

            **kwargs -- optional keyword arguments for buffer routine
        """
        default_url = 'http://sampleserver6.arcgisonline.com/arcgis/rest/services/Utilities/Geometry/GeometryServer'
        if not geometry_service:
            geometry_service = default_url

        if isinstance(geometry_service, basestring):
            geometry_service = GeometryService(geometry_service)

        if isinstance(geometry_service, GeometryService):
            return geometry_service.buffer(self.JSON, self.spatialReference, distance, units, **kwargs)

    def __str__(self):
        """dumps JSON to string"""
        return self.dumps()

    def __repr__(self):
        """represntation"""
        return '<restapi.Geometry: {}>'.format(self.geometryType)

class GeometryCollection(object):
    """represents an array of restapi.Geometry objects"""
    geometries = []
    JSON = {'geometries': []}
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
        elif isinstance(geometries, dict) and 'geometries' in geometries:

            # it is already a GeometryCollection in ESRI JSON format?
            self.geometries = [Geometry(g) for g in geometries['geometries']]

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
            self.JSON['geometries'] = [g.envelopeAsJSON() if use_envelopes else g.JSON for g in self.geometries]
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
    __slots__ = [SPATIAL_REFERENCE, 'results', 'fields', 'formattedResults']

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
            pt = arcpy.PointGeometry(arcpy.Point(res.location['x'],
                                                 res.location['y']),
                                                 self.spatialReference)

            yield (pt,) + tuple(res.attributes[f.name] for f in self.fields)

class ImageService(BaseImageService):

    def pointIdentify(self, **kwargs):
        """method to get pixel value from x,y coordinates or JSON point object

        Recognized key word arguments:
            geometry -- JSON point object as dictionary
            x -- x coordinate
            y -- y coordinate
            inSR -- input spatial reference.  Should be supplied if spatial
                reference is different from the Image Service's projection

        geometry example:
            geometry = {"x":3.0,"y":5.0,"spatialReference":{"wkid":102100}}
        """
        IDurl = self.url + '/identify'

        geometry = {}
        if 'inSR' in kwargs:
            inSR = kwargs['inSR']
        else:
            inSR = self.spatialReference

        if 'geometry' in kwargs:
            g = Geometry(kwargs['geometry'], spatialReference=inSR)
            inSR = g.spatialReference

        elif 'x' in kwargs and 'y' in kwargs:
            g = {'x': kwargs['x'], 'y': kwargs['y']}
            if 'sr' in kwargs:
                g["spatialReference"] = {"wkid": kwargs['sr']}
            else:
                g["spatialReference"] = {"wkid": self.spatialReference}
            geometry = json.dumps(g)

        params = {'geometry': geometry,
                  'inSR': inSR,
                  GEOMETRY_TYPE:ESRI_POINT,
                  'f':'json',
                  'returnGeometry': 'false',
                  'returnCatalogItems': 'false'
                  }

        for k,v in kwargs.iteritems():
            if k not in params:
                params[k] = v

        j = POST(IDurl, params, cookies=self._cookie)
        if 'value' in j:
            return j['value']

    def exportImage(self, poly, out_raster, envelope=False, rendering_rule={}, interp='RSP_BilinearInterpolation', nodata=None, **kwargs):
        """method to export an AOI from an Image Service

        Required:
            poly -- polygon features
            out_raster -- output raster image

        Optional:
            envelope -- option to use envelope of polygon
            rendering_rule -- rendering rule to perform raster functions
            kwargs -- optional key word arguments for other parameters
        """
        if not out_raster.endswith('.tif'):
            out_raster = os.path.splitext(out_raster)[0] + '.tif'
        query_url = '/'.join([self.url, 'exportImage'])

        if isinstance(poly, Geometry):
            in_geom = poly
        else:
            in_geom = Geometry(poly)
        sr = in_geom.spatialReference
        if envelope:
            geojson = in_geom.envelope()
            geometryType = ESRI_ENVELOPE
        else:
            geojson = in_geom.dumps()
            geometryType = in_geom.geometryType

        if sr != self.spatialReference:
            polyG = in_geom.asShape()
            polygon = polyG.projectAs(arcpy.SpatialReference(self.spatialReference))
        e = in_geom.asShape().extent
        bbox = self.adjustbbox([e.XMin, e.YMin, e.XMax, e.YMax])

        # imageSR
        if 'imageSR' not in kwargs:
            imageSR = sr
        else:
            imageSR = kwargs['imageSR']

        if 'noData' in kwargs:
            nodata = kwargs['noData']

        # check for raster function availability
        if not self.allowRasterFunction:
            rendering_rule = ''

        # find width and height for image size (round to whole number)
        bbox_int = map(int, bbox.split(','))
        width = abs(bbox_int[0] - bbox_int[2])
        height = abs(bbox_int[1] - bbox_int[3])

        matchType = 'esriNoDataMatchAny'
        if ',' in str(nodata):
            matchType = 'esriNoDataMatchAll'


        # set params
        p = {'f':'pjson',
             'renderingRule': rendering_rule,
             'adjustAspectRatio': 'true',
             'bbox': bbox,
             'format': 'tiff',
             'imageSR': imageSR,
             'bboxSR': self.spatialReference,
             'size': '{0},{1}'.format(width,height),
             'pixelType': self.pixelType,
             'noDataInterpretation': '&'.join(map(str, filter(lambda x: x not in (None, ''),
                ['noData=' + str(nodata) if nodata != None else '', 'noDataInterpretation=%s' %matchType if nodata != None else matchType]))),
             'interpolation': interp
            }

        # overwrite with kwargs
        for k,v in kwargs.iteritems():
            if k not in ['size', 'bboxSR']:
                p[k] = v

        # post request
        r = POST(query_url, p, cookies=self._cookie)

        # check for errors
        if 'error' in r:
            if 'details' in r['error']:
                raise RuntimeError('\n'.join(r['error']['details']))

        elif 'href' in r:
            tiff = urllib.urlopen(r['href'].strip()).read()
            with open(out_raster, 'wb') as f:
                f.write(tiff)
            try:
                arcpy.management.CalculateStatistics(out_raster)
            except:
                pass
            print('Created: "{0}"'.format(out_raster))

    def clip(self, poly, out_raster, envelope=True, imageSR='', noData=None):
        """method to clip a raster"""
        if envelope:
            if not isinstance(poly, Geometry):
                poly = Geometry(poly)
            e = poly.asShape().extent
            bbox = map(int, [float(i) for i in self.adjustbbox([e.XMin, e.YMin, e.XMax, e.YMax]).split(',')])
            flds = ['xmin','ymin','xmax','ymax']
            geojson = dict(zip(flds, bbox))
            geojson[SPATIAL_REFERENCE] = poly.JSON[SPATIAL_REFERENCE]
        else:
            geojson = Geometry(poly).dumps() if not isinstance(poly, Geometry) else poly.dumps()
        ren = {
          "rasterFunction" : "Clip",
          "rasterFunctionArguments" : {
            "ClippingGeometry" : geojson,
            "ClippingType": 1
            },
          "variableName" : "Raster"
        }
        self.exportImage(poly, out_raster, rendering_rule=ren, noData=noData, imageSR=imageSR)



    def arithmetic(self, poly, out_raster, raster_or_constant, operation=3, envelope=False, imageSR='', **kwargs):
        """perform arithmetic operations against a raster

        Required:
            poly -- input polygon or JSON polygon object
            out_raster -- full path to output raster
            raster_or_constant -- raster to perform opertion against or constant value

        Optional:
            operation -- arithmetic operation to use (1|2|3)
            envelope -- if true, will use bounding box of input features
            imageSR -- output image spatial reference

        Operations:
            1 -- esriRasterPlus
            2 -- esriRasterMinus
            3 -- esriRasterMultiply
        """
        ren = {
                  "rasterFunction" : "Arithmetic",
                  "rasterFunctionArguments" : {
                       "Raster" : "$$",
                       "Raster2": raster_or_constant,
                       "Operation" : operation
                     }
                  }
        self.exportImage(poly, out_raster, rendering_rule=json.dumps(ren), imageSR=imageSR, **kwargs)

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
