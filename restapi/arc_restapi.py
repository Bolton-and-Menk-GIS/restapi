# proprietary version (uses arcpy)
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
    if isinstance(feature_set, basestring):
        try:
            feature_set = json.loads(feature_set)
        except:
            raise IOError('Not a valid input for "features" parameter!')

    if not isinstance(feature_set, dict) or not 'features' in feature_set:
        raise IOError('Not a valid input for "features" parameter!')

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
    fields = [Field(f) for f in feature_set['fields']]

    sr_dict = feature_set['spatialReference']
    if 'latestWkid' in sr_dict:
        outSR = int(sr_dict['latestWkid'])
    else:
        outSR = int(sr_dict['wkid'])

    g_type = G_DICT[feature_set['geometryType']]
    path, fc_name = os.path.split(out_fc)
    arcpy.CreateFeatureclass_management(path, fc_name, g_type,
                                        spatial_reference=outSR)

    # add all fields
    cur_fields = []
    fMap = []
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
                    print 'added domain "{}" to geodatabase: "{}"'.format(field.domain['name'], ws)

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
                fMap.append(field)

    # insert cursor to write rows (using arcpy.FeatureSet() is too buggy)
    cur_fields.append('SHAPE@')
    fMap += [f for f in fields if f.type == SHAPE]
    with arcpy.da.InsertCursor(out_fc, cur_fields) as irows:
        for feat in feature_set['features']:
            irows.insertRow(Row(feat, fMap, outSR).values)

    # if output is a shapefile
    if isShp:
        out_fc = arcpy.management.CopyFeatures(out_fc, shp_name)

    print 'Created: "{0}"'.format(out_fc)
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
            if 'OID@' not in fields or oid_name not in fields:
                fields += ',{}'.format(oid_name)

    elif isinstance(fields, list):
        if 'OID@' not in fields or oid_name not in fields:
            fields.append(oid_name)

    # get feature set
    kwargs['returnGeometry'] = 'true'
    cursor = lyr.cursor(fields, where, records=max_recs, add_params=kwargs, get_all=get_all)
    oid_index = [i for i,f in enumerate(cursor.field_objects) if f.type == OID][0]

    # form feature set and call export feature set
    fs = {'features': cursor.features,
          'fields': lyr.response['fields'],
          'spatialReference': lyr.response['extent']['spatialReference'],
          'geometryType': lyr.geometryType}

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

    print 'Created: "{}"'.format(gdb)
    return gdb

def exportReplica(replica, out_folder):
    """converts a restapi.Replica() to a File Geodatabase

    replica -- input restapi.Replica() object, must be generated from restapi.FeatureService.createReplica()
    out_folder -- full path to folder location where new geodatabase will be stored.
                The geodatabase will be named the same as the replica
    """
    if not hasattr(replica, 'replicaName'):
        print 'Not a valid input!  Must be generated from restapi.FeatureService.createReplica() method!'
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

                if field.type == 'esriFieldTypeGlobalID':
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
            fld_names = ['SHAPE@'] + [f.name for f in layer_fields]
            if guid != None:
                fld_names[guid + 1] = 'ORIG_GlobalID'
            date_indices = [i for i,f in enumerate(layer_fields) if f.type == 'esriFieldTypeDate']

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

            print 'Created: "{}"'.format(gdb)
            return gdb

    return out_folder

class Geometry(object):
    """class to handle restapi.Geometry"""
    def __init__(self, geometry, *args):
        """converts geometry input to restapi.Geometry object

        Required:
            geometry -- input geometry.  Can be arcpy.Geometry(), shapefile/feature
                class, or JSON
        """
        self._inputGeometry = geometry
        self.spatialReference = None
        self.geometryType = None
        self.JSON = OrderedDict2()
        if isinstance(geometry, arcpy.mapping.Layer) and geometry.supports('DATASOURCE'):
            geometry = geometry.dataSource
        if isinstance(geometry, arcpy.Geometry):
            self.spatialReference = geometry.spatialReference.factoryCode
            self.geometryType = 'esriGeometry{}'.format(geometry.type.title())
            esri_json = json.loads(geometry.JSON)
            for k,v in sorted(esri_json.iteritems()):
                if k != 'spatialReference':
                    self.JSON[k] = v
            if 'spatialReference' in esri_json:
                self.JSON['spatialReference'] = esri_json['spatialReference']

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
                        if k != 'spatialReference':
                            self.JSON[k] = v
                    if 'spatialReference' in esri_json:
                        self.JSON['spatialReference'] = esri_json['spatialReference']
                else:
                    raise IOError('Not a valid geometry input!')

        if isinstance(geometry, dict):
            if 'spatialReference' in geometry:
                sr_json = geometry['spatialReference']
                if 'latestWkid' in sr_json:
                    self.spatialReference = sr_json['latestWkid']
                else:
                    try:
                        self.spatialReference = sr_json['wkid']
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
                        self.geometryType = JSON_DICT['rings']
                    elif 'paths' in geometry:
                        self.JSON['paths'] = geometry['paths']
                        self.geometryType = JSON_DICT['paths']
                    elif 'points' in geometry:
                        self.JSON['points'] = geometry['points']
                        self.geometryType = JSON_DICT['points']
                    elif 'x' in geometry and 'y' in geometry:
                        self.JSON['x'] = geometry['x']
                        self.JSON['y'] = geometry['y']
                        self.geometryType = 'esriGeometryPoint'
                    else:
                        raise IOError('Not a valid JSON object!')
                if not self.geometryType and 'geometryType' in geometry:
                    self.geometryType = geometry['geometryType']
        if not 'spatialReference' in self.JSON and self.spatialReference:
            self.JSON['spatialReference'] = {'wkid': self.spatialReference}

    def envelope(self):
        """return an envelope from shape"""
        e = arcpy.AsShape(self.JSON, True).extent
        return ','.join(map(str, [e.XMin, e.YMin, e.XMax, e.YMax]))

    def dumps(self):
        """retuns JSON as a string"""
        return json.dumps(self.JSON)

    def asShape(self):
        """returns JSON as arcpy.Geometry() object"""
        return arcpy.AsShape(self.JSON, True)

    def __str__(self):
        """dumps JSON to string"""
        return self.dumps()

class Cursor(BaseCursor):
    """Class to handle Cursor object"""
    def __init__(self, url, fields='*', where='1=1', records=None, token='', add_params={}, get_all=False):
        """Cusor object to handle queries to rest endpoints

        Required:
            url -- url to layer's rest endpoint

        Optional:
            fields -- option to limit fields returned.  All are returned by default
            where -- where clause for cursor
            records -- number of records to return.  Default is None to return all
                records within bounds of max record count unless get_all is True
            token -- token to handle security (only required if security is enabled)
            add_params -- option to add additional search parameters
            get_all -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records.
        """
        super(Cursor, self).__init__(url, fields, where, records, token, add_params, get_all)

    def get_rows(self):
        """returns row objects"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects, self.spatialReference)

    def rows(self):
        """returns Cursor.rows() as generator"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects, self.spatialReference).values

    def __iter__(self):
        """returns Cursor.rows()"""
        return self.rows()

class Row(BaseRow):
    """Class to handle Row object"""
    def __init__(self, features, fields, spatialReference):
        """Row object for Cursor

        Required:
            features -- features JSON object
            fields -- fields participating in cursor
            spatialReference -- spatial reference WKID for geometry
        """
        super(Row, self).__init__(features, fields, spatialReference)

    @property
    def geometry(self):
        """returns arcpy geometry object
        Warning: output is unprojected
            use the projectAs(wkid, {transformation_name})
            method to project geometry
        """
        if self.esri_json:
            return arcpy.AsShape(self.esri_json, True)
        return None

    @property
    def oid(self):
        """returns the OID for row"""
        if self.oid_field_ob:
            return self.atts[self.oid_field_ob.name]
        return None

    @property
    def values(self):
        """returns values as tuple"""
        _values = [self.atts[f.name] for f in self.fields
                   if f.type != SHAPE]

        if self.geometry and self.shape_field_ob:
            _values.insert(self.fields.index(self.shape_field_ob), self.geometry)

        elif self.geometry:
            _values.append(self.geometry)

        return tuple(_values)

class GeocodeHandler(object):
    """class to handle geocode results"""
    __slots__ = ['spatialReference', 'results', 'fields', 'formattedResults']

    def __init__(self, geocodeResult):
        """geocode response object handler

        Required:
            geocodeResult -- GeocodeResult object
        """
        self.results = geocodeResult.results
        self.spatialReference = geocodeResult.spatialReference['wkid']

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

class ArcServer(BaseArcServer):
    """class to handle ArcServer connection"""
    def __init__(self, url, usr='', pw='', token=''):
        """Base REST Endpoint Object to handle credentials and get JSON response

        Required:
            url -- ArcGIS services directory

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(ArcServer, self).__init__(url, usr, pw, token)


    def get_MapService(self, name_or_wildcard):
        """method to return MapService Object, supports wildcards

        Required:
            name_or_wildcard -- service name or wildcard used to grab service name
                (ex: "moun_webmap_rest/mapserver" or "*moun*mapserver")
        """
        full_path = self.get_service_url(name_or_wildcard)
        if full_path:
            return MapService(full_path, token=self.token)

class MapService(BaseMapService):
    def __init__(self, url, usr='', pw='', token=''):
        """MapService object

    Required:
        url -- MapService url

    Optional (below params only required if security is enabled):
        usr -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
        token -- token to handle security (alternative to usr and pw)
    """
        super(MapService, self).__init__(url, usr, pw, token)

    def layer(self, name):
        """Method to return a layer object with advanced properties by name

        Required:
            name -- layer name (supports wildcard syntax*)
        """
        layer_path = get_layer_url(self.url, name, self.token)
        if layer_path:
            return MapServiceLayer(layer_path, token=self.token)
        else:
            print 'Layer "{0}" not found!'.format(name)

    def cursor(self, layer_name, fields='*', where='1=1', records=None, add_params={}, get_all=False):
        """Cusor object to handle queries to rest endpoints

        Required:
           layer_name -- name of layer in map service

        Optional:
            fields -- option to limit fields returned.  All are returned by default
            where -- where clause for cursor
            records -- number of records to return (within bounds of max record count)
            token --
            add_params -- option to add additional search parameters
            get_all -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records
        """
        lyr = get_layer_url(self.url, layer_name, self.token)
        return Cursor(lyr, fields, where, records, self.token, add_params, get_all)

    def layer_to_fc(self, layer_name,  out_fc, fields='*', where='1=1',
                    records=None, params={}, get_all=False, sr=None):
        """Method to export a feature class from a service layer

        Required:
            layer_name -- name of map service layer to export to fc
            out_fc -- full path to output feature class

        Optional:
            where -- optional where clause
            params -- dictionary of parameters for query
            fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            records -- number of records to return. Default is none, will return maxRecordCount
            get_all -- option to get all records.  If true, will recursively query REST endpoint
                until all records have been gathered. Default is False.
            sr -- output spatial refrence (WKID)
        """
        lyr = self.layer(layer_name)
        lyr.layer_to_fc(out_fc, fields, where,records, params, get_all, sr)

    def layer_to_kmz(self, layer_name, out_kmz='', flds='*', where='1=1', params={}):
        """Method to create kmz from query

        Required:
            layer_name -- name of map service layer to export to fc

        Optional:
            out_kmz -- output kmz file path, if none specified will be saved on Desktop
            flds -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            where -- optional where clause
            params -- dictionary of parameters for query
        """
        lyr = self.layer(layer_name)
        lyr.layer_to_kmz(flds, where, params, kmz=out_kmz)

    def clip(self, layer_name, poly, output, fields='*', out_sr='', where='', envelope=False):
        """Method for spatial Query, exports geometry that intersect polygon or
        envelope features.

        Required:
            layer_name -- name of map service layer to export to fc
            poly -- polygon (or other) features used for spatial query
            output -- full path to output feature class

        Optional:
             fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            sr -- output spatial refrence (WKID)
            where -- optional where clause
            envelope -- if true, the polygon features bounding box will be used.  This option
                can be used if the feature has many vertices or to check against the full extent
                of the feature class
        """
        lyr = self.layer(layer_name)
        return lyr.clip(poly, output, fields, out_sr, where, envelope)

class MapServiceLayer(BaseMapServiceLayer):
    """Class to handle advanced layer properties"""
    def __init__(self, url='', usr='', pw='', token=''):
        """MapService Layer object

        Required:
            url -- MapService layer url (should include index to layer)

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(MapServiceLayer, self).__init__(url, usr, pw, token)

    def cursor(self, fields='*', where='1=1', records=None, add_params={}, get_all=False):
        """Run Cursor on layer, helper method that calls Cursor Object"""
        return Cursor(self.url, fields, where, records, self.token, add_params, get_all)

    def layer_to_fc(self, out_fc, fields='*', where='1=1', records=None, params={}, get_all=False, sr=None):
        """Method to export a feature class from a service layer

        Required:
            out_fc -- full path to output feature class

        Optional:
            where -- optional where clause
            params -- dictionary of parameters for query
            fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            records -- number of records to return. Default is none, will return maxRecordCount
            get_all -- option to get all records.  If true, will recursively query REST endpoint
                until all records have been gathered. Default is False.
            sr -- output spatial refrence (WKID)
        """
        if self.type == 'Feature Layer':
            if not fields:
                fields = '*'
            if fields == '*':
                _fields = self.fields
            else:
                if isinstance(fields, basestring):
                    fields = fields.split(',')
                _fields = [f for f in self.fields if f.name in fields]

            # filter fields for cusor object
            cur_fields = []
            for fld in _fields:
                if fld.type not in [OID] + SKIP_FIELDS.keys():
                    if not any(['shape_' in fld.name.lower(),
                                'shape.' in fld.name.lower(),
                                '(shape)' in fld.name.lower(),
                                'objectid' in fld.name.lower(),
                                fld.name.lower() == 'fid']):
                        cur_fields.append(fld.name)

            # make new feature class
            if not sr:
                sr = self.spatialReference
            else:
                params['outSR'] = sr

            # insert cursor to write rows (using arcpy.FeatureSet() is too buggy)
            if not self.SHAPE.name in cur_fields and 'SHAPE@' not in cur_fields:
                cur_fields.append('SHAPE@')
            query_resp = self.cursor(cur_fields, where, records, params, get_all).response

            # have to override here to get domains (why are they excluded in feature set response!?)
            query_resp['fields'] = [f.asJSON() for f in _fields]
            return exportFeatureSet(out_fc, query_resp)

        else:
            print 'Cannot convert layer: "{0}" to Feature Layer, Not a vector layer!'.format(self.name)

    def clip(self, poly, output, fields='*', out_sr='', where='', envelope=False):
        """Method for spatial Query, exports geometry that intersect polygon or
        envelope features.

        Required:
            poly -- polygon (or other) features used for spatial query
            output -- full path to output feature class

        Optional:
             fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            out_sr -- output spatial refrence (WKID)
            where -- optional where clause
            envelope -- if true, the polygon features bounding box will be used.  This option
                can be used if the feature has many vertices or to check against the full extent
                of the feature class
        """
        if isinstance(poly, Geometry):
            in_geom = poly
        else:
            in_geom = Geometry(poly)
        sr = in_geom.spatialReference
        if envelope:
            geojson = in_geom.envelope()
            geometryType = 'esriGeometryEnvelope'
        else:
            geojson = in_geom.dumps()
            geometryType = in_geom.geometryType

        if not out_sr:
            out_sr = sr

        d = {'geometryType': geometryType,
             'returnGeometry': 'true',
             'geometry': geojson,
             'inSR' : sr,
             'outSR': out_sr}

        return self.layer_to_fc(output, fields, where, params=d, get_all=True, sr=out_sr)

class ImageService(BaseImageService):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
        """Image Service object

        Required:
            url -- image service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(ImageService, self).__init__(url, usr, pw, token)

    def exportImage(self, poly, out_raster, envelope=False, rendering_rule={}, interp='RSP_BilinearInterpolation', **kwargs):
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
            geometryType = 'esriGeometryEnvelope'
        else:
            geojson = in_geom.dumps()
            geometryType = in_geom.geometryType

        if sr != self.spatialReference:
            polyG = in_geom.asShape()
            polygon = polyG.projectAs(arcpy.SpatialReference(self.spatialReference))
        e = in_geom.asShape().extent
        bbox = self.adjustbbox([e.XMin, e.YMin, e.XMax, e.YMax])

        # check for raster function availability
        if not self.allowRasterFunction:
            rendering_rule = ''

        # find width and height for image size (round to whole number)
        bbox_int = map(int, bbox.split(','))
        width = abs(bbox_int[0] - bbox_int[2])
        height = abs(bbox_int[1] - bbox_int[3])

        # set params
        p = {'f':'pjson',
             'renderingRule': rendering_rule,
             'bbox': bbox,
             'format': 'tiff',
             'imageSR': sr,
             'bboxSR': self.spatialReference,
             'size': '{0},{1}'.format(width,height),
             'pixelType': self.pixelType,
             'noDataInterpretation': 'esriNoMatchAny',
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
            print 'Created: "{0}"'.format(out_raster)

    def clip(self, poly, out_raster, envelope=False, imageSR=''):
        """method to clip a raster"""
        if envelope:
            geojson = Geometry(poly).envelope() if not isinstance(poly, Geometry) else poly.envelope()
        else:
            geojson = Geometry(poly).dumps() if not isinstance(poly, Geometry) else poly.dumps()
        ren = {
          "rasterFunction" : "Clip",
          "rasterFunctionArguments" : {
            "ClippingGeometry" : json.loads(geojson),
            "ClippingType": 1
            },
          "variableName" : "Raster"
        }
        self.exportImage(poly, out_raster, rendering_rule=ren, imageSR=imageSR)



    def arithmetic(self, poly, out_raster, raster_or_constant, operation=3, envelope=False, imageSR=''):
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
        self.exportImage(poly, out_raster, rendering_rule=json.dumps(ren), imageSR=imageSR)

class Geocoder(GeocodeService):
    """class to handle Geocoding operations"""
    def __init__(self, url, usr='', pw='', token=''):
        """Geocoder object, created from GeocodeService

        Required:
            url -- Geocode service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(Geocoder, self).__init__(url, usr, pw, token)

    def exportResults(self, geocodeResultObject, out_fc):
        """exports the geocode results to feature class

        Required:
            geocodeResultObject -- results from geocode operation, must be of type
                GeocodeResult.
            out_fc -- full path to output feature class
        """
        handler = GeocodeHandler(geocodeResultObject)
        if not handler.results:
            print 'Geocoder returned 0 results! Did not create output'
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
        print 'Created: "{}"'.format(out_fc)
        return out_fc