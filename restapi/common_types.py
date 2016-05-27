try:
    import imp
    imp.find_module('arcpy')
    from arc_restapi import *

except ImportError:
    from open_restapi import *

class Cursor(FeatureSet):
    """Class to handle Cursor object"""
    def __init__(self, feature_set, fieldOrder=[]):
        """Cursor object for a feature set

        Required:
            feature_set -- feature set as json or restapi.FeatureSet() object

        Optional:
            fieldOrder -- order of fields for cursor row returns.  To explicitly
                specify and OBJECTID field or Shape (geometry field), you must use
                the field tokens 'OID@' and 'SHAPE@' respectively.

        """
        if isinstance(feature_set, FeatureSet):
            feature_set = feature_set.json
        super(Cursor, self).__init__(feature_set)
        self.fieldOrder = self.__validateOrderBy(fieldOrder)

    @property
    def date_fields(self):
        """gets the names of any date fields within feature set"""
        return [f.name for f in self.fields if f.type == 'esriFieldTypeDate']

    @property
    def field_names(self):
        """gets the field names for feature set"""
        return self.fieldOrder

    @property
    def OIDFieldName(self):
        """gets the OID field name if it exists in feature set"""
        try:
            return [f.name for f in self.fields if f.type == OID][0]
        except IndexError:
           return None

    @property
    def ShapeFieldName(self):
        """gets the Shape field name if it exists in feature set"""
        try:
            return [f.name for f in self.fields if f.type == SHAPE][0]
        except IndexError:
           return None

    def get_rows(self):
        """returns row objects"""
        for feature in self.features:
            yield self.createRow(feature, self.spatialReference)

    def rows(self):
        """returns Cursor.rows() as generator"""
        for feature in self.features:
            yield self.createRow(feature, self.spatialReference).values

    def getRow(self, index):
        """returns row object at index"""
        return [r for r in self.get_rows()][index]

    def __validateOrderBy(self, fields):
        """fixes "fieldOrder" input fields, accepts esri field tokens too ("SHAPE@", "OID@")

        Required:
            fields -- list or comma delimited field list
        """
        if isinstance(fields, basestring):
            fields = fields.split(',')
        for i,f in enumerate(fields):
            if '@' in f:
                fields[i] = f.upper()
            if f == self.ShapeFieldName:
                fields[i] = 'SHAPE@'
            if f == self.OIDFieldName:
                fields[i] = 'OID@'

        if not fields:
            fields = self.field_names

        return fields

    def __iter__(self):
        """returns Cursor.rows()"""
        return self.rows()

    def createRow(self, feature, spatialReference):

        cursor = self

        class Row(object):
            """Class to handle Row object"""
            def __init__(self, feature, spatialReference):
                """Row object for Cursor

                Required:
                    feature -- features JSON object
                """
                self.feature = feature
                self.spatialReference = spatialReference

            @property
            def geometry(self):
                """returns a restapi.Geometry() object"""
                if 'geometry' in self.feature:
                    gd = copy.deepcopy(self.feature.geometry)
                    gd['spatialReference'] = cursor.json.spatialReference
                    return Geometry(gd)
                return None

            @property
            def oid(self):
                """returns the OID for row"""
                if cursor.OIDFieldName:
                    return self.get(cursor.OIDFieldName)
                return None

            @property
            def values(self):
                """returns values as tuple"""
                # fix date format in milliseconds to datetime.datetime()
                vals = []
                for i, field in enumerate(cursor.fieldOrder):
                    if field in cursor.date_fields:
                        vals.append(mil_to_date(self.feature.attributes[field]))
                    else:
                        if field == 'OID@':
                            vals.append(self.oid)
                        elif field == 'SHAPE@':
                            vals.append(self.geometry)
                        else:
                            vals.append(self.feature.attributes[field])

                return tuple(vals)

            def get(self, field):
                """gets an attribute by field name

                Required:
                    field -- name of field for which to get the value
                """
                return self.feature.attributes.get(field)

            def __getitem__(self, i):
                """allows for getting a field value by index"""
                return self.values[i]

        return Row(feature, spatialReference)

class ArcServer(BaseArcServer):

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

    def layer(self, name_or_id):
        """Method to return a layer object with advanced properties by name

        Required:
            name -- layer name (supports wildcard syntax*) or id (must be of type <int>)
        """
        if isinstance(name_or_id, int):
            # reference by id directly
            return MapServiceLayer('/'.join([self.url, str(name_or_id)]), token=self.token)

        layer_path = get_layer_url(self.url, name_or_id, self.token)
        if layer_path:
            return MapServiceLayer(layer_path, token=self.token)
        else:
            print('Layer "{0}" not found!'.format(name_or_id))

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

class FeatureService(MapService):
    """class to handle Feature Service

    Required:
        url -- image service url

    Optional (below params only required if security is enabled):
        usr -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
        token -- token to handle security (alternative to usr and pw)
        proxy -- option to use proxy page to handle security, need to provide
            full path to proxy url.
    """

    @property
    def replicas(self):
        """returns a list of replica objects"""
        if self.syncEnabled:
            reps = POST(self.url + '/replicas', cookies=self._cookie)
            return [namedTuple('Replica', r) for r in reps]
        else:
            return []

    def layer(self, name):
        """Method to return a layer object with advanced properties by name

        Required:
            name -- layer name (supports wildcard syntax*)
        """
        layer_path = self.get_layer_url(name)
        if layer_path:
            return FeatureLayer(layer_path, token=self.token)
        else:
            print('Layer "{0}" not found!'.format(name))

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

    def createReplica(self, layers, replicaName, geometry='', geometryType='', inSR='', replicaSR='', **kwargs):
        """query attachments, returns a JSON object

        Required:
            layers -- list of layers to create replicas for (valid inputs below)
            replicaName -- name of replica

        Optional:
            geometry -- optional geometry to query features
            geometryType -- type of geometry
            inSR -- input spatial reference for geometry
            replicaSR -- output spatial reference for replica data
            **kwargs -- optional keyword arguments for createReplica request
        """
        if hasattr(self, 'syncEnabled') and not self.syncEnabled:
            raise NotImplementedError('FeatureService "{}" does not support Sync!'.format(self.url))

        # validate layers
        if isinstance(layers, basestring):
            layers = [l.strip() for l in layers.split(',')]

        elif not isinstance(layers, (list, tuple)):
            layers = [layers]

        if all(map(lambda x: isinstance(x, int), layers)):
            layers = ','.join(map(str, layers))

        elif all(map(lambda x: isinstance(x, basestring), layers)):
            layers = ','.join(map(str, filter(lambda x: x is not None,
                                [s.id for s in self.layers if s.name.lower()
                                 in [l.lower() for l in layers]])))

        if not geometry and not geometryType:
            ext = self.initialExtent
            inSR = self.initialExtent.spatialReference
            geometry= ','.join(map(str, [ext.xmin,ext.ymin,ext.xmax,ext.ymax]))
            geometryType = 'esriGeometryEnvelope'
            inSR = self.spatialReference
            useGeometry = False
        else:
            useGeometry = True
            if isinstance(geometry, dict) and 'spatialReference' in geometry and not inSR:
                inSR = geometry['spatialReference']


        if not replicaSR:
            replicaSR = self.spatialReference

        validated = layers.split(',')
        options = {'replicaName': replicaName,
                   'layers': layers,
                   'layerQueries': '',
                   'geometry': geometry,
                   'geometryType': geometryType,
                   'inSR': inSR,
                   'replicaSR':	replicaSR,
                   'transportType':	'esriTransportTypeUrl',
                   'returnAttachments':	'true',
                   'returnAttachmentsDataByUrl': 'true',
                   'async':	'false',
                   'f': 'pjson',
                   'dataFormat': 'json',
                   'replicaOptions': '',
                   }

        for k,v in kwargs.iteritems():
            options[k] = v
            if k == 'layerQueries':
                if options[k]:
                    if isinstance(options[k], basestring):
                        options[k] = json.loads(options[k])
                    for key in options[k].keys():
                        options[k][key]['useGeometry'] = useGeometry
                        options[k] = json.dumps(options[k])

        if self.syncCapabilities.supportsPerReplicaSync:
            options['syncModel'] = 'perReplica'
        else:
            options['syncModel'] = 'perLayer'

        if options['async'] in ('true', True) and self.syncCapabilities.supportsAsync:
            st = POST(self.url + '/createReplica', options, cookies=self._cookie)
            while 'statusUrl' not in st:
                time.sleep(1)
        else:
            options['async'] = 'false'
            st = POST(self.url + '/createReplica', options, cookies=self._cookie)

        RequestError(st)
        js = POST(st['URL'] if 'URL' in st else st['statusUrl'], cookies=self._cookie)
        RequestError(js)

        if not replicaSR:
            replicaSR = self.spatialReference

        repLayers = []
        for i,l in enumerate(js['layers']):
            l['layerURL'] = '/'.join([self.url, validated[i]])
            layer_ob = FeatureLayer(l['layerURL'], token=self.token)
            l['fields'] = layer_ob.fields
            l['name'] = layer_ob.name
            l['geometryType'] = layer_ob.geometryType
            l['spatialReference'] = replicaSR
            if not 'attachments' in l:
                l['attachments'] = []
            repLayers.append(namedTuple('ReplicaLayer', l))

        rep_dict = js
        rep_dict['layers'] = repLayers
        return namedTuple('Replica', rep_dict)

    def replicaInfo(self, replicaID):
        """get replica information

        Required:
            replicaID -- ID of replica
        """
        query_url = self.url + '/replicas/{}'.format(replicaID)
        return namedTuple('ReplicaInfo', POST(query_url, cookies=self._cookie))

    def syncReplica(self, replicaID, **kwargs):
        """synchronize a replica.  Must be called to sync edits before a fresh replica
        can be obtained next time createReplica is called.  Replicas are snapshots in
        time of the first time the user creates a replica, and will not be reloaded
        until synchronization has occured.  A new version is created for each subsequent
        replica, but it is cached data.

        It is also recommended to unregister a replica
        AFTER sync has occured.  Alternatively, setting the "closeReplica" keyword
        argument to True will unregister the replica after sync.

        More info can be found here:
            http://server.arcgis.com/en/server/latest/publish-services/windows/prepare-data-for-offline-use.htm

        and here for key word argument parameters:
            http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Synchronize_Replica/02r3000000vv000000/

        Required:
            replicaID -- ID of replica
        """
        query_url = self.url + '/synchronizeReplica'
        params = {'replicaID': replicaID}

        for k,v in kwargs.iteritems():
            params[k] = v

        return POST(query_url, params, cookies=self._cookie)


    def unRegisterReplica(self, replicaID):
        """unregisters a replica on the feature service

        Required:
            replicaID -- the ID of the replica registered with the service
        """
        query_url = self.url + '/unRegisterReplica'
        params = {'replicaID': replicaID}
        return POST(query_url, params, cookies=self._cookie)

class FeatureLayer(MapServiceLayer):
    """class to handle Feature Service Layer

        Required:
            url -- image service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
        """

    def addFeatures(self, features, gdbVersion='', rollbackOnFailure=True):
        """add new features to feature service layer

        features -- esri JSON representation of features

        ex:
        adds = [{"geometry":
                     {"x":-10350208.415443439,
                      "y":5663994.806146532,
                      "spatialReference":
                          {"wkid":102100}},
                 "attributes":
                     {"Utility_Type":2,"Five_Yr_Plan":"No","Rating":None,"Inspection_Date":1429885595000}}]
        """
        add_url = self.url + '/addFeatures'
        params = {'features': json.dumps(features) if isinstance(features, list) else features,
                  'gdbVersion': gdbVersion,
                  'rollbackOnFailure': str(rollbackOnFailure).lower(),
                  'f': 'pjson'}

        # update features
        result = EditResult(POST(add_url, params, cookies=self._cookie))
        result.summary()
        return result

    def updateFeatures(self, features, gdbVersion='', rollbackOnFailure=True):
        """update features in feature service layer

        Required:
            features -- features to be updated (JSON)

        Optional:
            gdbVersion -- geodatabase version to apply edits
            rollbackOnFailure -- specify if the edits should be applied only if all submitted edits succeed

        # example syntax
        updates = [{"geometry":
                {"x":-10350208.415443439,
                 "y":5663994.806146532,
                 "spatialReference":
                     {"wkid":102100}},
            "attributes":
                {"Five_Yr_Plan":"Yes","Rating":90,"OBJECTID":1}}] #only fields that were changed!
        """
        update_url = self.url + '/updateFeatures'
        params = {'features': json.dumps(features),
                  'gdbVersion': gdbVersion,
                  'rollbackOnFailure': rollbackOnFailure,
                  'f': 'json'}

        # update features
        result = EditResult(POST(update_url, params, cookies=self._cookie))
        result.summary()
        return result

    def deleteFeatures(self, oids='', where='', geometry='', geometryType='',
                       spatialRel='', inSR='', gdbVersion='', rollbackOnFailure=True):
        """deletes features based on list of OIDs

        Optional:
            oids -- list of oids or comma separated values
            where -- where clause for features to be deleted.  All selected features will be deleted
            geometry -- geometry JSON object used to delete features.
            geometryType -- type of geometry
            spatialRel -- spatial relationship.  Default is "esriSpatialRelationshipIntersects"
            inSR -- input spatial reference for geometry
            gdbVersion -- geodatabase version to apply edits
            rollbackOnFailure -- specify if the edits should be applied only if all submitted edits succeed

        oids format example:
            oids = [1, 2, 3] # list
            oids = "1, 2, 4" # as string
        """
        if not geometryType:
            geometryType = 'esriGeometryEnvelope'
        if not spatialRel:
            spatialRel = 'esriSpatialRelIntersects'

        del_url = self.url + '/deleteFeatures'
        if isinstance(oids, (list, tuple)):
            oids = ', '.join(map(str, oids))
        params = {'objectIds': oids,
                  'where': where,
                  'geometry': geometry,
                  'geometryType': geometryType,
                  'spatialRel': spatialRel,
                  'gdbVersion': gdbVersion,
                  'rollbackOnFailure': rollbackOnFailure,
                  'f': 'json'}

        # delete features
        result = EditResult(POST(del_url, params, cookies=self._cookie))
        result.summary()
        return result

    def applyEdits(self, adds='', updates='', deletes='', gdbVersion='', rollbackOnFailure=True):
        """apply edits on a feature service layer

        Optional:
            adds -- features to add (JSON)
            updates -- features to be updated (JSON)
            deletes -- oids to be deleted (list, tuple, or comma separated string)
            gdbVersion -- geodatabase version to apply edits
            rollbackOnFailure -- specify if the edits should be applied only if all submitted edits succeed
        """
        # TO DO
        pass

    def addAttachment(self, oid, attachment, content_type='', gdbVersion=''):
        """add an attachment to a feature service layer

        Required:
            oid -- OBJECT ID of feature in which to add attachment
            attachment -- path to attachment

        Optional:
            content_type -- html media type for "content_type" header.  If nothing provided,
                will use a best guess based on file extension (using mimetypes)
            gdbVersion -- geodatabase version for attachment

            valid content types can be found here @:
                http://en.wikipedia.org/wiki/Internet_media_type
        """
        if self.hasAttachments:

            # use mimetypes to guess "content_type"
            if not content_type:
                import mimetypes
                known = mimetypes.types_map
                common = mimetypes.common_types
                ext = os.path.splitext(attachment)[-1].lower()
                if ext in known:
                    content_type = known[ext]
                elif ext in common:
                    content_type = common[ext]

            # make post request
            att_url = '{}/{}/addAttachment'.format(self.url, oid)
            files = {'attachment': (os.path.basename(attachment), open(attachment, 'rb'), content_type)}
            params = {'f': 'json'}
            if gdbVersion:
                params['gdbVersion'] = gdbVersion
            r = requests.post(att_url, params, files=files, cookies=self._cookie, verify=False).json()
            if 'addAttachmentResult' in r:
                print(r['addAttachmentResult'])
            return r

        else:
            raise NotImplementedError('FeatureLayer "{}" does not support attachments!'.format(self.name))

    def calculate(self, exp, where='1=1', sqlFormat='standard'):
        """calculate a field in a Feature Layer

        Required:
            exp -- expression as JSON [{"field": "Street", "value": "Main St"},..]

        Optional:
            where -- where clause for field calculator
            sqlFormat -- SQL format for expression (standard|native)

        Example expressions as JSON:
            exp = [{"field" : "Quality", "value" : 3}]
            exp =[{"field" : "A", "sqlExpression" : "B*3"}]
        """
        if hasattr(self, 'supportsCalculate') and self.supportsCalculate:
            calc_url = self.url + '/calculate'
            p = {'returnIdsOnly':'true',
                'returnGeometry': 'false',
                'outFields': '',
                'calcExpression': json.dumps(exp),
                'sqlFormat': sqlFormat}

            return POST(calc_url, where=where, add_params=p, cookies=self._cookie)

        else:
            raise NotImplementedError('FeatureLayer "{}" does not support field calculations!'.format(self.name))

    def __repr__(self):
        """string representation with service name"""
        return '<FeatureLayer: "{}" (id: {})>'.format(self.name, self.id)
