try:
    import imp
    imp.find_module('arcpy')
    from arc_restapi import *

except ImportError:
    from open_restapi import *

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
