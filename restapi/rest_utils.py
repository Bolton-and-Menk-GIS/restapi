"""Helper functions and base classes for restapi module"""
import requests
import socket
import getpass
import fnmatch
import datetime
import os
from itertools import izip_longest

# esri fields
OID = 'esriFieldTypeOID'
SHAPE = 'esriFieldTypeGeometry'

# dictionaries
FTYPES = {'esriFieldTypeDate':'DATE',
          'esriFieldTypeString':'TEXT',
          'esriFieldTypeSingle':'FLOAT',
          'esriFieldTypeDouble':'DOUBLE',
          'esriFieldTypeSmallInteger':'SHORT',
          'esriFieldTypeInteger':'LONG',
          'esriFieldTypeGUID':'GUID',
          'esriFieldTypeRaster':'RASTER',
          'esriFieldTypeGlobalID': 'GUID'}

EXTRA ={'esriFieldTypeOID': 'OID@',
        'esriFieldTypeGeometry': 'SHAPE@'}

G_DICT = {'esriGeometryPolygon': 'Polygon',
          'esriGeometryPoint': 'Point',
          'esriGeometryPolyline': 'Polyline',
          'esriGeometryMultipoint': 'Multipoint',
          'esriGeometryEnvelope':'Envelope'}

def _post(service, _params={'f': 'json'}, ret_json=True, token=''):
    """Post Request to REST Endpoint through query string, to post
    request with data in body, use requests.post(url, data={k : v}).

    Required:
    service -- full path to REST endpoint of service

    Optional:
    _params -- parameters for posting a request
    ret_json -- return the response as JSON.  Default is True.
    token -- token to handle security (only required if security is enabled)
    """
    h = {"content-type":"text"}
    r = requests.post(service, params=add_token(_params, token), headers=h, verify=False)

    # make sure return
    if r.status_code != 200:
        raise NameError('"{0}" service not found!\n{1}'.format(service, r.raise_for_status()))
    else:
        if ret_json:
            return r.json()
        else:
            return r

def validate_name(file_name):
    """validates an output name by removing special characters"""
    import string
    path = os.sep.join(file_name.split(os.sep)[:-1]) #forward slash messes up os.path.split()
    name = file_name.split(os.sep)[-1]
    root, ext = os.path.splitext(name)
    d = {s: '_' for s in string.punctuation}
    for f,r in d.iteritems():
        root = root.replace(f,r)
    return os.path.join(path, '_'.join(root.split()) + ext)

def add_token(p_dict={'f': 'json'}, token=None):
    """Adds a token to parameters dictionary for web request

    Optional:
        p_dict -- parameter dictionary
        token -- token to add to p_dict. If no token is supplied, the original
            dictionary is returned
    """
    if token:
        p_dict['token'] = token
    return p_dict

def mil_to_date(mil):
    """date items from REST services are reported in milliseconds,
    this function will convert milliseconds to datetime objects

    Required:
        mil -- time in milliseconds
    """
    if mil == None:
        return None
    elif mil < 0:
        return datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=(mil/1000))
    else:
        return datetime.datetime.fromtimestamp(mil / 1000)

def fix_fields(service_lyr, fields, token=''):
    """fixes input fields, accepts esri field tokens too ("SHAPE@", "OID@")

    Required:
        service_lyr -- full path to url for feature layer
        fields -- list or comma delimited field list
        token -- token to handle security (only required if security is enabled)
    """
    if fields == '*':
        return fields
    if isinstance(fields, list):
        fields = ','.join(fields)
    if '@' in fields:
        _fields = list_fields(service_lyr, token)
        if 'SHAPE@' in fields:
            fields = fields.replace('SHAPE@', [f.name for f in _fields if f.type == SHAPE][0])
        if 'OID@' in fields:
            fields = fields.replace('OID@', [f.name for f in _fields if f.type == OID][0])
    return fields

def query(service_lyr, fields='*', where='1=1', add_params={}, ret_form='json', token=''):
    """runs more robust queries against a rest mapservice layer.  extra arguments can be
    passed in using the add_params dictionary.

    example: http://some_domain/ArcGIS/services/rest/some_folder/some_map_service/43    #43 is layer ID

    Required:
        service_lyr -- full path to rest endpoint of a mapservice layer ID
        field -- field or fields separated by comma to be returned from query

    Optional:
        where -- where clause to return records (ex: "TAX_NAME LIKE '%SMITH%'")
        add_params -- dictionary with any additional params you want to add
        ret_form -- default is json.  Return format for results
        token -- token to handle security (only required if security is enabled)

    list of parameters can be found here:
        http://resources.arcgis.com/en/help/rest/apiref/
    """
    # query endpoint
    endpoint = '{0}/query'.format(service_lyr)

    # check for tokens (only shape and oid)
    fields = fix_fields(service_lyr, fields, token)

    # default params
    params = {'returnGeometry' : 'true', 'outFields' : fields,
              'where': where, 'f' : ret_form}

    if add_params:
        for k,v in add_params.iteritems():
            params[k] = v

    # create kmz file if requested
    if ret_form == 'kmz':
        import codecs
        r = _post(endpoint, add_token(params, token), False)
        name = _post(service_lyr)['name']
        r.encoding = 'zlib_codec'

        # write kmz using codecs
        kmz = validate_name(r'C:\Users\{0}\Desktop\{1}.kmz'.format(os.environ['USERNAME'], name))
        with codecs.open(kmz, 'wb') as f:
            f.write(r.content)
        print 'Created: "{0}"'.format(kmz)
        return kmz
    else:
        r = _post(endpoint, add_token(params, token))
        if ret_form == 'json':
            return r
    return None

def get_layerID_by_name(service, name, token='', grp_lyr=False):
    """gets a mapservice layer ID by layer name from a service (returns an integer)

    Required:
        service -- full path to rest service
        name -- name of layer from which to grab ID

    Optional:
        token -- token to handle security (only required if security is enabled)
        grp_lyr -- default is false, does not return layer ID for group layers.  Set
            to true to search for group layers too.
    """
    r = _post(service, add_token(token=token))
    if not 'layers' in r:
        return None
    all_layers = r['layers']
    for layer in all_layers:
        if fnmatch.fnmatch(layer['name'], name):
            if grp_lyr and layer['subLayerIds'] != None:
                return layer['id']
            elif not grp_lyr and not layer['subLayerIds']:
                return layer['id']
    for tab in r['tables']:
        if fnmatch.fnmatch(tab['name'], name):
            return tab['id']
    print 'No Layer found matching "{0}"'.format(name)
    return None

def get_layer_url(service, name, token='', grp_lyr=False):
    """returns the fully qualified path to a layer url by pattern match on name,
    will return the first match.

    Required:
        service -- full path to rest service
        name -- name of layer from which to grab ID

    Optional:
        token -- token to handle security (only required if security is enabled)
        grp_lyr -- default is false, does not return layer ID for group layers.  Set
            to true to search for group layers too.
    """
    return '/'.join([service, str(get_layerID_by_name(service, name, token, grp_lyr))])

def list_fields(service_lyr, token=''):
    """lists the field objects from a mapservice layer

    Returns a list of field objects with the following properties:
        name -- name of field
        alias -- alias name of field
        type -- type of field
        length -- length of field (if applicable, otherwise returns None)

    example:
    >>> mapservice = 'http://some_domain.com/ArcGIS/rest/services/folder/a_service/MapServer/23'
    >>> for field in get_field_info(mapservice):
           print field.name, field.alias, field.type, field.length

    Required:
        service_lyr -- full path to rest endpoint of a mapservice layer ID

    Optional:
        token -- token to handle security (only required if security is enabled)
    """
    try:
        return [Field(f) for f in _post(service_lyr, add_token(token=token))['fields']]
    except: return []

def list_services(service, token='', filterer=True):
    """returns a list of all services

    Required:
        service -- full path to a rest services directory

    Optional:
        token -- token to handle security (only required if security is enabled)
        filterer -- default is true to exclude "Utilities" and "System" folders,
            set to false to list all services.
    """
    all_services = []
    r = _post(service, add_token(token=token))
    for s in r['services']:
        all_services.append('/'.join([service, s['name'], s['type']]))
    folders = r['folders']
    if filterer:
        for fld in ('Utilities', 'System'):
            try:
                folders.remove(fld)
            except: pass
    for s in folders:
        new = '/'.join([service, s])
        endpt = _post(new, add_token(token=token))
        for serv in endpt['services']:
           all_services.append('/'.join([service, serv['name'], serv['type']]))
    return all_services

def iter_services(service, token='', filterer=True):
    """returns a generator for all services

    Required:
        service -- full path to a rest services directory

    Optional:
        token -- token to handle security (only required if security is enabled)
        filterer -- default is true to exclude "Utilities" and "System" folders,
            set to false to list all services.
    """
    r = _post(service, add_token(token=token))
    for s in r['services']:
        yield '/'.join([service, s['name'], s['type']])
    folders = r['folders']
    if filterer:
        for fld in ('Utilities', 'System'):
            try:
                folders.remove(fld)
            except: pass
    for s in folders:
        new = '/'.join([service, s])
        endpt = _post(new, add_token(token=token))
        for serv in endpt['services']:
           yield '/'.join([service, serv['name'], serv['type']])

def list_layers(service, token=''):
    """lists all layers in a mapservice

    Returns a list of field objects with the following properties:
        name -- name of layer
        id -- layer id (int)
        minScale -- minimum scale range at which layer draws
        maxScale -- maximum scale range at which layer draws
        defaultVisiblity -- the layer is visible (bool)
        parentLayerId -- layer id of parent layer if in group layer (int)
        subLayerIds -- list of id's of all child layers if group layer (list of int's)

    Required:
        service -- full path to mapservice

    Optional:
        token -- token to handle security (only required if security is enabled)
    """
    r = _post(service, add_token(token=token))
    if 'layers' in r:
        return [Layer(p) for p in r['layers']]
    return []

def list_tables(service, token=''):
    """List all tables in a MapService

    Required:
        service -- map service url

    Optional:
        token -- token to handle security (only required if security is enabled)
    """
    r = _post(service, add_token(token=token))
    if 'tables' in r:
        return [Table(p) for p in r['tables']]
    return []

def validate(obj, filterer=[]):
    """will dynamically create new a new object and set the properties
    if its attribute is a dictionary

    Required:
        obj -- object to validate

    Optional:
        filterer -- list of object dictionary keys to skip
    """
    filterer.append('response')
    for prop in obj.__dict__.keys():
        p = getattr(obj, prop)
        if isinstance(p, dict) and prop not in filterer:
            setattr(obj, prop, type(prop, (object,), p))
    return obj

def generate_token(url, user='', pw=''):
    """Generates a token to handle ArcGIS Server Security, this is
    different from generating a token from the admin side.  Meant
    for external use.

    Required:
        url -- url to services directory or individual map service
        user -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
    """
    if not pw:
        pw = getpass.getpass('Type password and hit Enter:\n')
    ref, ip = '', ''
    use_body = False
    base = url.split('/rest')[0] + '/tokens'
    version = _post(url.split('arcgis')[0] + 'arcgis/rest/services')

    # changed at 10.3, must pass credentials through body now and differnt URL
    if 'currentVersion' in version:
        if float('.'.join(str(version['currentVersion']).split('.')[:2])) >= 10.3:
            use_body = True
            base += '/generateToken'
    ip = socket.gethostbyname(socket.gethostname())
    params = {'f': 'json', 'username': user,
              'password': pw, 'client': 'requestip',
              'referer': ref, 'ip': ip}

    if use_body:
        # must use requets.post() to pass data through body
        r = requests.post(url=base, data=params).json() #use data
    else:
        r = _post(base, params)
    if 'token' in r:
        return r['token']
    return None

def query_all(layer_url, oid, max_recs, where='1=1', add_params={}, token=''):
    """query all records.  Will iterate through "chunks" of OID's until all
    records have been returned (grouped by maxRecordCount)

    *Thanks to Wayne Whitley for the brilliant idea to use itertools.izip_longest()

    Required:
        layer_url -- full path to layer url
        oid -- oid field name
        max_recs -- maximum amount of records returned

    Optional:
        where -- where clause for OID selection
        add_params -- dictionary with any additional params you want to add
        token -- token to handle security (only required if security is enabled)
    """
    if isinstance(add_params, dict):
        add_params['returnIdsOnly'] = 'true'

    # get oids
    oids = sorted(query(layer_url, where=where, add_params=add_params,token=token)['objectIds'])
    print 'total records: {0}'.format(len(oids))

    # remove returnIdsOnly from dict
    del add_params['returnIdsOnly']

    # iterate through groups to form queries
    for each in izip_longest(*(iter(oids),) * max_recs):
        theRange = filter(lambda x: x != None, each) # do not want to remove OID "0"
        _min, _max = min(theRange), max(theRange)
        del each

        yield '{0} >= {1} and {0} <= {2}'.format(oid, _min, _max)

def _print_info(obj):
    """Method to print all properties of service

    Required:
        obj -- object to iterate through its __dict__ to print properties
    """
    if hasattr(obj, 'response'):
        for attr, value in sorted(obj.response.iteritems()):
            if attr != 'response':
                if attr in ('layers', 'tables', 'fields'):
                    print '\n{0}:'.format(attr.title())
                    for layer in value:
                        print '\n'.join('\t{0}: {1}'.format(k,v)
                                        for k,v in layer.iteritems())
                        print '\n'
                elif isinstance(value, dict):
                    print '{0} Properties:'.format(attr)
                    for k,v in value.iteritems():
                        print '\t{0}: {1}'.format(k,v)
                else:
                    print '{0}: {1}'.format(attr, value)
    return

def walk(url, filterer=True, token=''):
    """method to walk through ArcGIS REST Services

    Required:
        url -- url to ArcGIS REST Services directory or folder

    Optional:
        filterer -- will filter Utilities, default is True. If
          false, will list all services.
        token -- token to handle security (only required if security is enabled)

    will return tuple of folders and services from the topdown.
    (root, folders, services) example:

    ags = restapi.ArcServer(url, username, password)
    for root, folders, services in ags.walk():
        print root
        print folders
        print services
        print '\n\n'
    """
    r = _post(url, token=token)
    services = []
    for s in r['services']:
        services.append('/'.join([s['name'], s['type']]))
    folders = r['folders']
    if filterer:
        for fld in ('Utilities', 'System'):
            try:
                folders.remove(fld)
            except: pass
    yield (url, folders, services)

    for f in folders:
        new = '/'.join([url, f])
        endpt = _post(new, token=token)
        services = []
        for serv in endpt['services']:
           services.append('/'.join([serv['name'], serv['type']]))
        yield (f, endpt['folders'], services)

class Service(object):
    """class to handle ArcGIS REST Service (basic info)"""
    def __init__(self, service_dict):
        for key, value in service_dict.iteritems():
            setattr(self, key, value)

class Folder(object):
    """class to handle ArcGIS REST Folder"""
    def __init__(self, folder_url, token=''):
        self.url = folder_url.rstrip('/')
        self.token = token
        self.response = _post(self.url, token=self.token)
        for key, value in self.response.iteritems():
            if key.lower() != 'services':
                setattr(self, key, value)

    @property
    def name(self):
        """returns the folder name"""
        return self.url.split('/')[-1]

    @property
    def services(self):
        """property to list Service objects (basic info)"""
        return [Service(serv) for serv in self.response['services']]

    def list_services(self):
        """method to list services"""
        return [s.name for s in self.services]

class Field(object):
    """class for field to handle field info (name, alias, type, length)"""
    def __init__(self, f_dict):
        self.length = ''
        for key, value in f_dict.items():
            setattr(self, key, value)

class Layer(object):
    """class to handle basic layer info"""
    def __init__(self, lyr_dict):
        for key, value in lyr_dict.items():
            setattr(self, key, value)

class Table(object):
    """class to handle table info"""
    def __init__(self, tab_dict):
        for key, value in tab_dict.items():
            setattr(self, key, value)
        validate(self)

class BaseCursor(object):
    """class to handle query returns"""
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
        self.url = url
        self.token = token
        self.records = records
        layer_info = _post(self.url, token=self.token)
        self._all_fields = [Field(f) for f in layer_info['fields']]
        self.field_objects_string = fix_fields(self.url, fields, self.token)
        if fields == '*':
            self.field_objects = self._all_fields
        else:
            self.field_objects = []
            for field in self.field_objects_string.split(','):
                for fld in self._all_fields:
                    if fld.name == field:
                        self.field_objects.append(fld)
        if get_all:
            self.records = None
            oid = [f.name for f in self._all_fields if f.type == OID][0]
            if 'maxRecordCount' in layer_info:
                max_recs = layer_info['maxRecordCount']
            else:
                # guess at 500 (default 1000 limit cut in half at 10.0 if returning geometry)
                max_recs = 500

            for i, where2 in enumerate(query_all(self.url, oid, max_recs, where, add_params, self.token)):
                sql = ' and '.join(filter(None, [where.replace('1=1', ''), where2])) #remove default
                resp = query(self.url, self.field_objects_string, sql,
                             add_params=add_params, token=self.token)
                if i < 1:
                    self.response = resp
                else:
                    self.response['features'] += resp['features']

        else:
            self.response = query(self.url, self.field_objects_string, where,
                                   add_params=add_token(add_params, self.token))

        # check for errors
        if 'error' in self.response:
            print 'Errors:\n'
            for err,msg in  self.response['error'].iteritems():
                print '\t{0}: {1}'.format(err, msg)
            raise ValueError(self.response['error']['message'])

        # fix date format in milliseconds to datetime.datetime()
        self.date_indices = []
        for f in self.field_objects:
            if f.type == 'esriFieldTypeDate':
                self.date_indices.append(f.name)
        if self.date_indices:
            for att in self.response['features']:
                for field_name in self.date_indices:
                    milliseconds = att['attributes'][field_name]
                    att['attributes'][field_name] = mil_to_date(milliseconds)

    @property
    def geometryType(self):
        """returns geometry type for features"""
        if 'geometryType' in self.response:
            return self.response['geometryType']
        else:
            return None

    @property
    def spatialReference(self):
        """returns the spatial Reference for features"""
        if 'spatialReference' in self.response:
            if 'latestWkid' in self.response['spatialReference']:
                    return self.response['spatialReference']['latestWkid']
            elif 'wkid' in self.repsonse['spatialReference']:
                return self.response['spatialReference']['wkid']
        else:
            try:
                # maybe it's well known text (wkt)?
                return self.response['spatialReference']
            except:
                return None

    @property
    def fields(self):
        """field names for cursor"""
        return [f.name for f in self.field_objects]

    @property
    def features(self):
        """returns json features"""
        return self.response['features']

    @property
    def count(self):
        """returns total number of records in Cursor"""
        return len(self.features[:self.records])

class BaseRow(object):
    """Class to handle Row object"""
    def __init__(self, features, fields):
        """Row object for Cursor

        Required:
            features -- features JSON object
            fields -- fields participating in cursor
        """
        self.fields = fields
        self.features = features
        self.atts = self.features['attributes']
        self.esri_json = ''
        self.oid_field_ob = None
        self.shape_field_ob = None
        esri_fields = [f for f in self.fields if f.type in EXTRA.keys()]
        if esri_fields:
            FIELD_TYPES = [f.type for f in esri_fields]
            if OID in FIELD_TYPES:
                self.oid_field_ob = [f for f in self.fields if f.type == OID][0]
            if SHAPE in FIELD_TYPES:
                self.esri_json = self.features['geometry']
                self.shape_field_ob = [f for f in self.fields if f.type == SHAPE][0]

        # set attributes by field name access
        for field, value in self.atts.iteritems():
            setattr(self, field, value)

class BaseArcServer(object):
    """Class to handle ArcGIS Server Connection"""
    def __init__(self, url, usr='', pw='', token=''):
        """ArcGIS Server Object

        Required:
            url -- url to ArcGIS REST Services directory

        Optional:
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        self.url = url.rstrip('/')
        params = {'f': 'json'}
        self.token = token
        if not self.token:
            if usr and pw:
                self.token = generate_token(self.url, usr, pw)
                if self.token:
                    params['token'] = self.token
        self.raw_response = _post(self.url, params, ret_json=False)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        if 'error' in self.response:
            self.print_info()
        for key, value in self.response.iteritems():
            if key.lower() not in ('services', 'folders'):
                setattr(self, key, value)
        self._services = list_services(self.url, self.token)

    @property
    def services(self):
        """list of services"""
        return self._services

    @property
    def top_services(self):
        """list of top directory services (unfolderized)"""
        if 'services' in self.response:
            return self.response['services']
        print 'Services not available!'
        return []

    @property
    def folders(self):
        """list of top directory services (unfolderized)"""
        if 'folders' in self.response:
            return self.response['folders']
        print 'Folders not available!'
        return []

    def list_services(self, exclude_utilities=True):
        """return list of services

        Optional:
            exclude_utilities -- default is True, set to False to
            view System and Utility services
        """
        return iter_services(self.url, self.token, exclude_utilities)

    def get_service_url(self, wildcard='*', _list=False):
        """method to return a service url

        Optional:
            wildcard -- wildcard used to grab service name (ex "moun*featureserver")
            _list -- default is false.  If true, will return a list of all services
                matching the wildcard.  If false, first match is returned.
        """
        if '*' in wildcard:
            if wildcard == '*':
                return self.services[0]
            else:
                if _list:
                    return [s for s in self.services if fnmatch.fnmatch(s, wildcard)]
            for s in self.services:
                if fnmatch.fnmatch(s, wildcard):
                    return s
        else:
            if _list:
                return [s for s in self.services if wildcard.lower() in s.lower()]
            for s in self.services:
                if wildcard.lower() in s.lower():
                    return s
        print '"{0}" not found in services'.format(wildcard)
        return ''

    def print_info(self):
        """Method to print all properties of service"""
        _print_info(self)

    def get_folders(self):
        """method to get folder objects"""
        folder_objects = []
        for folder in self.folders:
            folder_url = '/'.join([self.url, folder])
            folder_objects.append(Folder(folder_url, self.token))
        return folder_objects

    def walk(self, filterer=True):
        """method to walk through ArcGIS REST Services

        Optional:
            filterer -- will filter Utilities, default is True. If
            false, will list all services.

        will return tuple of folders and services from the topdown.
        (root, folders, services) example:

        ags = restapi.ArcServer(url, username, password)
        for root, folders, services in ags.walk():
            print root
            print folders
            print services
            print '\n\n'
        """
        return walk(self.url, filterer, self.token)

    def refresh(self):
        """refreshes the MapService"""
        self.__init__(self.url, token=self.token)

class BaseMapService(object):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
        """MapService Object

        Required:
            url -- map service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        self.url = url.rstrip('/')
        params = {'f': 'json'}
        self.token = token
        if not self.token:
            if usr and pw:
                self.token = generate_token(self.url, usr, pw)
                if self.token:
                    params['token'] = self.token
        else:
            params['token'] = self.token
        self.raw_response = _post(self.url, params, ret_json=False)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        if 'error' in self.response:
            self.print_info()
        self.layers = []
        self.tables = []
        if 'layers' in self.response:
            self.layers = [Layer(p) for p in self.response['layers']]
        if 'tables' in self.response:
            self.tables = [Table(p) for p in self.response['tables']]
        for key, value in self.response.items():
            if key not in ('layers', 'tables'):
                setattr(self, key, value)
        try:
            if 'latestWkid' in self.response['spatialReference']:
                self.spatialReference = self.response['spatialReference']['latestWkid']
            else:
                self.spatialReference = self.response['spatialReference']['wkid']
        except:
            try:
                # try well known text (wkt)
                self.spatialReference = self.response['spatialReference']
            except:
                self.spatialReference = None
        validate(self, ['spatialReference'])
        self.properties = sorted(self.__dict__.keys())

    def list_layers(self):
        """Method to return a list of layer names in a MapService"""
        return [l.name for l in self.layers]

    def list_tables(self):
        """Method to return a list of layer names in a MapService"""
        return [t.name for t in self.tables]

    def list_fields(self, layer_name):
        """Method to return field names from a layer"""
        lyr = get_layer_url(self.url, layer_name, self.token)
        return [f.name for f in list_fields(lyr, self.token)]

    def get_fields(self, layer_name):
        """Method to return field objects from a layer"""
        lyr = get_layer_url(self.url, layer_name, self.token)
        return list_fields(lyr, self.token)

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

    def layer_to_kmz(self, layer_name, flds='*', where='1=1', params={}):
        """Method to create kmz from query

        Required:
            layer_name -- name of map service layer to export to fc

        Optional:
            flds -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            where -- optional where clause
            params -- dictionary of parameters for query
        """
        lyr = self.layer(layer_name)
        lyr.layer_to_kmz(flds, where, params)

    def print_info(self):
        """Method to print all properties of service"""
        _print_info(self)

    def refresh(self):
        """refreshes the MapService"""
        self.__init__(self.url, token=self.token)

class BaseMapServiceLayer(object):
    """Class to handle advanced layer properties"""
    def __init__(self, url='', usr='', pw='', token=''):
        params = {'f': 'json'}
        self.url = url
        self.token = token
        if not self.token:
            if usr and pw:
                self.token = generate_token(self.url, usr, pw)
                if self.token:
                    params['token'] = self.token
        else:
            params['token'] = self.token
        self.response = _post(self.url, add_token(token=self.token))
        for key, value in self.response.iteritems():
            setattr(self, key, value)

        validate(self, ['fields', 'spatialReference'])
        self.fields_dict = self.response['fields']
        self.fields = [Field(f) for f in self.fields_dict]

    @property
    def OID(self):
        """OID field object"""
        try:
            return [f for f in self.fields if f.type == OID][0]
        except:
            return None

    @property
    def SHAPE(self):
        """SHAPE field object"""
        try:
            return [f for f in self.fields if f.type == SHAPE][0]
        except:
            return None

    @property
    def spatialReference(self):
        """spatial reference (WKID)"""
        try:
            if 'latestWkid' in self.extent.spatialReference:
                return self.extent.spatialReference['latestWkid']
            else:
                return self.extent.spatialReference['wkid']
        except:
            return None

    def print_info(self):
        """Method to print all layer info"""
        _print_info(self)

    def list_fields(self):
        """method to list field names"""
        return [f.name for f in self.fields]

    def layer_to_kmz(self, flds='*', where='1=1', params={}):
        """Method to create kmz from query

        Required:
            layer_name -- name of map service layer to export to fc

        Optional:
            flds -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            where -- optional where clause
            params -- dictionary of parameters for query
        """
        return query(self.url, flds, where=where, add_params=params, ret_form='kmz', token=self.token)

    def refresh(self):
        """refreshes the MapServiceLayer"""
        self.__init__(self.url, token=self.token)

class BaseImageService(object):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
        """MapService Object

        Required:
            url -- image service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        self.url = url.rstrip('/')
        params = {'f': 'json'}
        self.token = token
        if not self.token:
            if usr and pw:
                self.token = generate_token(self.url, usr, pw)
                if self.token:
                    params['token'] = self.token
        else:
            params['token'] = self.token
        self.raw_response = _post(self.url, params, ret_json=False)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        if 'error' in self.response:
            self.print_info()

        for k, v in self.response.iteritems():
            if k != 'spatialReference':
                setattr(self, k, v)

    @property
    def spatialReference(self):
        try:
            if 'latestWkid' in self.extent.spatialReference:
                return self.extent.spatialReference['latestWkid']
            else:
                return self.extent.spatialReference['wkid']
        except:
            return None

    def print_info(self):
        """Method to print all properties of service"""
        _print_info(self)

    def refresh(self):
        """refreshes the ImageService"""
        self.__init__(self.url, token=self.token)