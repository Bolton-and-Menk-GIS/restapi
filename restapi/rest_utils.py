"""Helper functions and base classes for restapi module"""
import requests
import getpass
import fnmatch
import datetime
import collections
import json
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
          'esriFieldTypeInteger':'LONG'}

SKIP_FIELDS = {
          'esriFieldTypeGUID':'GUID',
          'esriFieldTypeRaster':'RASTER',
          'esriFieldTypeGlobalID': 'GUID',
          'esriFieldTypeBlob': 'BLOB'}

EXTRA ={'esriFieldTypeOID': 'OID@',
        'esriFieldTypeGeometry': 'SHAPE@'}

G_DICT = {'esriGeometryPolygon': 'Polygon',
          'esriGeometryPoint': 'Point',
          'esriGeometryPolyline': 'Polyline',
          'esriGeometryMultipoint': 'Multipoint',
          'esriGeometryEnvelope':'Envelope'}

FIELD_SCHEMA = collections.namedtuple('FieldSchema', 'name type')

def Field(f_dict={}, name='Field'):
    """returns a named tuple for lightweight, dynamic Field objects

    f_dict -- dictionary containing Field properties
    name -- name for Field object"""
    # make sure always has at least name, length, type
    for attr in ('name', 'length', 'type'):
        if not attr in f_dict:
            f_dict[attr] = None
    col_ob = collections.namedtuple(name, ' '.join(f_dict.keys()))
    return col_ob(**f_dict)

def Round(x, base=5):
    """round to nearest n"""
    return int(base * round(float(x)/base))

def POST(service, _params={'f': 'json'}, ret_json=True, token=''):
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
        return datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(seconds=(mil/1000))
    else:
        return datetime.datetime.utcfromtimestamp(mil / 1000)

def date_to_mil(date):
    """converts datetime.datetime() object to milliseconds

    date -- datetime.datetime() object"""
    if isinstance(date, datetime.datetime):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return long((date - epoch).total_seconds() * 1000.0)

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
        r = POST(endpoint, add_token(params, token), False)
        name = POST(service_lyr)['name']
        r.encoding = 'zlib_codec'

        # write kmz using codecs
        kmz = validate_name(r'C:\Users\{0}\Desktop\{1}.kmz'.format(os.environ['USERNAME'], name))
        with codecs.open(kmz, 'wb') as f:
            f.write(r.content)
        print 'Created: "{0}"'.format(kmz)
        return kmz
    else:
        r = POST(endpoint, add_token(params, token))
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
    r = POST(service, add_token(token=token))
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
        return [Field(f) for f in POST(service_lyr, add_token(token=token))['fields']]
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
    r = POST(service, add_token(token=token))
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
        endpt = POST(new, add_token(token=token))
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
    r = POST(service, add_token(token=token))
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
        endpt = POST(new, add_token(token=token))
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
    r = POST(service, add_token(token=token))
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
    r = POST(service, add_token(token=token))
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
    atts = []
    if hasattr(obj, '__dict__'):
        atts = obj.__dict__.keys()
    elif hasattr(obj, '__slots__'):
        atts = obj.__slots__
    for prop in atts:
        p = getattr(obj, prop)
        if isinstance(p, dict) and prop not in filterer:
            setattr(obj, prop, type(prop, (object,), p))
    return obj

def generate_token(url, user='', pw='', expiration=60):
    """Generates a token to handle ArcGIS Server Security, this is
    different from generating a token from the admin side.  Meant
    for external use.

    Required:
        url -- url to services directory or individual map service
        user -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server

    Optional:
        expiration -- time (in minutes) for token lifetime.  Max is 100.
    """
    if not pw:
        pw = getpass.getpass('Type password and hit Enter:\n')
    ref = ''
    use_body = False
    base = url.split('/rest')[0] + '/tokens'
    version = POST(url.split('arcgis')[0] + 'arcgis/rest/services')
    params = {'f': 'json',
              'username': user,
              'password': pw,
              'client': 'requestip',
              'referer': ref,
              'expiration': min([expiration, 100])} #set max at 100

    # changed at 10.3, must pass credentials through body now and differnt URL
    if 'currentVersion' in version:
        if float('.'.join(str(version['currentVersion']).split('.')[:2])) >= 10.3:
            use_body = True
            base += '/generateToken'

    r = requests.post(url=base, data=params).json()
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
                if attr in ('layers', 'tables', 'fields') or 'fields' in attr.lower():
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
    r = POST(url, token=token)
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
        endpt = POST(new, token=token)
        services = []
        for serv in endpt['services']:
           services.append('/'.join([serv['name'], serv['type']]))
        yield (f, endpt['folders'], services)

class RequestError(object):
    """class to handle restapi request errors"""
    def __init__(self, err):
        if 'error' in err:
            raise RuntimeError('\n' + '\n'.join('{} : {}'.format(k,v) for k,v in err['error'].items()))

class Service(object):
    """class to handle ArcGIS REST Service (basic info)"""
    __slots__ = ['name', 'type']
    def __init__(self, service_dict):
        for key, value in service_dict.iteritems():
            setattr(self, key, value)

class Folder(object):
    """class to handle ArcGIS REST Folder"""
    __slots__ = ['url', 'folders', 'token', 'currentVersion', 'response',
                 'name', 'services', 'list_services']
    def __init__(self, folder_url, token=''):
        self.url = folder_url.rstrip('/')
        self.token = token
        self.response = POST(self.url, token=self.token)
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
        return ['/'.join([s.name, s.type]) for s in self.services]

class Layer(object):
    """class to handle basic layer info"""
    __slots__ = ['subLayerIds', 'name', 'maxScale', 'defaultVisibility',
                 'parentLayerId', 'minScale', 'id']
    def __init__(self, lyr_dict):
        for key, value in lyr_dict.items():
            setattr(self, key, value)

class Table(object):
    """class to handle table info"""
    __slots__ = ['id', 'name']
    def __init__(self, tab_dict):
        for key, value in tab_dict.items():
            setattr(self, key, value)

class GPParam(object):
    """class to handle GP Parameter Info"""
    __slots__ = ['name', 'dataType', 'displayName','description', 'paramInfo',
                 'direction', 'defaultValue', 'parameterType', 'category']
    def __init__(self, p_dict):
        """handler for GP Task parameters

        p_dict -- JSON object or dictionary containing parameter info
        """
        for key, value in p_dict.items():
            setattr(self, key, value)
        self.paramInfo = p_dict

class GPResult(object):
    """class to handle GP Result"""
    def __init__(self, res_dict):
        """handler for GP result

        res_dict -- JSON response from GP Task execution
        """
        self.response = res_dict
        if 'results' in res_dict:
            for k,v in res_dict['results'][0].items():
                setattr(self, k, v)
        else:
            RequestError(res_dict)

    @property
    def messages(self):
        """return messages as JSON"""
        if 'messages' in self.response:
            return self.response['messages']
        else:
            return []

    def print_messages(self):
        """prints all the GP messages"""
        for msg in self.messages:
            print msg['description']

class GeocodeResult(object):
    """class to handle Reverse Geocode Result"""
    __slots__ = ['response', 'spatialReference','Result', 'type',
                'candidates', 'locations', 'address', 'results']

    def __init__(self, res_dict, geo_type):
        """geocode response object

        Required:
            res_dict -- JSON response from geocode request
            geo_type -- type of geocode operation (reverseGeocode|findAddressCandidates|geocodeAddresses)
        """
        RequestError(res_dict)
        self.response = res_dict
        self.type = 'esri_' + geo_type
        self.spatialReference = None
        self.candidates = []
        self.locations = []
        self.address = []
        if 'spatialReference' in self.response:
            self.spatialReference = self.response['spatialReference']

        if self.type == 'esri_reverseGeocode':
            addr_dict = {}
            self.spatialReference = self.response['location']['spatialReference']
            loc = self.response['location']
            addr_dict['location'] = {'x': loc['x'], 'y': loc['y']}
            addr_dict['attributes'] = self.response['address']
            addr_dict['address'] = self.response['address']['Address']
            addr_dict['score'] = None
            self.address.append(addr_dict)

        # legacy response from find? <- deprecated?
        # http://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find #still works
        elif self.type == 'esri_find':
            # format legacy results
            for res in self.response['locations']:
                ref_dict = {}
                for k,v in res.iteritems():
                    if k == 'name':
                        ref_dict['address'] = v
                    elif k == 'feature':
                        atts_dict = {}
                        for att, val in res[k].iteritems():
                            if att == 'geometry':
                                ref_dict['location'] = val
                            elif att == 'attributes':
                                for att2, val2 in res[k][att].iteritems():
                                    if att2.lower() == 'score':
                                        ref_dict['score'] = val2
                                    else:
                                        atts_dict[att2] = val2
                            ref_dict['attributes'] = atts_dict
                self.locations.append(ref_dict)

        else:
            if self.type == 'esri_findAddressCandidates':
                self.candidates = self.response['candidates']

            elif self.type == 'esri_geocodeAddresses':
                self.locations = self.response['locations']

        self.Result = collections.namedtuple('GeocodeResult_result',
                                        'address attributes location score')
    @property
    def results(self):
        """returns list of result objects"""
        results = []
        for res in self.address + self.candidates + self.locations:
            results.append(self.Result(*[v for k,v in sorted(res.items())]))
        return results

    def __len__(self):
        """get count of results"""
        return len(self.results)

    def __iter__(self):
        """return an iterator (as generator)"""
        for r in self.results:
            yield r

class EditResult(object):
    """class to handle Edit operation results"""
    __slots__ = ['addResults', 'updateResults', 'deleteResults',
                'summary', 'affectedOIDs', 'failedOIDs']
    def __init__(self, res_dict):
        RequestError(res_dict)
        self.failedOIDs = []
        self.addResults = []
        self.updateResults = []
        self.deleteResults = []
        for key, value in res_dict.iteritems():
            for v in value:
                if v['success'] in (True, 'true'):
                    getattr(self, key).append(v['objectId'])
                else:
                    self.failedOIDs.append(v['objectId'])
        self.affectedOIDs = self.addResults + self.updateResults + self.deleteResults

    def summary(self):
        """print summary of edit operation"""
        if self.affectedOIDs:
            if self.addResults:
                print 'Added {} feature(s)'.format(len(self.addResults))
            if self.updateResults:
                print 'Updated {} feature(s)'.format(len(self.updateResults))
            if self.deleteResults:
                print 'Deleted {} feature(s)'.format(len(self.deleteResults))
        if self.failedOIDs:
            print 'Failed to edit {0} feature(s)!\n{1}'.format(len(self.failedOIDs), self.failedOIDs)

    def __len__(self):
        """return count of affected OIDs"""
        return len(self.affectedOIDs)

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
        layer_info = POST(self.url, token=self.token)
        self._all_fields = [Field(f) for f in layer_info['fields']]
        self.field_objects_string = fix_fields(self.url, fields, self.token)
        if fields == '*':
            self.field_objects = [f for f in self._all_fields if f.type not in SKIP_FIELDS.keys()]
        else:
            self.field_objects = []
            for field in self.field_objects_string.split(','):
                for fld in self._all_fields:
                    if fld.name == field and fld.type not in SKIP_FIELDS.keys():
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
        """returns total number of records in Cursor (user queried)"""
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

class RESTEndpoint(object):
    """Base REST Endpoint Object to handle credentials and get JSON response

    Required:
        url -- image service url

    Optional (below params only required if security is enabled):
        usr -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
        token -- token to handle security (alternative to usr and pw)
    """
    def __init__(self, url, usr='', pw='', token=''):
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
        self.raw_response = POST(self.url, params, ret_json=False)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        if 'error' in self.response:
            self.print_info()

    def print_info(self):
        """Method to print all properties of service"""
        _print_info(self)

class BaseArcServer(RESTEndpoint):
    """Class to handle ArcGIS Server Connection"""
    def __init__(self, url, usr='', pw='', token=''):
        super(BaseArcServer, self).__init__(url, usr, pw, token)

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
    def mapServices(self):
        """list of all MapServer objects"""
        return [s for s in self.services if s.endswith('MapServer')]

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

    def __iter__(self):
        """returns an generator for services"""
        return self.list_services()

class FeatureService(RESTEndpoint):
    """class to handle Feature Service

    Required:
        url -- image service url

    Optional (below params only required if security is enabled):
        usr -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
        token -- token to handle security (alternative to usr and pw)
    """
    def __init__(self, url, usr='', pw='', token=''):
        super(FeatureService, self).__init__(url, usr, pw, token)

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
            # try well known text (wkt)
            self.spatialReference = self.response['spatialReference']

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

class FeatureLayer(RESTEndpoint):
    def __init__(self, url, usr='', pw='', token=''):
        """class to handle Feature Service Layer

        Required:
            url -- image service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(FeatureLayer, self).__init__(url, usr, pw, token)

        for key, value in self.response.iteritems():
            if key == 'fields':
                setattr(self, key, [Field(v, 'FeatureLayerField') for v in value])
            else:
                setattr(self, key, value)

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
                     {"Utility_Type":2,"Five_Yr_Plan":"No","Rating":None,"Inspection_Date":1429885595000}}
        """
        add_url = self.url + '/addFeatures'
        params = {'features': json.dumps(features),
                  'gdbVersion': gdbVersion,
                  'rollbackOnFailure': str(rollbackOnFailure).lower(),
                  'f': 'json'}

        # update features
        result = EditResult(POST(add_url, params, token=self.token))
        result.summary()
        return result

    def updateFeatures(self, features, gdbVersion='', rollbackOnFailure=True):
        """add new features to feature service layer

        features -- features to be added (JSON)
        gdbVersion -- geodatabase version to apply edits
        rollbackOnFailure -- specify if the edits should be applied only if all submitted edits succeed
        """
        update_url = self.url + '/updateFeatures'
        params = {'features': json.dumps(features),
                  'gdbVersion': gdbVersion,
                  'rollbackOnFailure': rollbackOnFailure,
                  'f': 'json'}

        # update features
        result = EditResult(POST(update_url, params, token=self.token))
        result.summary()
        return result

    def deleteFeatures(self, oids='', where='', geometry='', geometryType='',
                       spatialRel='', inSR='', gdbVersion='', rollbackOnFailure=True):
        """deletes features based on list of OIDs

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
            oids = "1, 2, 4" # as string"""
        if not geometryType:
            geometryType = 'esriGeometryEnvelope'
        if not spatialRel:
            spatialRel = 'esriSpatialRelIntersects'
        del_url = self.url + '/deleteFeatures'
        if isinstance(oids, (list, tuple)):
            oids = ', '.join(map(str, oids))
        params = {'objectIds': oids,
                  'where': where,
                  'geometry': json.dumps(geometry),
                  'geometryType': geometryType,
                  'spatialRel': spatialRel,
                  'gdbVersion': gdbVersion,
                  'rollbackOnFailure': rollbackOnFailure,
                  'f': 'json'}

        # delete features
        result = EditResult(POST(del_url, params, token=self.token))
        result.summary()
        return result

    def applyEdits(self, adds='', updates='', deletes='', gdbVersion='', rollbackOnFailure=True):
        """apply edits on a feature service layer

        adds -- features to add (JSON)
        updates -- features to be updated (JSON)
        deletes -- oids to be deleted (list, tuple, or comma separated string)
        gdbVersion -- geodatabase version to apply edits
        rollbackOnFailure -- specify if the edits should be applied only if all submitted edits succeed
        """
    def addAttachment(self, oid, attachment, content_type=''):
        """add an attachment to a feature service layer

        Required:
            oid -- OBJECT ID of feature in which to add attachment
            attachment -- path to attachment

        Optional:
            content_type -- html media type for "content_type" header.  If nothing provided,
            will use a best guess based on file extension (using mimetypes)

            valid content types can be found here @:
                http://en.wikipedia.org/wiki/Internet_media_type
        """
        if self.hasAttachments:

            # use mimetypes to guess "content_type"
            if not content_type:
                import mimetypes
                known = mimetypes.types_map
                common = mimetypes.common_types
                ext = os.path.splitext(attachment)[-1]
                if ext in known:
                    content_type = known[ext]
                elif ext in common:
                    content_type = common[ext]

            # make post request
            att_url = '{}/{}/addAttachment'.format(self.url, oid)
            files = {'attachment': (os.path.basename(attachment), open(attachment, 'rb'), content_type)}
            params = {'token': self.token,'f': 'json'}
            r = requests.post(att_url, params, files=files).json()
            if 'addAttachmentResult' in r:
                print r['addAttachmentResult']
            return r

        else:
            raise NotImplementedError('FeatureLayer "{}" does not support attachments!'.format(self.name))

    def refresh(self):
        """refreshes the FeatureService"""
        self.__init__(self.url, token=self.token)

class BaseMapService(RESTEndpoint):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
        super(BaseMapService, self).__init__(url, usr, pw, token)

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

    def refresh(self):
        """refreshes the MapService"""
        self.__init__(self.url, token=self.token)

class BaseMapServiceLayer(RESTEndpoint):
    """Class to handle advanced layer properties"""
    def __init__(self, url='', usr='', pw='', token=''):
        super(BaseMapServiceLayer, self).__init__(url, usr, pw, token)

        for key, value in self.response.iteritems():
            setattr(self, key, value)

        validate(self, ['fields', 'spatialReference'])
        self.fields_dict = self.response['fields']
        if self.fields_dict:
            self.fields = [Field(f) for f in self.fields_dict]
        else:
            self.fields = []

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

class BaseImageService(RESTEndpoint):
    """Class to handle Image service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
        super(BaseImageService, self).__init__(url, usr, pw, token)

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

    def adjustbbox(self, boundingBox):
        """method to adjust bounding box for image clipping to maintain
        cell size.

        Required:
            boundingBox -- bounding box string (comma separated)
        """
        cell_size = int(self.pixelSizeX)
        if isinstance(boundingBox, basestring):
            boundingBox = boundingBox.split(',')
        return ','.join(map(str, map(lambda x: Round(x, cell_size), boundingBox)))

    def refresh(self):
        """refreshes the ImageService"""
        self.__init__(self.url, token=self.token)

class GeocodeService(RESTEndpoint):
    """class to handle Geocode Service"""
    def __init__(self, url, usr='', pw='', token=''):
        """Geocode Service object

        Required:
            url -- Geocode service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(GeocodeService, self).__init__(url, usr, pw, token)

        self.locators = []
        for key, value in self.response.iteritems():
            if key in ('addressFields',
                       'candidateFields',
                       'intersectionCandidateFields'):
                setattr(self, key, [Field(v, 'GeocodeField') for v in value])
            elif key == 'singleLineAddressField':
                setattr(self, key, Field(value, 'GeocodeField'))
            elif key == 'locators':
                for loc_dict in value:
                    self.locators.append(loc_dict['name'])
            else:
                setattr(self, key, value)

    def geocodeAddresses(self, recs, outSR=4326, address_field=''):
        """geocode a list of addresses.  If there is a singleLineAddress field present in the
        geocoding service, the only input required is a list of addresses.  Otherwise, a record
        set an be passed in for the "recs" parameter.  See formatting example at bottom.

        Required:
            recs -- JSON object for fields as record set if no SingleLine field available.
                If singleLineAddress is present a list of full addresses can be passed in.

        Optional:
            outSR -- output spatial refrence for geocoded addresses
            address_field -- name of address field or Single Line address field

        # recs param examples
        # preferred option as record set (from esri help docs):
        recs = {
            "records": [
                {
                    "attributes": {
                        "OBJECTID": 1,
                        "STREET": "440 Arguello Blvd",
                        "ZONE": "94118"
                    }
                },
           {
                    "attributes": {
                        "OBJECTID": 2,
                        "STREET": "450 Arguello Blvd",
                        "ZONE": "94118"
                    }
                }
            ]
        }

        # full address list option if singleLineAddressField is present
        recs = ['100 S Riverfront St, Mankato, MN 56001',..]
        """
        geo_url = self.url + '/geocodeAddresses'
        if isinstance(recs, (list, tuple)):
            addr_list = recs[:]
            recs = {"records": []}
            if not address_field:
                if hasattr(self, 'singleLineAddressField'):
                    address_field = self.singleLineAddressField.name
                else:
                    address_field = self.addressFields[0].name
                    print 'Warning, no singleLineAddressField found...Using "{}" field'.format(address_field)
            for i, addr in enumerate(addr_list):
                recs['records'].append({"attributes": {"OBJECTID": i+1,
                                                       address_field: addr}})

        # validate recs, make sure OBECTID is present
        elif isinstance(recs, dict) and 'records' in recs:
            for i, atts in enumerate(recs['records']):
                if not 'OBJECTID' in atts['attributes']:
                    atts['attributes']['OBJECTID'] = i + 1 #do not start at 0

        else:
            raise ValueError('Not a valid input for "recs" parameter!')

        params = {'addresses': json.dumps(recs),
                      'outSR': outSR,
                      'f': 'json'}

        return GeocodeResult(POST(geo_url, params, token=self.token), geo_url.split('/')[-1])

    def reverseGeocode(self, x, y, distance=100, outSR=4326, returnIntersection=False):
        """reverse geocodes an address by x, y coordinates

        Required:
            x -- longitude, x-coordinate
            y -- latitude, y-coordinate
            distance -- distance in meters from given location which a matching address will be found
            outSR -- wkid for output address
        """
        geo_url = self.url + '/reverseGeocode'
        params = {'location': '{},{}'.format(x,y),
                  'distance': distance,
                  'outSR': outSR,
                  'returnIntersection': str(returnIntersection).lower(),
                  'f': 'json'}

        return GeocodeResult(POST(geo_url, params, token=self.token), geo_url.split('/')[-1])

    def findAddressCandidates(self, address='', outSR=4326, outFields='*', returnIntersection=False, **kwargs):
        """finds address candidates for an anddress

        Required:
            address -- full address (380 New York Street, Redlands, CA 92373)
            outFields -- list of fields for output. Default is * for all fields.  Will
                accept either list of fields [], or comma separated string.
            outSR -- wkid for output address
            **kwargs -- key word arguments to use for Address, City, State, etc fields if no SingleLine field
        """
        geo_url = self.url + '/findAddressCandidates'
        params = {'outSR': outSR,
                  'outFields': outFields,
                  'returnIntersection': str(returnIntersection).lower(),
                  'f': 'json'}
        if address:
            if hasattr(self, 'singleLineAddressField'):
                params[self.singleLineAddressField.name] = address
            else:
                params[self.addressFields[0].name] = address
        if kwargs:
            for fld_name, fld_query in kwargs.iteritems():
                params[fld_name] = fld_query

        return GeocodeResult(POST(geo_url, params, token=self.token), geo_url.split('/')[-1])

class GPService(RESTEndpoint):
    """ Class to handle GP Service Object"""
    def __init__(self, url, usr='', pw='', token=''):
        """GP Service object

        Required:
            url -- GP service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(GPService, self).__init__(url, usr, pw, token)

        for key, value in self.response.iteritems():
            setattr(self, key, value)

    def task(self, name):
        """returns a GP Task object"""
        return GPTask('/'.join([self.url, name]), token=self.token)

class GPTask(RESTEndpoint):
    """class to handle GP Task"""
    def __init__(self, url, usr='', pw='', token=''):
        """GP Task object

        Required:
            url -- GP Task url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(GPTask, self).__init__(url, usr, pw, token)

        for key,value in self.response.iteritems():
            if key != 'parameters':
                setattr(self, key, value)

    @property
    def isSynchronous(self):
        return self.executionType == 'esriExecutionTypeSynchronous'

    @property
    def isAsynchronous(self):
        return self.executionType == 'esriExecutionTypeAsynchronous'

    @property
    def parameters(self):
        """returns list of GPParam objects"""
        return [GPParam(pd) for pd in self.response['parameters']]

    def list_parameters(self):
        """lists the parameter names"""
        return [p.name for p in self.parameters]

    def run(self, params_json={}, outSR='', processSR='', returnZ=False, returnM=False, **kwargs):
        """Runs a Syncrhonous/Asynchronous GP task, automatically uses appropriate option

        Required:
            task -- name of task to run
            params_json -- JSON object with {parameter_name: value, param2: value2, ...}

        Optional:
            outSR -- spatial reference for output geometries
            processSR -- spatial reference used for geometry opterations
            returnZ -- option to return Z values with data if applicable
            returnM -- option to return M values with data if applicable
            kwargs -- keyword arguments, can substitute this to pass in GP params by name instead of
                using the params_json dictionary.  Only valid if params_json dictionary is not supplied.
        """
        if self.isSynchronous:
            runType = 'execute'
        else:
            runType = 'submitJob'
        gp_exe_url = '/'.join([self.url, runType])
        if not params_json:
            params_json = {}
            for k,v in kwargs.iteritems():
                params_json[k] = v
        params_json['env:outSR'] = outSR
        params_json['env:processSR'] = processSR
        params_json['returnZ'] = returnZ
        params_json['returnM'] = returnZ
        params_json['f'] = 'json'
        params_json['token'] = self.token
        r = requests.post(gp_exe_url, params_json).json()

        # return result object
        return GPResult(r)
