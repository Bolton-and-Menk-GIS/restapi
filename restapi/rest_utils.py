"""Helper functions and base classes for restapi module"""
from __future__ import print_function
import requests
import fnmatch
import datetime
import collections
import mimetypes
import urllib
import tempfile
import time
import codecs
import json
import copy
import os
import sys
import munch
from itertools import izip_longest
from collections import namedtuple, OrderedDict
from requests.packages.urllib3.exceptions import InsecureRequestWarning, InsecurePlatformWarning, SNIMissingWarning
from ._strings import *

# python 3 compat
try:
    basestring
except NameError:
    basestring = str

# disable ssl warnings (we are not verifying SSL certificates at this time...future ehnancement?)
for warning in [SNIMissingWarning, InsecurePlatformWarning, InsecureRequestWarning]:
    requests.packages.urllib3.disable_warnings(warning)

class IdentityManager(object):
    """Identity Manager for secured services.  This will allow the user to only have
    to sign in once (until the token expires) when accessing a services directory or
    individual service on an ArcGIS Server Site"""
    def __init__(self):
        self.tokens = {}
        self.proxies = {}

    def findToken(self, url):
        """returns a token for a specific domain from token store if one has been
        generated for the ArcGIS Server resource

        Required:
            url -- url for secured resource
        """
        if self.tokens:
            if '/admin/' in url:
                url = url.split('/admin/')[0] + '/admin/services'
            else:
                url = url.lower().split('/rest/services')[0] + '/rest/services'
            if url in self.tokens:
                if not self.tokens[url].isExpired:
                    return self.tokens[url]
                else:
                    raise RuntimeError('Token expired at {}! Please sign in again.'.format(token.expires))

        return None

    def findProxy(self, url):
        """returns a proxy url for a specific domain from token store if one has been
        used to access the ArcGIS Server resource

        Required:
            url -- url for secured resource
        """
        if self.proxies:
            url = url.lower().split('/rest/services')[0] + '/rest/services'
            if url in self.proxies:
                return self.proxies[url]

        return None

# initialize Identity Manager
ID_MANAGER = IdentityManager()

# temp dir for json outputs
TEMP_DIR = os.environ['TEMP']
if not os.access(TEMP_DIR, os.W_OK| os.X_OK):
    TEMP_DIR = None

def namedTuple(name, pdict):
    """creates a named tuple from a dictionary

    Required:
        name -- name of namedtuple object
        pdict -- parameter dictionary that defines the properties
    """
    class obj(namedtuple(name, sorted(pdict.keys()))):
        """class to handle {}""".format(name)
        __slots__ = ()
        def __new__(cls,  **kwargs):
            return super(obj, cls).__new__(cls, **kwargs)

        def asJSON(self):
            """return object as JSON"""
            return {f: getattr(self, f) for f in self._fields}

    o = obj(**pdict)
    o.__class__.__name__ = name
    return o

def Round(x, base=5):
    """round to nearest n"""
    return int(base * round(float(x)/base))

def tmp_json_file():
    """returns a valid path for a temporary json file"""
    global TEMP_DIR
    if TEMP_DIR is None:
        TEMP_DIR = tempfile.mkdtemp()
    return os.path.join(TEMP_DIR, 'restapi_{}.json'.format(time.strftime('%Y%m%d%H%M%S')))

def do_post(service, params={F: JSON}, ret_json=True, token='', cookies=None, proxy=None):
    """Post Request to REST Endpoint through query string, to post
    request with data in body, use requests.post(url, data={k : v}).

    Required:
        service -- full path to REST endpoint of service

    Optional:
        params -- parameters for posting a request
        ret_json -- return the response as JSON.  Default is True.
        token -- token to handle security (only required if security is enabled)
        cookies -- cookie object {'agstoken': 'your_token'}
        proxy -- option to use proxy page to handle security, need to provide
            full path to proxy url.
    """
    global PROTOCOL
    if PROTOCOL != '':
        service = '{}://{}'.format(PROTOCOL, service.split('://')[-1])
    if not cookies and not proxy:
        if not token:
            token = ID_MANAGER.findToken(service)
        if token and isinstance(token, Token):# and token.domain.lower() in service.lower():
            if isinstance(token, Token) and token.isExpired:
                raise RuntimeError('Token expired at {}! Please sign in again.'.format(token.expires))
            if not token.isAGOL and not token.isAdmin:
                cookies = {AGS_TOKEN: str(token)}
            else:
                if TOKEN not in params:
                    params[TOKEN] = str(token)
        elif token:
            if not token.isAGOL and not token.isAdmin:
                cookies = {AGS_TOKEN: str(token)}
            else:
                if TOKEN not in params:
                    params[TOKEN] = str(token)

    # auto fill in geometry params if a restapi.Geometry object is passed in (derived from BaseGeometry)
    if params.get(GEOMETRY) and isinstance(params.get(GEOMETRY), BaseGeometry):
        geometry = params.get(GEOMETRY)
        if not GEOMETRY_TYPE in params and hasattr(geometry, GEOMETRY_TYPE):
            params[GEOMETRY_TYPE] = getattr(geometry, GEOMETRY_TYPE)
        if not IN_SR in params:
            params[IN_SR] = geometry.getWKID() or geometry.getWKT()

    for pName, p in params.iteritems():
        if isinstance(p, dict) or hasattr(p, 'json'):
            params[pName] = json.dumps(p, cls=RestapiEncoder)

    if not F in params:
        params[F] = JSON

    if not token and not proxy:
        proxy = ID_MANAGER.findProxy(service)

    if token:
        if isinstance(token, Token):
            if token.isAGOL or token.isAdmin:
                params[TOKEN] = str(token)
                cookies = None

    if proxy:
        r = do_proxy_request(proxy, service, params)
        ID_MANAGER.proxies[service.split('/rest')[0].lower() + '/rest/services'] = proxy
    else:
        r = requests.post(service, params, headers={'User-Agent': USER_AGENT}, cookies=cookies, verify=False)

    # make sure return
    if r.status_code != 200:
        raise NameError('"{0}" service not found!\n{1}'.format(service, r.raise_for_status()))
    else:
        if ret_json is True:
            _json = r.json()
            RequestError(_json)
            return munch.munchify(_json)
        else:
            return r

def do_proxy_request(proxy, url, params={}):
    """make request against ArcGIS service through a proxy.  This is designed for a
    proxy page that stores access credentials in the configuration to handle authentication.
    It is also assumed that the proxy is a standard Esri proxy, i.e. retrieved from their
    repo on GitHub @:

        https://github.com/Esri/resource-proxy

    Required:
        proxy -- full url to proxy
        url -- service url to make request against
    Optional:
        params -- query parameters, user is responsible for passing in the
            proper parameters
    """
    frmat = params.get(F, JSON)
    if F in params:
        del params[F]

    p = '&'.join('{}={}'.format(k,v) for k,v in params.iteritems())

    # probably a better way to do this...
    return requests.post('{}?{}?f={}&{}'.format(proxy, url, frmat, p).rstrip('&'), verify=False, headers={'User-Agent': USER_AGENT})

def guess_proxy_url(domain):
    """grade school level hack to see if there is a standard esri proxy available for a domain

    Required:
        domain -- url to domain to check for proxy
    """
    domain = domain.lower().split('/arcgis')[0]
    if not domain.startswith('http'):
        domain = 'http://' + domain
    types = ['.ashx', '.jsp', '.php']
    for ptype in types:
        proxy_url = '/'.join([domain, 'proxy' + ptype])
        r = requests.get(proxy_url)
        # should produce an error in JSON if using esri proxy out of the box
        try:
            if r.status_code == 400 or 'error' in r.json():
                return r.url
        except:
            pass

    # try again looking to see if it is in a folder called "proxy"
    for ptype in types:
        proxy_url = '/'.join([domain, PROXY, PROXY + ptype])
        r = requests.get(proxy_url)
        try:
            if r.status_code == 400 or r.content:
                return r.url
        except:
            pass
    return None

def validate_name(file_name):
    """validates an output name by removing special characters"""
    import string
    path = os.sep.join(file_name.split(os.sep)[:-1]) #forward slash in name messes up os.path.split()
    name = file_name.split(os.sep)[-1]
    root, ext = os.path.splitext(name)
    d = {s: '_' for s in string.punctuation}
    for f,r in d.iteritems():
        root = root.replace(f,r)
    return os.path.join(path, '_'.join(root.split()) + ext)

def guess_wkid(wkt):
    """attempts to guess a well-known ID from a well-known text imput (WKT)

    Required:
        wkt -- well known text spatial reference
    """
    if wkt in PRJ_STRINGS:
        return PRJ_STRINGS[wkt]
    if 'PROJCS' in wkt:
        name = wkt.split('PROJCS["')[1].split('"')[0]
    elif 'GEOGCS' in wkt:
        name = wkt.split('GEOGCS["')[1].split('"')[0]
    if name in PRJ_NAMES:
        return PRJ_NAMES[name]
    return 0


def assign_unique_name(fl):
    """assigns a unique file name

    Required:
        fl -- file name
    """
    if not os.path.exists(fl):
        return fl

    i = 1
    head, tail = os.path.splitext(fl)
    new_name = '{}_{}{}'.format(head, i, tail)
    while os.path.exists(new_name):
        i += 1
        new_name = '{}_{}{}'.format(head, i, tail)
    return new_name

def mil_to_date(mil):
    """date items from REST services are reported in milliseconds,
    this function will convert milliseconds to datetime objects

    Required:
        mil -- time in milliseconds
    """
    if isinstance(mil, basestring):
        mil = long(mil)
    if mil == None:
        return None
    elif mil < 0:
        return datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(seconds=(mil/1000))
    else:
        # safely cast, to avoid being out of range for platform local time
        try:
            struct = time.gmtime(mil /1000.0)
            return datetime.datetime.fromtimestamp(time.mktime(struct))
        except Exception as e:
            print(mil)
            raise e

def date_to_mil(date=None):
    """converts datetime.datetime() object to milliseconds

    date -- datetime.datetime() object"""
    if isinstance(date, datetime.datetime):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return long((date - epoch).total_seconds() * 1000.0)

def generate_token(url, user, pw, expiration=60):
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
    suffix = '/rest/info'
    isAdmin = False
    if '/admin/' in url:
        isAdmin = True
        if '/rest/admin/' in url:
            infoUrl = url.split('/rest/')[0] + suffix
        else:
            infoUrl = url.split('/admin/')[0] + suffix
    else:
        infoUrl = url.split('/rest/')[0] + suffix
    infoResp = do_post(infoUrl)
    is_agol = False
    if AUTH_INFO in infoResp and TOKEN_SERVICES_URL in infoResp[AUTH_INFO]:
        base = infoResp[AUTH_INFO][TOKEN_SERVICES_URL]
        is_agol = AGOL_BASE in base
        if is_agol:
            base = AGOL_TOKEN_SERVICE

        global PROTOCOL
        PROTOCOL =  base.split('://')[0]
        print('set PROTOCOL to "{}" from generate token'.format(PROTOCOL))
        try:
            shortLived = infoResp[AUTH_INFO][SHORT_LIVED_TOKEN_VALIDITY]
        except KeyError:
            shortLived = 100
    else:
        base = url.split('/rest/')[0] + '/tokens'
        shortLived = 100

    params = {F: JSON,
              USER_NAME: user,
              PASSWORD: pw,
              CLIENT: REQUEST_IP,
              EXPIRATION: max([expiration, shortLived])}

    if is_agol:
        params[REFERER] = AGOL_BASE
        del params[CLIENT]

    resp = do_post(base, params)
    if is_agol:
        # now call portal sharing
        portal_params = {TOKEN: resp.get(TOKEN)}
        org_resp = do_post(AGOL_PORTAL_SELF,portal_params)
        org_referer = org_resp.get(URL_KEY) + ORG_MAPS
        params[REFERER]= org_referer
        resp = do_post(AGOL_TOKEN_SERVICE, params)

    if '/services/' in url:
        resp[DOMAIN] = url.split('/services/')[0] + '/services'
    elif '/admin/' in url:
        resp[DOMAIN] = url.split('/admin/')[0] + '/admin'
    else:
        resp[DOMAIN] = url
    resp[IS_AGOL] = is_agol
    resp[IS_ADMIN] = isAdmin
    token = Token(resp)
    ID_MANAGER.tokens[token.domain] = token
    return token

class RestapiEncoder(json.JSONEncoder):
    """encoder for restapi objects to make serializeable for JSON"""
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return date_to_mil(o)
        if hasattr(o, JSON):
            return getattr(o, JSON)
        elif isinstance(o, (dict, list)):
            return o
        try:
            return o.__dict__
        except:
            return {}

class JsonGetter(object):
    """override getters to also check its json property"""
    json = {}

    def get(self, name, default=None):
        """gets an attribute from json"""
        return self.json.get(name, default)

    def dump(self, out_json_file, indent=2, **kwargs):
        """dump as JSON file"""
        if hasattr(out_json_file, 'read'):
            json.dump(self.json, out_json_file, indent=indent, **kwargs)
        elif isinstance(out_json_file, basestring):
            head, tail = os.path.splitext(out_json_file)
            if not tail == '.json':
                out_json_file = head + '.json'
            with open(out_json_file, 'w') as f:
                json.dump(self.json, f, indent=indent, **kwargs)
        return out_json_file

    def dumps(self):
        """dump as string"""
        return json.dumps(self.json)

    def __getitem__(self, name):
        """dict like access to json definition"""
        if name in self.json:
            return self.json[name]

    def __getattr__(self, name):
        """get normal class attributes and those from json response"""
        try:
            # it is a class attribute
            return object.__getattribute__(self, name)
        except AttributeError:
            # it is in the json definition, abstract it to the class level
            if name in self.json:
                return self.json[name]
            else:
                raise AttributeError(name)

    def __str__(self):
        return json.dumps(self.json, sort_keys=True, indent=2, ensure_ascii=False)

class RESTEndpoint(JsonGetter):
    """Base REST Endpoint Object to handle credentials and get JSON response

    Required:
        url -- service url

    Optional (below params only required if security is enabled):
        usr -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
        token -- token to handle security (alternative to usr and pw)
        proxy -- option to use proxy page to handle security, need to provide
            full path to proxy url.
    """
    url = None
    raw_response = None
    response = None
    token = None
    elapsed = None
    json = {}
    _cookie = None
    _proxy = None

    def __init__(self, url, usr='', pw='', token='', proxy=None):

        if PROTOCOL:
            self.url = PROTOCOL + '://' + url.split('://')[-1].rstrip('/') if not url.startswith(PROTOCOL) else url.rstrip('/')
        else:
            self.url = 'http://' + url.rstrip('/') if not url.startswith('http') else url.rstrip('/')
        if not fnmatch.fnmatch(self.url, BASE_PATTERN):
            _plus_services = self.url + '/arcgis/rest/services'
            if fnmatch.fnmatch(_plus_services, BASE_PATTERN):
                self.url = _plus_services
            else:
                RequestError({'error':{'URL Error': '"{}" is an invalid ArcGIS REST Endpoint!'.format(self.url)}})
        params = {F: JSON}
        self.token = token
        self._cookie = None
        self._proxy = proxy
        if not self.token and not self._proxy:
            if usr and pw:
                self.token = generate_token(self.url, usr, pw)
            else:
                self.token = ID_MANAGER.findToken(self.url)
                if isinstance(self.token, Token) and self.token.isExpired:
                    raise RuntimeError('Token expired at {}! Please sign in again.'.format(self.token.expires))
                elif isinstance(self.token, Token) and not self.token.isExpired:
                    pass
                else:
                    self.token = None
        else:
            if isinstance(self.token, Token) and self.token.isExpired and self.token.domain in self.url.lower():
                raise RuntimeError('Token expired at {}! Please sign in again.'.format(self.token.expires))

        if self.token:
            if isinstance(self.token, Token) and self.token.domain.lower() in url.lower():
                self._cookie = self.token._cookie
            else:
                self._cookie = {AGS_TOKEN: self.token.token if isinstance(self.token, Token) else self.token}
        if (not self.token or not self._cookie) and not self._proxy:
            if self.url in ID_MANAGER.proxies:
                self._proxy = ID_MANAGER.proxies[self.url]

        self.raw_response = do_post(self.url, params, ret_json=False, token=self.token, cookies=self._cookie, proxy=self._proxy)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        self.json = munch.munchify(self.response)
        RequestError(self.json)

    def compatible_with_version(self, version):
        """checks if ArcGIS Server version is compatible with input version.  A
        service is compatible with the version if it is greater than or equal to
        the input version

        Required:
            version -- minimum version compatibility as float (ex: 10.3 or 10.31)
        """
        def validate_version(ver):
            if isinstance(ver, (float, int)):
                return ver
            elif isinstance(ver, basestring):
                try:
                    ver = float(ver)
                except:
                    # we want an exception here if it does not match the format
                    whole, dec = ver.split('.')
                    ver = float('.'.join([whole, ''.join([i for i in dec if i.isdigit()])]))
        try:
            return validate_version(self.currentVersion) >= validate_version(version)
        except AttributeError:
            return False

    def refresh(self):
        """refreshes the service"""
        self.__init__(self.url, token=self.token)

    @classmethod
    def __get_cls(cls):
        return cls

    def __dir__(self):
        atts = []
        bases = self.__get_cls().__bases__
        while bases:
            for base in bases:
                atts.extend(base.__dict__.keys())
                bases = base.__bases__
        return sorted(list(set(self.__class__.__dict__.keys() + self.json.keys() + atts)))

class SpatialReferenceMixin(object):
    """mixin to allow convenience methods for grabbing the spatial reference from a service"""
    json = {}

    @property
    def _spatialReference(self):
        """gets the spatial reference dict"""
        resp_d = {}
        if SPATIAL_REFERENCE in self.json:
            resp_d = self.json[SPATIAL_REFERENCE]
        elif EXTENT in self.json and SPATIAL_REFERENCE in self.json[EXTENT]:
            resp_d = self.json[EXTENT][SPATIAL_REFERENCE]
        return munch.munchify(resp_d)

    def getSR(self):
        """return the spatial reference"""
        resp_d = self._spatialReference
        for key in [LATEST_WKID, WKID, WKT]:
            if key in resp_d:
                return resp_d[key]

    def getWKID(self):
        """returns the well known id for service spatial reference"""
        resp_d = self._spatialReference
        for key in [LATEST_WKID, WKID]:
            if key in resp_d:
                return resp_d[key]

    def getWKT(self):
        """returns the well known text (if it exists) for a service"""
        return self._spatialReference.get(WKT, '')

class BaseService(RESTEndpoint, SpatialReferenceMixin):
    """base class for all services"""
    def __init__(self, url, usr='', pw='', token='', proxy=None):
        super(BaseService, self).__init__(url, usr, pw, token, proxy)
        if NAME not in self.json:
            self.name = self.url.split('/')[-2]
        self.name = self.name.split('/')[-1]

    def __repr__(self):
        """string representation with service name"""
        try:
            qualified_name = '/'.join(filter(None, [self.url.split('/services/')[-1].split('/' + self.name)[0], self.name]))
        except:
            qualified_name = self.name
        return '<{}: {}>'.format(self.__class__.__name__, qualified_name)

class Feature(JsonGetter):
    def __init__(self, feature):
        """represents a single feature

        Required:
            feature -- input json for feature
        """
        self.json = munch.munchify(feature)
        self.geometry = self.json.get(GEOMETRY)

    def get(self, field):
        """gets an attribute from the feature

        Required:
            field -- name of field for which to get attribute
        """
        return self.json[ATTRIBUTES].get(field)

    def __repr__(self):
        return str(self)

class RelatedRecords(JsonGetter, SpatialReferenceMixin):
    def __init__(self, in_json):
        """related records response

        Required:
            in_json -- json response for query related records operation
        """
        self.json = munch.munchify(in_json)
        self.geometryType = self.json.get(GEOMETRY_TYPE)
        self.spatialReference = self.json.get(SPATIAL_REFERENCE)

    def list_related_OIDs(self):
        """returns a list of all related object IDs"""
        return [f.get('objectId') for f in iter(self)]

    def get_related_records(self, oid):
        """gets the related records for an object id

        Required:
            oid -- object ID for related records
        """
        for group in iter(self):
            if oid == group.get('objectId'):
                return [Feature(f) for f in group[RELATED_RECORDS]]

    def __iter__(self):
        for group in self.json[RELATED_RECORD_GROUPS]:
            yield group

class FeatureSet(JsonGetter, SpatialReferenceMixin):

    def __init__(self, in_json):
        """class to handle feature set

        Required:
            in_json -- input json response from request
        """
        super(FeatureSet, self).__init__()
        if isinstance(in_json, basestring):
            in_json = json.loads(in_json)
        elif isinstance(in_json, self.__class__):
            self.json = in_json.json
        else:
            self.json = munch.munchify(in_json)
        if not all([self.json.get(k) for k in (FIELDS, FEATURES)]):
            raise ValueError('Not a valid Feature Set!')

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
    def hasGeometry(self):
        """boolean for if it has geometry"""
        if self.count:
            if self.features[0].get(GEOMETRY):
                return True
        return False

    @property
    def count(self):
        """returns total number of records in Cursor (user queried)"""
        return len(self)

    def list_fields(self):
        """returns a list of field names"""
        return [f.name for f in self.fields]

    def __getattr__(self, name):
        """get normal class attributes and those from json response"""
        try:
            # it is a class attribute
            return object.__getattribute__(self, name)
        except AttributeError:
            # it is in the json definition, abstract it to the class level
            if name in self.json:
                return self.json[name]
            else:
                raise AttributeError(name)

    def __getitem__(self, key):
        """supports grabbing feature by index and json keys by name"""
        if isinstance(key, int):
            return Feature(self.json.features[key])
        else:
            return Feature(self.json.get(key))

    def __iter__(self):
        for feature in self.features:
            yield Feature(feature)

    def __len__(self):
        return len(self.features)

    def __bool__(self):
        return bool(len(self))

    def __dir__(self):
        return sorted(self.__class__.__dict__.keys() + self.json.keys())

class OrderedDict2(OrderedDict):
    """wrapper for OrderedDict"""
    def __init__(self, *args, **kwargs):
        super(OrderedDict2, self).__init__(*args, **kwargs)

    def __repr__(self):
        """we want it to look like a dictionary"""
        return json.dumps(self, indent=2, ensure_ascii=False)

class Token(JsonGetter):
    """class to handle token authentication"""
    def __init__(self, response):
        """response JSON object from generate_token"""
        self.json = munch.munchify(response)
        self._cookie = {AGS_TOKEN: self.token}
        self.isAGOL = self.json.get(IS_AGOL, False)
        self.isAdmin = self.json.get(IS_ADMIN, False)

    @property
    def time_expires(self):
        return mil_to_date(self.expires)

    @property
    def isExpired(self):
        """boolean value for expired or not"""
        if datetime.datetime.now() > self.time_expires:
            return True
        else:
            return False

    def __str__(self):
        """return token as string representation"""
        return self.token

class RequestError(object):
    """class to handle restapi request errors"""
    def __init__(self, err):
        if 'error' in err:
            raise RuntimeError(json.dumps(err, indent=2))

class Folder(RESTEndpoint):
    """class to handle ArcGIS REST Folder"""

    @property
    def name(self):
        """returns the folder name"""
        return self.url.split('/')[-1]

    def list_services(self):
        """method to list services"""
        return ['/'.join([s.name, s.type]) for s in self.services]

    def __len__(self):
        """return number of services in folder"""
        return len(self.services)

    def __bool__(self):
        """return True if services are present"""
        return bool(len(self))

class GPResult(object):
    """class to handle GP Result"""
    def __init__(self, response):
        """handler for GP result

        res_dict -- JSON response from GP Task execution
        """
        self.response = response
        RequestError(self.response)

    @property
    def results(self):
        if RESULTS in self.response:
           return [namedTuple('Result', r) for r in self.response[RESULTS]]
        return []

    @property
    def value(self):
        """returns a value (if any) from results"""
        if VALUE in self.response:
            return self.response[VALUE]
        return None

    @property
    def messages(self):
        """return messages as JSON"""
        if 'messages' in self.response:
            return [namedTuple('Message', d) for d in self.response['messages']]
        return []

    def print_messages(self):
        """prints all the GP messages"""
        for msg in self.messages:
            print('Message Type: {}'.format(msg.type))
            print('\tDescription: {}\n'.format(msg.description))

    def __len__(self):
        """return length of results"""
        return len(self.results)

    def __getitem__(self, i):
        """return result at index, usually will only be 1"""
        return self.results[i]

    def __bool__(self):
        """return True if results"""
        return bool(len(self))

class GeocodeResult(object):
    """class to handle Reverse Geocode Result"""
    __slots__ = [RESPONSE, SPATIAL_REFERENCE, TYPE, CANDIDATES,
                LOCATIONS, ADDRESS, RESULTS, 'result', 'Result']

    def __init__(self, res_dict, geo_type):
        """geocode response object

        Required:
            res_dict -- JSON response from geocode request
            geo_type -- type of geocode operation (reverseGeocode|findAddressCandidates|geocodeAddresses)
        """
        RequestError(res_dict)
        self.response = res_dict
        self.type = 'esri_' + geo_type
        self.candidates = []
        self.locations = []
        self.address = []
        try:
            sr_dict = self.response[LOCATION][SPATIAL_REFERENCE]
            wkid = sr_dict.get(LATEST_WKID, None)
            if wkid is None:
                wkid = sr_dict.get(WKID, None)
            self.spatialReference = wkid
        except:
            self.spatialReference = None

        if self.type == 'esri_reverseGeocode':
            addr_dict = {}
            addr_dict[LOCATION] = self.response[LOCATION]
            addr_dict[ATTRIBUTES] = self.response[ADDRESS]
            address = self.response[ADDRESS].get('Address', None)
            if address is None:
                add = self.response[ADDRESS]
                addr_dict[ADDRESS] = ' '.join(filter(None, [add.get('Street'), add.get('City'), add.get('ZIP')]))
            else:
                addr_dict[ADDRESS] = address
            addr_dict[SCORE] = None
            self.address.append(addr_dict)

        # legacy response from find? <- deprecated?
        # http://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find #still works
        elif self.type == 'esri_find':
            # format legacy results
            for res in self.response[LOCATIONS]:
                ref_dict = {}
                for k,v in res.iteritems():
                    if k == NAME:
                        ref_dict[ADDRESS] = v
                    elif k == FEATURE:
                        atts_dict = {}
                        for att, val in res[k].iteritems():
                            if att == GEOMETRY:
                                ref_dict[LOCATION] = val
                            elif att == ATTRIBUTES:
                                for att2, val2 in res[k][att].iteritems():
                                    if att2.lower() == SCORE:
                                        ref_dict[SCORE] = val2
                                    else:
                                        atts_dict[att2] = val2
                            ref_dict[ATTRIBUTES] = atts_dict
                self.locations.append(ref_dict)

        else:
            if self.type == 'esri_findAddressCandidates':
                self.candidates = self.response[CANDIDATES]

            elif self.type == 'esri_geocodeAddresses':
                self.locations = self.response[LOCATIONS]

        defaults = 'address attributes location score'
        self.Result = collections.namedtuple('GeocodeResult_result', defaults)

    @property
    def results(self):
        """returns list of result objects"""
        gc_results = self.address + self.candidates + self.locations
        results = []
        for res in gc_results:
            results.append(self.Result(*[v for k,v in sorted(res.items())]))
        return results

    @property
    def result(self):
        """returns the top result"""
        try:
            return self.results[0]
        except IndexError:
            return None

    def __getitem__(self, index):
        """allows for indexing of results"""
        return self.results[index]

    def __len__(self):
        """get count of results"""
        return len(self.results)

    def __iter__(self):
        """return an iterator for results (as generator)"""
        for r in self.results:
            yield r

    def __bool__(self):
        """returns True if results are returned"""
        return bool(len(self))

class EditResult(object):
    """class to handle Edit operation results"""
    __slots__ = [ADD_RESULTS, UPDATE_RESULTS, DELETE_RESULTS, ADD_ATTACHMENT_RESULT,
                SUMMARY, AFFECTED_OIDS, FAILED_OIDS, RESPONSE, JSON]
    def __init__(self, res_dict, feature_id=None):
        RequestError(res_dict)
        self.response = munch.munchify(res_dict)
        self.failedOIDs = []
        self.addResults = []
        self.updateResults = []
        self.deleteResults = []
        self.addAttachmentResult = {}
        for key, value in res_dict.iteritems():
            if isinstance(value, dict):
                value = [value]
            for v in value:
                res_id = v.get(RESULT_OBJECT_ID)
                if res_id is None:
                    res_id = v.get(RESULT_GLOBAL_ID)
                if v[SUCCESS_STATUS] in (True, TRUE):
                    if key == ADD_ATTACHMENT_RESULT:
                        self.addAttachmentResult[feature_id] = res_id
                    else:
                        getattr(self, key).append(res_id)
                else:
                    self.failedOIDs.append(res_id)
        self.affectedOIDs = self.addResults + self.updateResults + self.deleteResults + self.addAttachmentResult.keys()
        self.json = munch.munchify(res_dict)

    def summary(self):
        """print summary of edit operation"""
        if self.affectedOIDs:
            if self.addResults:
                print('Added {} feature(s)'.format(len(self.addResults)))
            if self.updateResults:
                print('Updated {} feature(s)'.format(len(self.updateResults)))
            if self.deleteResults:
                print('Deleted {} feature(s)'.format(len(self.deleteResults)))
            if self.addAttachmentResult:
                try:
                    k,v = self.addAttachmentResult.items()[0]
                    print("Added attachment '{}' for feature {}".format(v, k))
                except IndexError: # should never happen?
                    print('Added 1 attachment')
        if self.failedOIDs:
            print('Failed to edit {0} feature(s)!\n{1}'.format(len(self.failedOIDs), self.failedOIDs))

    def __len__(self):
        """return count of affected OIDs"""
        return len(self.affectedOIDs)

class BaseGeometry(SpatialReferenceMixin):
    """base geometry obect"""

    def dumps(self):
        """retuns JSON as a string"""
        return json.dumps(self.json)

class BaseGeometryCollection(object):
    """Base Geometry Collection"""
    geometries = []
    json = {GEOMETRIES: []}
    geometryType = None

    @property
    def count(self):
        return len(self)

    def dumps(self):
        """retuns JSON as a string"""
        return json.dumps(self.json)

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
        return '<restapi.GeometryCollection ({}): [{}]>'.format(self.count, self.geometryType)

class GeocodeService(RESTEndpoint):
    """class to handle Geocode Service"""

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
            recs = {RECORDS: []}
            if not address_field:
                if hasattr(self, 'singleLineAddressField'):
                    address_field = self.singleLineAddressField.name
                else:
                    address_field = self.addressFields[0].name
                    print('Warning, no singleLineAddressField found...Using "{}" field'.format(address_field))
            for i, addr in enumerate(addr_list):
                recs[RECORDS].append({ATTRIBUTES: {"OBJECTID": i+1,
                                                       address_field: addr}})

        # validate recs, make sure OBECTID is present
        elif isinstance(recs, dict) and RECORDS in recs:
            for i, atts in enumerate(recs[RECORDS]):
                if not OBJECTID in atts[ATTRIBUTES]:
                    atts[ATTRIBUTES][OBJECTID] = i + 1 #do not start at 0

        else:
            raise ValueError('Not a valid input for "recs" parameter!')

        params = {ADDRESSES: json.dumps(recs),
                      OUT_SR: outSR,
                      F: JSON}

        return GeocodeResult(do_post(geo_url, params, token=self.token, cookies=self._cookie), geo_url.split('/')[-1])

    def reverseGeocode(self, location, distance=100, outSR=4326, returnIntersection=False, langCode='eng'):
        """reverse geocodes an address by x, y coordinates

        Required:
            location -- input point object as JSON
            distance -- distance in meters from given location which a matching address will be found
            outSR -- wkid for output address

        Optional:
            langCode -- optional language code, default is eng (only used for StreMap Premium locators)
        """
        geo_url = self.url + '/reverseGeocode'
        params = {LOCATION: location,
                  DISTANCE: distance,
                  OUT_SR: outSR,
                  RETURN_INTERSECTION: returnIntersection,
                  F: JSON}

        return GeocodeResult(do_post(geo_url, params, token=self.token, cookies=self._cookie), geo_url.split('/')[-1])

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
        params = {OUT_SR: outSR,
                  OUT_FIELDS: outFields,
                  RETURN_INTERSECTION: returnIntersection,
                  F: JSON}
        if address:
            if hasattr(self, 'singleLineAddressField'):
                params[self.singleLineAddressField.name] = address
            else:
                params[self.addressFields[0].name] = address
        if kwargs:
            for fld_name, fld_query in kwargs.iteritems():
                params[fld_name] = fld_query

        return GeocodeResult(do_post(geo_url, params, token=self.token, cookies=self._cookie), geo_url.split('/')[-1])

    def __repr__(self):
        """string representation with service name"""
        return '<GeocodeService: {}>'.format('/'.join(self.url.split('/services/')[-1].split('/')[:-1]))