"""Helper functions and base classes for restapi module"""
from __future__ import print_function
import requests
import fnmatch
import datetime
import collections
import mimetypes
import tempfile
import time
import codecs
import json
import copy
import os
import sys
import munch
from collections import namedtuple, OrderedDict
from ._strings import *

from . import six
from .six.moves import urllib

# disable ssl warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning, InsecurePlatformWarning, SNIMissingWarning
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
                url = url.split('/admin/')[0] + '/admin'
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
TEMP_DIR = tempfile.mkdtemp('', 'restapi__')
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

def iter_chunks(iterable, n):
    """iterate an array in chunks"""
    args = [iter(iterable)] * n
    for group in six.moves.zip_longest(*args, fillvalue=None):
        yield filter(None, group)

def tmp_json_file():
    """returns a valid path for a temporary json file"""
    global TEMP_DIR
    if TEMP_DIR is None:
        TEMP_DIR = tempfile.mkdtemp()
    return os.path.join(TEMP_DIR, 'restapi_{}.json'.format(time.strftime('%Y%m%d%H%M%S')))

def do_post(service, params={F: JSON}, ret_json=True, token='', cookies=None, proxy=None, referer=None):
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
            if isinstance(token, Token) and (not token.isAGOL and not token.isAdmin):
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

    for pName, p in six.iteritems(params):
        if isinstance(p, dict) or hasattr(p, 'json'):
            params[pName] = json.dumps(p, ensure_ascii=False, cls=RestapiEncoder)

    if F not in params:
        params[F] = JSON

    if not token and not proxy:
        proxy = ID_MANAGER.findProxy(service)

    if token:
        if isinstance(token, Token):
            if token.isAGOL or token.isAdmin:
                params[TOKEN] = str(token)
                cookies = None

    if proxy:
        r = do_proxy_request(proxy, service, params, referer)
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


def do_proxy_request(proxy, url, params={}, referer=None):
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

    #p = '&'.join('{}={}'.format(k,v) for k,v in six.iteritems(params))

    # probably a better way to do this...
    headers = {'User-Agent': USER_AGENT}
    if referer:
        headers[REFERER_HEADER] = referer
    #return requests.post('{}?{}?f={}&{}'.format(proxy, url, frmat, p).rstrip('&'), verify=False, headers=headers)
    return requests.post('{}?{}?f={}'.format(proxy, url, frmat).rstrip('&'), params, verify=False, headers=headers)

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
    name = fix_encoding(file_name.split(os.sep)[-1])
    root, ext = os.path.splitext(name)
    d = {s: '_' for s in string.punctuation}
    for f,r in six.iteritems(d):
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
    if isinstance(mil, six.string_types):
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

def fix_encoding(s):
    """fixes unicode by treating as ascii and ignoring errors"""
    if isinstance(s, six.string_types):
        return s.encode('ascii', 'ignore').decode('ascii')
    return s

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
            shortLived = infoResp.get(AUTH_INFO, {}).get(SHORT_LIVED_TOKEN_VALIDITY)
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
        org_resp = do_post(AGOL_PORTAL_SELF, portal_params)
        org_referer = org_resp.get(URL_KEY, '') + ORG_MAPS
        params[REFERER]= org_referer
        resp = do_post(AGOL_TOKEN_SERVICE, params)
        resp['_' + PORTAL_INFO] = org_resp
    else:
        resp['_' + PORTAL_INFO] = {}


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
        if o == True:
            return TRUE
        if o == False:
            return FALSE
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

class NameEncoder(json.JSONEncoder):
    """encoder for restapi objects to make serializeable for JSON"""
    def default(self, o):
        return o.__repr__()

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
        elif isinstance(out_json_file, six.string_types):
            head, tail = os.path.splitext(out_json_file)
            if not tail == '.json':
                out_json_file = head + '.json'
            with open(out_json_file, 'w') as f:
                if not 'cls' in kwargs:
                    kwargs['cls'] = RestapiEncoder
                json.dump(self.json, f, indent=indent, ensure_ascii=False, **kwargs)
        return out_json_file

    def dumps(self, **kwargs):
        """dump as string"""
        if not 'cls' in kwargs:
            kwargs['cls'] = RestapiEncoder
        kwargs['ensure_ascii'] = False
        return json.dumps(self.json, **kwargs)

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
        return json.dumps(self.json, sort_keys=True, indent=2, cls=RestapiEncoder, ensure_ascii=False)

    def __repr__(self):
        return json.dumps(self.json, sort_keys=True, indent=2, cls=NameEncoder, ensure_ascii=False)

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
    _referer = None

    def __init__(self, url, usr='', pw='', token='', proxy=None, referer=None, **kwargs):

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
        params = {F: PJSON}
        for k,v in six.iteritems(kwargs):
            params[k] = v
        self.token = token
        self._cookie = None
        self._proxy = proxy
        self._referer = referer
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

        self.raw_response = do_post(self.url, params, ret_json=False, token=self.token, cookies=self._cookie, proxy=self._proxy, referer=self._referer)
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
            elif isinstance(ver, six.string_types):
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

    def request(self, *args, **kwargs):
        """wrapper for request to automatically pass in credentials"""
        for key, value in six.iteritems({'token': 'token',
            'cookies': '_cookie',
            'proxy': '_proxy',
            'referer': '_referer'
        }):
            if key not in kwargs:
                kwargs[key] = getattr(self, value)
        return do_post(*args, **kwargs)

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
        return sorted(list(set(list(self.__class__.__dict__.keys()) + list(self.json.keys()) + atts)))

    def __repr__(self):
        return '<{}>'.format(self.__class__.__name__)

class SpatialReferenceMixin(object):
    """mixin to allow convenience methods for grabbing the spatial reference from a service"""
    json = {}

    @classmethod
    def _find_wkid(cls, in_json):
        """recursivly search for WKID in a dict/json structure"""
        if isinstance(in_json, six.integer_types):
            return in_json
        if isinstance(in_json, six.string_types):
            try:
                in_json = json.loads(in_json)
                if not isinstance(json, dict):
                    return None
            except:
                return None
        if isinstance(in_json, list):
            try:
                return cls._find_wkid(in_json[0])
            except IndexError:
                return None
        if not isinstance(in_json, dict):
            return None
        for k, v in six.iteritems(in_json):
            if k == SPATIAL_REFERENCE:
                if isinstance(v, int):
                    return v
                elif isinstance(v, dict):
                    return cls._find_wkid(v)
            elif k == LATEST_WKID:
                return v
            elif k == WKID:
                return v
        if hasattr(in_json, 'factoryCode'):
            return getattr(in_json, 'factoryCode')

    @property
    def spatialReference(self):
        return self.getWKID()

    @spatialReference.setter
    def spatialReference(self, wkid):
        if isinstance(wkid, int):
            self.json[SPATIAL_REFERENCE] = {WKID: wkid}
        elif isinstance(wkid, dict):
            self.json[SPATIAL_REFERENCE] = wkid

    @property
    def _spatialReference(self):
        """gets the spatial reference dict"""
        resp_d = {}
        if SPATIAL_REFERENCE in self.json:
            resp_d = self.json[SPATIAL_REFERENCE]
        elif self.json.get(EXTENT) and SPATIAL_REFERENCE in self.json[EXTENT]:
            resp_d = self.json[EXTENT][SPATIAL_REFERENCE]
        elif GEOMETRIES in self.json:
            try:
                first = self.json.get(GEOMETRIES, [])[0]
                resp_d = first.get(SPATIAL_REFERENCE) or {}
            except IndexError:
                pass
        return munch.munchify(resp_d)

    def getSR(self):
        """return the spatial reference"""
        sr_dict = self._spatialReference
        sr = self._find_wkid(sr_dict)
        if sr is None:
            if isinstance(sr_dict, dict):
                return sr_dict.get(WKT)
        return sr


    def getWKID(self):
        """returns the well known id for service spatial reference"""
        return self._find_wkid(self._spatialReference)
##        resp_d = self._spatialReference
##        for key in [LATEST_WKID, WKID]:
##            if key in resp_d:
##                return resp_d[key]

    def getWKT(self):
        """returns the well known text (if it exists) for a service"""
        return self._spatialReference.get(WKT, '')


class FieldsMixin(object):
    json = {}

    @property
    def OIDFieldName(self):
        """gets the OID field name if it exists in feature set"""
        if hasattr(self, OBJECTID_FIELD) and getattr(self, OBJECTID_FIELD):
            return getattr(self, OBJECTID_FIELD)

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

    @property
    def GlobalIdFieldName(self):
        """gets the Shape field name if it exists in feature set"""
        if hasattr(self, GLOBALID_FIELD) and getattr(self, GLOBALID_FIELD):
            return getattr(self, GLOBALID_FIELD)

        try:
            return [f.name for f in self.fields if f.type == GLOBALID][0]
        except IndexError:
           return None

    @property
    def fieldLookup(self):
        """convenience property for field lookups"""
        return {f.name: f for f in self.fields}

    def list_fields(self):
        """returns a list of field names"""
        return [f.name for f in self.fields]

class FeatureSet(JsonGetter, SpatialReferenceMixin, FieldsMixin):

    def __init__(self, in_json):
        """class to handle feature set

        Required:
            in_json -- input json response from request
        """
        if isinstance(in_json, six.string_types):
            in_json = json.loads(in_json)
        if isinstance(in_json, self.__class__):
            self.json = in_json.json
        elif isinstance(in_json, dict):
            self.json = munch.munchify(in_json)
        if not all(map(lambda k: k in self.json.keys(), [FIELDS, FEATURES])):
            print(self.json.keys())
            raise ValueError('Not a valid Feature Set!')

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

    def extend(self, other):
        """combines features from another FeatureSet with this one.

        Required:
            other -- other FeatureSet to combine with this one.
        """
        if not isinstance(other, FeatureSet):
            other = FeatureSet(other)
        otherCopy = copy.deepcopy(other)

        # get max oid
        oidF = getattr(self, OID_FIELD_NAME) if hasattr(self, OID_FIELD_NAME) else 'OBJECTID'
        nextOID = max([ft.get(oidF, 0) for ft in iter(self)]) + 1

        if sorted(self.list_fields()) == sorted(other.list_fields()):
            for ft in otherCopy.features:
                if ft.get(oidF) < nextOID:
                    ft.attributes[oidF] = nextOID
                    nextOID += 1
            self.features.extend(otherCopy.features)

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
            yield feature

    def __len__(self):
        return len(self.features)

    def __bool__(self):
        return bool(len(self))

    def __dir__(self):
        return sorted(list(self.__class__.__dict__.keys()) + self.json.keys())

    def __repr__(self):
        return '<{} (count: {})>'.format(self.__class__.__name__, self.count)

class Feature(JsonGetter):
    def __init__(self, feature):
        """represents a single feature

        Required:
            feature -- input json for feature
        """
        self.json = munch.munchify(feature)

    def get(self, field, default=None):
        """gets an attribute from the feature

        Required:
            field -- name of field for which to get attribute
        """
        if field in (ATTRIBUTES, GEOMETRY):
            return self.json.get(field, default)
        return self.json.get(ATTRIBUTES, {}).get(field, default)

    def __repr__(self):
        return self.dumps(indent=2)

    def __str__(self):
        return self.__repr__()

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

    def toFeatureSet(self):
        features = []
        for group in iter(self):
            features.extend(group[RELATED_RECORDS])
        return FeatureSet({FIELDS: self.json.fields, FEATURES: features})

    def __iter__(self):
        for group in self.json[RELATED_RECORD_GROUPS]:
            yield group

class BaseService(RESTEndpoint, SpatialReferenceMixin):
    """base class for all services"""
    def __init__(self, url, usr='', pw='', token='', proxy=None, referer=None, **kwargs):
        super(BaseService, self).__init__(url, usr, pw, token, proxy, referer, **kwargs)
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

class OrderedDict2(OrderedDict):
    """wrapper for OrderedDict"""

    def __repr__(self):
        """we want it to look like a dictionary"""
        return json.dumps(self, indent=2, ensure_ascii=False)

class PortalInfo(JsonGetter):
    def __init__(self, response):
        self.json = response
        #super(PortalInfo, self).__init__(response)

    @property
    def username(self):
        return self.json.get(USER, {}).get(USER_NAME)

    @property
    def fullName(self):
        return self.json.get(USER, {}).get(FULL_NAME)

    @property
    def domain(self):
        return (self.json.get(URL_KEY, '') + ORG_MAPS).lower()

    @property
    def org(self):
        return self.json.get(URL_KEY)

    def __repr__(self):
        return '<PortaInfo: {}>'.format(self.domain)

class Token(JsonGetter):
    """class to handle token authentication"""
    _portal = None
    def __init__(self, response):
        """response JSON object from generate_token"""
        self.json = munch.munchify(response)
        super(JsonGetter, self).__init__()
        self._cookie = {AGS_TOKEN: self.token}
        self._portal = self.json.get('_{}'.format(PORTAL_INFO))
        del self.json._portalInfo
##        self.isAGOL = self.json.get(IS_AGOL, False)
##        self.isAdmin = self.json.get(IS_ADMIN, False)


    @property
    def portalInfo(self):
        return PortalInfo(self._portal)

    @property
    def portalUser(self):
        if isinstance(self.portalInfo, PortalInfo):
            return self.portalInfo.username
        return None

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
            raise RuntimeError(json.dumps(err, indent=2, ensure_ascii=False))

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

class GeocodeResult(JsonGetter, SpatialReferenceMixin):
    """class to handle Reverse Geocode Result"""
    def __init__(self, res_dict, geo_type):
        """geocode response object

        Required:
            res_dict -- JSON response from geocode request
            geo_type -- type of geocode operation (reverseGeocode|findAddressCandidates|geocodeAddresses)
        """
        RequestError(res_dict)
        super(GeocodeResult, self).__init__()
        self.json = res_dict
        self.type = 'esri_' + geo_type

    @property
    def results(self):
        """returns list of result objects"""
        if self.type == 'esri_findAddressCandidates':
            return self.candidates
        elif self.type == 'esri_reverseGeocode':
            return [self.address]
        else:
            return self.json.get(LOCATIONS, [])

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

    def __repr__(self):
        return '<{}: {} match{}>'.format(self.__class__.__name__, len(self), 'es' if len(self) else '')

class EditResult(JsonGetter):
    """class to handle Edit operation results"""
    def __init__(self, res_dict, feature_id=None):
        RequestError(res_dict)
        self.json = munch.munchify(res_dict)

    @staticmethod
    def success_count(l):
        return len([d for d in l if d.get(SUCCESS_STATUS) in (True, TRUE)])

    def summary(self):
        """print summary of edit operation"""
        if self.json.get(ADD_RESULTS, []):
            print('Added {} feature(s)'.format(self.success_count(getattr(self, ADD_RESULTS))))
        if self.json.get(UPDATE_RESULTS, []):
            print('Updated {} feature(s)'.format(self.success_count(getattr(self, UPDATE_RESULTS))))
        if self.json.get(DELETE_RESULTS, []):
            print('Deleted {} feature(s)'.format(self.success_count(getattr(self, DELETE_RESULTS))))
        if self.json.get(ATTACHMENTS, []):
            print('Attachment Edits: {}'.format(self.success_count(getattr(self, ATTACHMENTS))))
        if self.json.get(ADD_ATTACHMENT_RESULT):
            try:
                k,v = list(getattr(self, ADD_ATTACHMENT_RESULT).items())[0]
                print("Added attachment '{}' for feature {}".format(v, k))
            except IndexError: # should never happen?
                print('Added 1 attachment')
        if self.json.get(DELETE_ATTACHMENT_RESULTS):
            try:
                for res in getattr(self, DELETE_ATTACHMENT_RESULTS, []) or []:
                    if res.get(SUCCESS_STATUS) in (True, TRUE):
                        print("Deleted attachment '{}'".format(res.get(RESULT_OBJECT_ID)))
                    else:
                        print("Failed to Delete attachment '{}'".format(res.get(RESULT_OBJECT_ID)))
            except IndexError: # should never happen?
                print('Deleted {} attachment(s)'.format(len(getattr(self, DELETE_ATTACHMENT_RESULT))))
        if self.json.get(UPDATE_ATTACHMENT_RESULT):
            try:
                print("Updated attachment '{}'".format(self.json.get(UPDATE_ATTACHMENT_RESULT, {}).get(RESULT_OBJECT_ID)))
            except IndexError: # should never happen?
                print('Updated 1 attachment')

class BaseGeometry(SpatialReferenceMixin):
    """base geometry obect"""

    def dumps(self, **kwargs):
        """retuns JSON as a string"""
        if 'ensure_ascii' not in kwargs:
            kwargs['ensure_ascii'] = False
        return json.dumps(self.json, **kwargs)


class BaseGeometryCollection(SpatialReferenceMixin):
    """Base Geometry Collection"""
    geometries = []
    json = {GEOMETRIES: []}
    geometryType = NULL

    @property
    def count(self):
        return len(self)

    def dumps(self, **kwargs):
        """retuns JSON as a string"""
        if 'ensure_ascii' not in kwargs:
            kwargs['ensure_ascii'] = False
        return json.dumps(self.json, **kwargs)

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
                recs[RECORDS].append({ATTRIBUTES: {"OBJECTID": i+1, address_field: addr}})

        # validate recs, make sure OBECTID is present
        elif isinstance(recs, dict) and RECORDS in recs:
            for i, atts in enumerate(recs[RECORDS]):
                if not OBJECTID in atts[ATTRIBUTES]:
                    atts[ATTRIBUTES][OBJECTID] = i + 1 #do not start at 0

        else:
            raise ValueError('Not a valid input for "recs" parameter!')

        params = {ADDRESSES: json.dumps(recs, ensure_ascii=False),
                      OUT_SR: outSR,
                      F: JSON}

        return GeocodeResult(self.request(geo_url, params), geo_url.split('/')[-1])

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

        return GeocodeResult(self.request(geo_url, params), geo_url.split('/')[-1])

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
            for fld_name, fld_query in six.iteritems(kwargs):
                params[fld_name] = fld_query

        return GeocodeResult(self.request(geo_url, params), geo_url.split('/')[-1])

    def __repr__(self):
        """string representation with service name"""
        return '<GeocodeService: {}>'.format('/'.join(self.url.split('/services/')[-1].split('/')[:-1]))