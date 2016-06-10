"""Helper functions and base classes for restapi module"""
from __future__ import print_function
import requests
import getpass
import fnmatch
import datetime
import collections
import urllib
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

# disable ssl warnings (we are not verifying certs...maybe should look at trying to auto verify ssl in future)
for warning in [SNIMissingWarning, InsecurePlatformWarning, InsecureRequestWarning]:
    requests.packages.urllib3.disable_warnings(warning)

class IdentityManager(object):
    """Identity Manager for secured services.  This will allow the user to only have
    to sign in once (until the token expires) when accessing a services directory or
    individual service on an ArcGIS Server Site"""
    tokens = {}
    proxies = {}

    def findToken(self, url):
        """returns a token for a specific domain from token store if one has been
        generated for the ArcGIS Server resource

        Required:
            url -- url for secured resource
        """
        if self.tokens:
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

def Field(f_dict={}):
    """returns a list of safe field Munch() objects that will
    always have the following keys:
        ('name', 'length', 'type', 'domain')

    Required:
        f_dict -- dictionary containing Field properties
    """
    # make sure always has at least (name, length, type, domain)
    for attr in (NAME, LENGTH, TYPE, DOMAIN):
        if not attr in f_dict:
            f_dict[attr] = None

    return munch.munchify(f_dict)

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

def POST(service, params={F: JSON}, ret_json=True, token='', cookies=None, proxy=None):
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
    if PROTOCOL != '':
        service = '{}://{}'.format(PROTOCOL, service.split('://')[-1])
    if not cookies and not proxy:
        if not token:
            token = ID_MANAGER.findToken(service)
        if token and isinstance(token, Token) and token.domain.lower() in service.lower():
            if isinstance(token, Token) and token.isExpired:
                raise RuntimeError('Token expired at {}! Please sign in again.'.format(token.expires))
            cookies = {AGS_TOKEN: str(token)}
        elif token:
            cookies = {AGS_TOKEN: str(token)}

    for pName, p in params.iteritems():
        if isinstance(p, dict):
            params[pName] = json.dumps(p)

    if not F in params:
        params[F] = JSON

    if not token and not proxy:
        proxy = ID_MANAGER.findProxy(service)

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
            RequestError(r.json())
            return munch.munchify(r.json())
        else:
            return r

def do_proxy_request(proxy, url, params={}):
    """make request against ArcGIS service through a proxy.  This is designed for a
    proxy page that stores access credentials in the configuration to handle authentication.
    It is also assumed that the proxy is a standard Esri proxy (i.e. retrieved from their
    repo on GitHub)

    Required:
        proxy -- full url to proxy
        url -- service url to make request against
    Optional:
        params -- query parameters, user is responsible for passing in the
            proper paramaters
    """
    if not F in params:
        params[F] = JSON
    p = '&'.join('{}={}'.format(k,v) for k,v in params.iteritems() if k != F)

    # probably a better way to do this, but I couldn't figure out how to use the "proxies" kwarg
    return requests.post('{}?{}?f={}&{}'.format(proxy, url, params[F], p).rstrip('&'), headers={'User-Agent': USER_AGENT})

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

def guessWKID(wkt):
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


def assignUniqueName(fl):
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
    infoUrl = url.split('/rest')[0] + '/rest/info'
    infoResp = POST(infoUrl)
    if AUTH_INFO in infoResp and TOKEN_SERVICES_URL in infoResp[AUTH_INFO]:
        base = infoResp[AUTH_INFO][TOKEN_SERVICES_URL]
        setattr(sys.modules[__name__], 'PROTOCOL', base.split('://')[0])
        print('set PROTOCOL to "{}" from generate token'.format(PROTOCOL))
        shortLived = infoResp[AUTH_INFO][SHORT_LIVED_TOKEN_VALIDITY]
    else:
        base = url.split('/rest')[0] + '/tokens'
        shortLived = 100

    params = {F: JSON,
              USER_NAME: user,
              PASSWORD: pw,
              CLIENT: REQUEST_IP,
              EXPIRATION: max([expiration, shortLived])}

    resp = POST(base, params)
    resp[DOMAIN] = base.split('/tokens')[0].lower() + '/rest/services'
    token = Token(resp)
    ID_MANAGER.tokens[token.domain] = token
    return token

class RESTEndpoint(object):
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

        self.raw_response = POST(self.url, params, ret_json=False, cookies=self._cookie, proxy=self._proxy)
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

class FeatureSet(SpatialReferenceMixin):

    def __init__(self, in_json):
        """class to handle feature set

        Required:
            in_json -- input json response from request
        """
        if isinstance(in_json, basestring):
            in_json = json.loads(in_json)
        elif isinstance(in_json, self.__class__):
            self.json = in_json.json
        else:
            self.json = munch.munchify(in_json)
        if not all([self.json.get(k) for k in (FIELDS, FEATURES, GEOMETRY_TYPE)]):
            raise ValueError('Not a valid Feature Set!')

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
            return self.features[key]
        else:
            return self.json.get(key)

    def __iter__(self):
        for feature in self.features:
            yield feature

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

class Token(object):
    """class to handle token authentication"""
    def __init__(self, response):
        """response JSON object from generate_token"""
        self.token = response[TOKEN]
        self.expires = mil_to_date(response[EXPIRES])
        self._cookie = {AGS_TOKEN: self.token}
        self.domain = response[DOMAIN]
        self._response = response

    @property
    def isExpired(self):
        """boolean value for expired or not"""
        if datetime.datetime.now() > self.expires:
            return True
        else:
            return False

    def asJSON(self):
        """return original server response as JSON"""
        return self._response

    def __str__(self):
        """return token as string representation"""
        return self.token

class RequestError(object):
    """class to handle restapi request errors"""
    def __init__(self, err):
        if 'error' in err:
            raise RuntimeError('\n' + '\n'.join('{} : {}'.format(k,v) for k,v in err['error'].items()))

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
    __slots__ = ['response', SPATIAL_REFERENCE, TYPE, 'candidates',
                LOCATIONS, 'address', RESULTS, 'result', 'Result']

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
            addr_dict[ATTRIBUTES] = self.response['address']
            address = self.response['address'].get('Address', None)
            if address is None:
                add = self.response['address']
                addr_dict['address'] = ' '.join(filter(None, [add.get('Street'), add.get('City'), add.get('ZIP')]))
            else:
                addr_dict['address'] = address
            addr_dict['score'] = None
            self.address.append(addr_dict)

        # legacy response from find? <- deprecated?
        # http://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find #still works
        elif self.type == 'esri_find':
            # format legacy results
            for res in self.response[LOCATIONS]:
                ref_dict = {}
                for k,v in res.iteritems():
                    if k == NAME:
                        ref_dict['address'] = v
                    elif k == 'feature':
                        atts_dict = {}
                        for att, val in res[k].iteritems():
                            if att == GEOMETRY:
                                ref_dict[LOCATION] = val
                            elif att == ATTRIBUTES:
                                for att2, val2 in res[k][att].iteritems():
                                    if att2.lower() == 'score':
                                        ref_dict['score'] = val2
                                    else:
                                        atts_dict[att2] = val2
                            ref_dict[ATTRIBUTES] = atts_dict
                self.locations.append(ref_dict)

        else:
            if self.type == 'esri_findAddressCandidates':
                self.candidates = self.response['candidates']

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
    __slots__ = ['addResults', 'updateResults', 'deleteResults',
                'summary', 'affectedOIDs', 'failedOIDs', 'response']
    def __init__(self, res_dict):
        RequestError(res_dict)
        self.response = res_dict
        self.failedOIDs = []
        self.addResults = []
        self.updateResults = []
        self.deleteResults = []
        for key, value in res_dict.iteritems():
            for v in value:
                if v['success'] in (True, TRUE):
                    getattr(self, key).append(v['objectId'])
                else:
                    self.failedOIDs.append(v['objectId'])
        self.affectedOIDs = self.addResults + self.updateResults + self.deleteResults

    def summary(self):
        """print summary of edit operation"""
        if self.affectedOIDs:
            if self.addResults:
                print('Added {} feature(s)'.format(len(self.addResults)))
            if self.updateResults:
                print('Updated {} feature(s)'.format(len(self.updateResults)))
            if self.deleteResults:
                print('Deleted {} feature(s)'.format(len(self.deleteResults)))
        if self.failedOIDs:
            print('Failed to edit {0} feature(s)!\n{1}'.format(len(self.failedOIDs), self.failedOIDs))

    def __len__(self):
        """return count of affected OIDs"""
        return len(self.affectedOIDs)

class BaseGeometryCollection(object):
    """Base Geometry Collection"""
    geometries = []
    json = {GEOMETRIES: []}
    geometryType = None

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

class BaseArcServer(RESTEndpoint):
    """Class to handle ArcGIS Server Connection"""
    def __init__(self, url, usr='', pw='', token='', proxy=None):
        super(BaseArcServer, self).__init__(url, usr, pw, token, proxy)
        self.service_cache = []

    @property
    def mapServices(self):
        """list of all MapServer objects"""
        if not self.service_cache:
            self.service_cache = self.list_services()
        return [s for s in self.service_cache if s.endswith('MapServer')]

    @property
    def featureServices(self):
        """list of all MapServer objects"""
        if not self.service_cache:
            self.service_cache = self.list_services()
        return [s for s in self.service_cache if s.endswith('FeatureServer')]

    @property
    def imageServices(self):
        """list of all MapServer objects"""
        if not self.service_cache:
            self.service_cache = self.list_services()
        return [s for s in self.service_cache if s.endswith('ImageServer')]

    @property
    def gpServices(self):
        """list of all MapServer objects"""
        if not self.service_cache:
            self.service_cache = self.list_services()
        return [s for s in self.service_cache if s.endswith('GPServer')]


    def list_services(self, filterer=True):
        """returns a list of all services

        Optional:
            filterer -- default is true to exclude "Utilities" and "System" folders,
                set to false to list all services.
        """
        return list(self.iter_services(filterer))

    def iter_services(self, token='', filterer=True):
        """returns a generator for all services

        Required:
            service -- full path to a rest services directory

        Optional:
            token -- token to handle security (only required if security is enabled)
            filterer -- default is true to exclude "Utilities" and "System" folders,
                set to false to list all services.
        """
        self.service_cache = []
        for s in self.services:
            full_service_url = '/'.join([self.url, s[NAME], s[TYPE]])
            self.service_cache.append(full_service_url)
            yield full_service_url
        folders = self.folders
        if filterer:
            for fld in ('Utilities', 'System'):
                try:
                    folders.remove(fld)
                except: pass
        for s in folders:
            new = '/'.join([self.url, s])
            resp = POST(new, token=self.token)
            for serv in resp[SERVICES]:
                full_service_url =  '/'.join([self.url, serv[NAME], serv[TYPE]])
                self.service_cache.append(full_service_url)
                yield full_service_url

    def get_service_url(self, wildcard='*', _list=False):
        """method to return a service url

        Optional:
            wildcard -- wildcard used to grab service name (ex "moun*featureserver")
            _list -- default is false.  If true, will return a list of all services
                matching the wildcard.  If false, first match is returned.
        """
        if not self.service_cache:
            self.list_services()
        if '*' in wildcard:
            if wildcard == '*':
                return self.service_cache[0]
            else:
                if _list:
                    return [s for s in self.service_cache if fnmatch.fnmatch(s, wildcard)]
            for s in self.service_cache:
                if fnmatch.fnmatch(s, wildcard):
                    return s
        else:
            if _list:
                return [s for s in self.service_cache if wildcard.lower() in s.lower()]
            for s in self.service_cache:
                if wildcard.lower() in s.lower():
                    return s
        print('"{0}" not found in services'.format(wildcard))
        return ''

    def get_folders(self):
        """method to get folder objects"""
        folder_objects = []
        for folder in self.folders:
            folder_url = '/'.join([self.url, folder])
            folder_objects.append(Folder(folder_url, self.token))
        return folder_objects

    def walk(self, filterer=True):
        """method to walk through ArcGIS REST Services. ArcGIS Server only supports single
        folder heiarchy, meaning that there cannot be subdirectories within folders.

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
        self.service_cache = []
        services = []
        for s in self.services:
            qualified_service = '/'.join([s[NAME], s[TYPE]])
            full_service_url = '/'.join([self.url, qualified_service])
            services.append(qualified_service)
            self.service_cache.append(full_service_url)
        folders = self.folders
        if filterer:
            for fld in ('Utilities', 'System'):
                try:
                    folders.remove(fld)
                except: pass
        yield (self.url, folders, services)

        for f in folders:
            new = '/'.join([self.url, f])
            endpt = POST(new, token=self.token)
            services = []
            for serv in endpt[SERVICES]:
                qualified_service = '/'.join([serv[NAME], serv[TYPE]])
                full_service_url = '/'.join([self.url, qualified_service])
                services.append(qualified_service)
                self.service_cache.append(full_service_url)
            yield (f, endpt[FOLDERS], services)

    def __iter__(self):
        """returns an generator for services"""
        return self.list_services()

    def __len__(self):
        """returns number of services"""
        return len(self.service_cache)

class BaseMapService(BaseService):
    """Class to handle map service and requests"""

    def getLayerIdByName(self, name, grp_lyr=False):
        """gets a mapservice layer ID by layer name from a service (returns an integer)

        Required:
            name -- name of layer from which to grab ID

        Optional:
            grp_lyr -- default is false, does not return layer ID for group layers.  Set
                to true to search for group layers too.
        """
        all_layers = self.layers
        for layer in all_layers:
            if fnmatch.fnmatch(layer[NAME], name):
                if SUB_LAYER_IDS in layer:
                    if grp_lyr and layer[SUB_LAYER_IDS] != None:
                        return layer[ID]
                    elif not grp_lyr and not layer[SUB_LAYER_IDS]:
                        return layer[ID]
                return layer[ID]
        for tab in r[TABLES]:
            if fnmatch.fnmatch(tab[NAME], name):
                return tab[ID]
        print('No Layer found matching "{0}"'.format(name))
        return None

    def get_layer_url(self, name, grp_lyr=False):
        """returns the fully qualified path to a layer url by pattern match on name,
        will return the first match.

        Required:
            name -- name of layer from which to grab ID

        Optional:
            grp_lyr -- default is false, does not return layer ID for group layers.  Set
                to true to search for group layers too.
        """
        return '/'.join([self.url, str(self.getLayerIdByName(name,grp_lyr))])

    def list_layers(self):
        """Method to return a list of layer names in a MapService"""
        return [l.name for l in self.layers]

    def list_tables(self):
        """Method to return a list of layer names in a MapService"""
        return [t.name for t in self.tables]

    def getNameFromId(self, lyrID):
        """method to get layer name from ID

        Required:
            lyrID -- id of layer for which to get name
        """
        return [l.name for l in self.layers if l.id == lyrID][0]

    def export(self, out_image, imageSR=None, bbox=None, bboxSR=None, size=None, dpi=96, format='png8', transparent=True, **kwargs):
        """exports a map image

        Required:
            out_image -- full path to output image

        Optional:
            imageSR -- spatial reference for exported image
            bbox -- bounding box as comma separated string
            bboxSR -- spatial reference for bounding box
            size -- comma separated string for the size of image in pixels. It is advised not to use
                this parameter and let this method generate it automatically
            dpi -- output resolution, default is 96
            format -- image format, default is png8
            transparent -- option to support transparency in exported image, default is True
            kwargs -- any additional keyword arguments for export operation (must be supported by REST API)

        Keyword Arguments can be found here:
            http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Export_Map/02r3000000v7000000/
        """
        query_url = self.url + '/export'

        # defaults if params not specified
        if bbox and not size:
            if isinstance(bbox, (list, tuple)):
                size = ','.join([abs(int(bbox[0]) - int(bbox[2])), abs(int(bbox[1]) - int(bbox[3]))])
        if not bbox:
            ie = self.initialExtent
            bbox = ','.join(map(str, [ie.xmin, ie.ymin, ie.xmax, ie.ymax]))

            if not size:
                size = ','.join(map(str, [abs(int(ie.xmin) - int(ie.xmax)), abs(int(ie.ymin) - int(ie.ymax))]))

            bboxSR = self.spatialReference

        if not imageSR:
            imageSR = self.spatialReference

        # initial params
        params = {FORMAT: format,
          F: IMAGE,
          IMAGE_SR: imageSR,
          BBOX_SR: bboxSR,
          BBOX: bbox,
          TRANSPARENT: transparent,
          DPI: dpi,
          SIZE: size}

        # add additional params from **kwargs
        for k,v in kwargs.iteritems():
            if k not in params:
                params[k] = v

        # do post
        r = POST(query_url, params, ret_json=False)

        # save image
        with open(out_image, 'wb') as f:
            f.write(r.content)

        return r

    def __iter__(self):
        for lyr in self.layers:
            yield lyr

class BaseMapServiceLayer(RESTEndpoint, SpatialReferenceMixin):
    """Class to handle advanced layer properties"""
    def __init__(self, url='', usr='', pw='', token='', proxy=None):
        super(BaseMapServiceLayer, self).__init__(url, usr, pw, token, proxy)
        try:
            self.json[FIELDS] = [Field(f) for f in self.json[FIELDS]]
        except:
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

    def list_fields(self):
        """method to list field names"""
        return [f.name for f in self.fields]

    def fix_fields(self, fields):
        """fixes input fields, accepts esri field tokens too ("SHAPE@", "OID@")

        Required:
            fields -- list or comma delimited field list
        """
        field_list = []
        if fields == '*':
            return fields
        elif isinstance(fields, basestring):
            fields = fields.split(',')
        if isinstance(fields, list):
            all_fields = self.list_fields()
            for f in fields:
                if '@' in f:
                    if f.upper() == SHAPE_TOKEN:
                        if self.SHAPE:
                            field_list.append(self.SHAPE.name)
                    elif f.upper() == OID_TOKEN:
                        if self.OID:
                            field_list.append(self.OID.name)
                else:
                    if f in all_fields:
                        field_list.append(f)
        return ','.join(field_list)

    def query_all(self, oid, max_recs, where='1=1', add_params={}, token=''):
        """generator to form where clauses to query all records.  Will iterate through "chunks"
        of OID's until all records have been returned (grouped by maxRecordCount)

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
            add_params[RETURN_IDS_ONLY] = TRUE

        # get oids
        oids = sorted(self.query(where=where, add_params=add_params, token=token)[OBJECT_IDS])
        print('total records: {0}'.format(len(oids)))

        # set returnIdsOnly to False
        add_params[RETURN_IDS_ONLY] = 'false'

        # iterate through groups to form queries
        for each in izip_longest(*(iter(oids),) * max_recs):
            theRange = filter(lambda x: x != None, each) # do not want to remove OID "0"
            _min, _max = min(theRange), max(theRange)
            del each

            yield '{0} >= {1} and {0} <= {2}'.format(oid, _min, _max)

    def query(self, fields='*', where='1=1', add_params={}, records=None, get_all=False, f=JSON, kmz='', **kwargs):
        """query layer and get response as JSON

        Optional:
            fields -- fields to return. Default is "*" to return all fields
            where -- where clause
            add_params -- extra parameters to add to query string passed as dict
            records -- number of records to return.  Default is None to return all
                records within bounds of max record count unless get_all is True
            get_all -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records.
            kwargs -- extra parameters to add to query string passed as key word arguments,
                will override add_params***

        # default params for all queries
        params = {'returnGeometry' : 'true', 'outFields' : fields,
                  'where': where, 'f' : 'json'}
        """
        query_url = self.url + '/query'

        # default params
        params = {RETURN_GEOMETRY : TRUE, WHERE: where, F : f}

        for k,v in add_params.iteritems():
            params[k] = v

        for k,v in kwargs.iteritems():
            params[k] = v

        # check for tokens (only shape and oid)
        fields = self.fix_fields(fields)
        params[OUT_FIELDS] = fields

        # create kmz file if requested (does not support get_all parameter)
        if f == 'kmz':
            r = POST(query_url, params, ret_json=False, token=self.token)
            r.encoding = 'zlib_codec'

            # write kmz using codecs
            if not kmz:
                kmz = validate_name(os.path.join(os.path.expanduser('~'), 'Desktop', '{}.kmz'.format(self.name)))
            with codecs.open(kmz, 'wb') as f:
                f.write(r.content)
            print('Created: "{0}"'.format(kmz))
            return kmz

        else:

            server_response = {}

            if get_all:
                records = None
                max_recs = self.json.get('maxRecordCount')
                if not max_recs:
                    # guess at 500 (default 1000 limit cut in half at 10.0 if returning geometry)
                    max_recs = 500

                for i, where2 in enumerate(self.query_all(oid_name, max_recs, where, add_params, self.token)):
                    sql = ' and '.join(filter(None, [where.replace('1=1', ''), where2])) #remove default
                    resp = POST(query_url, params, token=self.token)
                    if i < 1:
                        server_response = resp
                    else:
                        server_response[FEATURES] += resp[FEATURES]

            else:
                server_response = POST(query_url, params, token=self.token)

            return FeatureSet(server_response)

    def select_by_location(self, geometry, geometryType='', inSR='', spatialRel='esriSpatialRelIntersects', distance=0, units='esriSRUnit_Meter', add_params={}, **kwargs):
        """Selects features by location of a geometry, returns a feature set

        Required:
            geometry -- geometry as JSON

        Optional:
            geometryType -- type of geometry object, this can be gleaned automatically from the geometry input
            inSR -- input spatial reference
            spatialRel -- spatial relationship applied on the input geometry when performing the query operation
            distance -- distance for search
            units -- units for distance, only used if distance > 0 and if supportsQueryWithDistance is True
            add_params -- dict containing any other options that will be added to the query
            kwargs -- keyword args to add to the query


        Spatial Relationships:
            esriSpatialRelIntersects | esriSpatialRelContains | esriSpatialRelCrosses | esriSpatialRelEnvelopeIntersects | esriSpatialRelIndexIntersects
            | esriSpatialRelOverlaps | esriSpatialRelTouches | esriSpatialRelWithin | esriSpatialRelRelation

        Unit Options:
            esriSRUnit_Meter | esriSRUnit_StatuteMile | esriSRUnit_Foot | esriSRUnit_Kilometer | esriSRUnit_NauticalMile | esriSRUnit_USNauticalMile
        """
        if isinstance(geometry, basestring):
            geometry = json.loads(geometry)

        if not geometryType:
            for key,gtype in GEOM_DICT.iteritems():
                if key in geometry:
                    geometryType = gtype
                    break

        if SPATIAL_REFERENCE in geometry:
            sr_dict = geometry[SPATIAL_REFERENCE]
            inSR = sr_dict.get(LATEST_WKID) if sr_dict.get(LATEST_WKID) else sr_dict.get(WKID)

        params = {GEOMETRY: geometry,
                  GEOMETRY_TYPE: geometryType,
                  SPATIAL_REL: spatialRel,
                  IN_SR: inSR,
            }

        if int(distance):
            params[DISTANCE] = distance
            params[UNITS] = units

        # add additional params
        for k,v in add_params.iteritems():
            if k not in params:
                params[k] = v

        # add kwargs
        for k,v in kwargs.iteritems():
            if k not in params:
                params[k] = v

        return FeatureSet(self.query(add_params=params))

    def layer_to_kmz(self, out_kmz='', flds='*', where='1=1', params={}):
        """Method to create kmz from query

        Optional:
            out_kmz -- output kmz file path, if none specified will be saved on Desktop
            flds -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            where -- optional where clause
            params -- dictionary of parameters for query
        """
        return query(self.url, flds, where=where, add_params=params, ret_form='kmz', token=self.token, kmz=out_kmz)

    def getOIDs(self, where='1=1', max_recs=None, **kwargs):
        """return a list of OIDs from feature layer

        Optional:
            where -- where clause for OID selection
            max_recs -- maximimum number of records to return (maxRecordCount does not apply)
            **kwargs -- optional key word arguments to further limit query (i.e. add geometry interesect)
        """
        p = {RETURN_IDS_ONLY:TRUE,
             RETURN_GEOMETRY: FALSE,
             OUT_FIELDS: ''}

        # add kwargs if specified
        for k,v in kwargs.iteritems():
            if k not in p.keys():
                p[k] = v

        return sorted(self.query(where=where, add_params=p)[OBJECT_IDS])[:max_recs]

    def getCount(self, where='1=1', **kwargs):
        """get count of features, can use optional query and **kwargs to filter

        Optional:
            where -- where clause
            kwargs -- keyword arguments for query operation
        """
        return len(self.getOIDs(where,  **kwargs))

    def attachments(self, oid, gdbVersion=''):
        """query attachments for an OBJECTDID

        Required:
            oid -- object ID

        Optional:
            gdbVersion -- Geodatabase version to query, only supported if self.isDataVersioned is true
        """
        if self.hasAttachments:
            query_url = '{0}/{1}/attachments'.format(self.url, oid)
            r = POST(query_url, cookies=self._cookie)

            add_tok = ''
            if self.token:
                add_tok = '?token={}'.format(self.token.token if isinstance(self.token, Token) else self.token)

            if ATTACHMENT_INFOS in r:
                for attInfo in r[ATTACHMENT_INFOS]:
                    attInfo[URL] = '{}/{}'.format(query_url, attInfo[ID])
                    attInfo[URL_WITH_TOKEN] = '{}/{}{}'.format(query_url, attInfo[ID], add_tok)

                class Attachment(namedtuple('Attachment', 'id name size contentType url urlWithToken')):
                    """class to handle Attachment object"""
                    __slots__ = ()
                    def __new__(cls,  **kwargs):
                        return super(Attachment, cls).__new__(cls, **kwargs)

                    def __repr__(self):
                        if hasattr(self, ID) and hasattr(self, NAME):
                            return '<Attachment ID: {} ({})>'.format(self.id, self.name)
                        else:
                            return '<Attachment> ?'

                    def download(self, out_path, name='', verbose=True):
                        """download the attachment to specified path

                        out_path -- output path for attachment

                        optional:
                            name -- name for output file.  If left blank, will be same as attachment.
                            verbose -- if true will print sucessful download message
                        """
                        if not name:
                            out_file = assignUniqueName(os.path.join(out_path, self.name))
                        else:
                            ext = os.path.splitext(self.name)[-1]
                            out_file = os.path.join(out_path, name.split('.')[0] + ext)

                        with open(out_file, 'wb') as f:
                            f.write(urllib.urlopen(self.url).read())

                        if verbose:
                            print('downloaded attachment "{}" to "{}"'.format(self.name, out_path))
                        return out_file

                return [Attachment(**a) for a in r[ATTACHMENT_INFOS]]

            return []

        else:
            raise NotImplementedError('Layer "{}" does not support attachments!'.format(self.name))

    def __repr__(self):
        """string representation with service name"""
        return '<{}: "{}" (id: {})>'.format(self.__class__.__name__, self.name, self.id)

class BaseImageService(BaseService):
    """Class to handle Image service and requests"""

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

class GeocodeService(RESTEndpoint):
    """class to handle Geocode Service"""
    def __init__(self, url, usr='', pw='', token='', proxy=None):
        """Geocode Service object

        Required:
            url -- Geocode service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
        """
        super(GeocodeService, self).__init__(url, usr, pw, token, proxy)
        self.name = self.url.split('/')[-2]

        self.locators = []
        for key, value in self.response.iteritems():
            if key in ('addressFields',
                       'candidateFields',
                       'intersectionCandidateFields'):
                setattr(self, key, [Field(v) for v in value])
            elif key == 'singleLineAddressField':
                setattr(self, key, Field(value))
            elif key == LOCATORS:
                for loc_dict in value:
                    self.locators.append(loc_dict[NAME])
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
                    print('Warning, no singleLineAddressField found...Using "{}" field'.format(address_field))
            for i, addr in enumerate(addr_list):
                recs['records'].append({"attributes": {"OBJECTID": i+1,
                                                       address_field: addr}})

        # validate recs, make sure OBECTID is present
        elif isinstance(recs, dict) and 'records' in recs:
            for i, atts in enumerate(recs['records']):
                if not OBJECTID in atts[ATTRIBUTES]:
                    atts[ATTRIBUTES][OBJECTID] = i + 1 #do not start at 0

        else:
            raise ValueError('Not a valid input for "recs" parameter!')

        params = {'addresses': json.dumps(recs),
                      OUT_SR: outSR,
                      F: JSON}

        return GeocodeResult(POST(geo_url, params, cookies=self._cookie), geo_url.split('/')[-1])

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

        return GeocodeResult(POST(geo_url, params, cookies=self._cookie), geo_url.split('/')[-1])

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
                  'returnIntersection': str(returnIntersection).lower(),
                  F: JSON}
        if address:
            if hasattr(self, 'singleLineAddressField'):
                params[self.singleLineAddressField.name] = address
            else:
                params[self.addressFields[0].name] = address
        if kwargs:
            for fld_name, fld_query in kwargs.iteritems():
                params[fld_name] = fld_query

        return GeocodeResult(POST(geo_url, params, cookies=self._cookie), geo_url.split('/')[-1])

    def __repr__(self):
        """string representation with service name"""
        return '<GeocodeService: {}>'.format('/'.join(self.url.split('/services/')[-1].split('/')[:-1]))


class GPService(BaseService):
    """GP Service object

        Required:
            url -- GP service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
        """

    def task(self, name):
        """returns a GP Task object"""
        return GPTask('/'.join([self.url, name]))

class GPTask(BaseService):
    """GP Task object

    Required:
        url -- GP Task url

    Optional (below params only required if security is enabled):
        usr -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
        token -- token to handle security (alternative to usr and pw)
        proxy -- option to use proxy page to handle security, need to provide
            full path to proxy url.
     """

    @property
    def isSynchronous(self):
        """task is synchronous"""
        return self.executionType == SYNCHRONOUS

    @property
    def isAsynchronous(self):
        """task is asynchronous"""
        return self.executionType == ASYNCHRONOUS

    @property
    def outputParameter(self):
        """returns the first output parameter (if there is one)"""
        try:
            return self.outputParameters[0]
        except IndexError:
            return None

    @property
    def outputParameters(self):
        """returns list of all output parameters"""
        return [p for p in self.parameters if p.direction == OUTPUT_PARAMETER]


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
            runType = EXECUTE
        else:
            runType = SUBMIT_JOB
        gp_exe_url = '/'.join([self.url, runType])
        if not params_json:
            params_json = {}
            for k,v in kwargs.iteritems():
                params_json[k] = v
        params_json['env:outSR'] = outSR
        params_json['env:processSR'] = processSR
        params_json[RETURN_Z] = returnZ
        params_json[RETURN_M] = returnZ
        params_json[F] = JSON
        r = POST(gp_exe_url, params_json, ret_json=False, cookies=self._cookie)
        gp_elapsed = r.elapsed

        # get result object as JSON
        res = r.json()

        # determine if there's an output parameter: if feature set, push result value into defaultValue
        if self.outputParameter and self.outputParameter.dataType == 'GPFeatureRecordSetLayer':
            try:
                default = self.outputParameter.defaultValue
                feature_set = default
                feature_set[FEATURES] = res[RESULTS][0][VALUE][FEATURES]
                feature_set[FIELDS] = default['Fields'] if 'Fields' in default else default[FIELDS]
                res[VALUE] = feature_set
            except:
                pass
        else:
            res[VALUE] = res[RESULTS][0].get(VALUE)

        print('GP Task "{}" completed successfully. (Elapsed time {})'.format(self.name, gp_elapsed))
        return GPResult(res)