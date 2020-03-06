"""Helper functions and base classes for restapi module"""
from __future__ import print_function
# from . import requests
import requests
import fnmatch
import datetime
import collections
import mimetypes
import warnings
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
from urllib3.exceptions import InsecureRequestWarning, InsecurePlatformWarning, SNIMissingWarning
from urllib3 import disable_warnings
from . import projections
from . import enums

import six
from six.moves import urllib

# disable ssl warnings
for warning in [SNIMissingWarning, InsecurePlatformWarning, InsecureRequestWarning]:
    disable_warnings(warning)

class RestapiEncoder(json.JSONEncoder):
    """Encoder for restapi objects to make serializeable for JSON."""
    def default(self, o):
        """Encodes object for JSON.
        
        Arg:
            o: Object.
        """
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
            return o.__class__.__name__ #{}

class TokenExpired(Exception):
    pass

def munch_repr(self):
    """ method override for munch, want to impersonate a pretty printed dict"""
    return json.dumps(self, indent=2, sort_keys=True, ensure_ascii=False, cls=RestapiEncoder)
   
# override repr(Munch)
munch.Munch.__repr__ = munch_repr

class IdentityManager(object):
    """Identity Manager for secured services.  This will allow the user to only have
            to sign in once (until the token expires) when accessing a services 
            directory or individual service on an ArcGIS Server Site.
            
    Attributes:
        tokens: Dictionary of the tokens.
        proxies: Dictionary of the proxies.
        portal_tokens: Dictionary of the portal tokens.       
    """
    def __init__(self):
        self.tokens = {}
        self.proxies = {}
        self._portal_tokens = {}

    def findToken(self, url):
        """Returns a token for a specific domain from token store if one has been
                generated for the ArcGIS Server resource.

        Arg:
            url: URL for secured resource.
        
        Raises:
            TokenExpired: 'Token expired at {}! Please sign in again.'
        """

        if self.tokens:
            # if fnmatch.fnmatch(url, PORTAL_BASE_PATTERN) and not fnmatch.fnmatch(url, PORTAL_SERVICES_PATTERN):
            #     url = get_portal_base(url)
            # elif '/admin/' in url:
            #     url = url.split('/admin/')[0] + '/admin'
            # else:
            #     url = url.split('/rest/services')[0] + '/rest/services'
            to_remove = []
            for registered_url, token in list(self.tokens.items()) + list(self._portal_tokens.items()):
                if fnmatch.fnmatch(url, registered_url + '*'):
                    if not token.isExpired:
                        return token
                    else:
                        to_remove.append(token)

            if to_remove:
                for token in to_remove:
                    if token.domain in self.tokens:
                        del self.tokens[token.domain]
                    elif token.domain in self._portal_tokens:
                        del self._portal_tokens[token.domain]

                msg = 'Token expired at {}! Please sign in again. ({})'
                raise TokenExpired('\n'.join([msg.format(token.time_expires, token.domain) for token in to_remove]))         
            
        return None

    def findProxy(self, url):
        """Returns a proxy url for a specific domain from token store if one has been
                used to access the ArcGIS Server resource

        Arg:
            url: URL for secured resource.
        """

        if self.proxies:
            url = url.lower().split('/rest/services')[0] + '/rest/services'
            if url in self.proxies:
                return self.proxies[url]

        return None

# initialize Identity Manager
ID_MANAGER = IdentityManager()

# temp dir for json outputs
TEMP_DIR = tempfile.gettempdir()
if not os.access(TEMP_DIR, os.W_OK| os.X_OK):
    TEMP_DIR = None

def parse_url(url):
    """alias for urllib parse
    
    Args:
        url (str): url to parse
    """
    return six.moves.urllib.parse.urlparse(url)

def namedTuple(name, pdict):
    """Creates a named tuple from a dictionary.

    Args:
        name: Name of namedtuple object.
        pdict: Parameter dictionary that defines the properties.
    """
    class obj(namedtuple(name, sorted(pdict.keys()))):
        """Class to handle {}""".format(name)
        __slots__ = ()
        def __new__(cls,  **kwargs):
            return super(obj, cls).__new__(cls, **kwargs)

        def asJSON(self):
            """Returns object as JSON."""
            return {f: getattr(self, f) for f in self._fields}

    o = obj(**pdict)
    o.__class__.__name__ = name
    return o

def Round(x, base=5):
    """Returns integer rounded to nearest n.
    
    Args:
        x: Number to be rounded.
        base: Number to divide and multiply x by.
    """
    return int(base * round(float(x)/base))

def iter_chunks(iterable, n):
    """Iterates an array in chunks.
    
    Args:
        iterable: A valid iterable.
        n = Number of chunks.
    """

    args = [iter(iterable)] * n
    for group in six.moves.zip_longest(*args, fillvalue=None):
        yield filter(None, group)

def tmp_json_file():
    """Returns a valid path for a temporary json file"""
    global TEMP_DIR
    if TEMP_DIR is None:
        TEMP_DIR = tempfile.mkdtemp()
    return os.path.join(TEMP_DIR, 'restapi_{}.json'.format(time.strftime('%Y%m%d%H%M%S')))

def do_post(service, params={F: JSON}, ret_json=True, token='', cookies=None, proxy=None, referer=None, **kwargs):
    """Post Request to REST Endpoint through query string, to post
            request with data in body, use requests.post(url, data:{k : v}).
    
    Args:
        service: Full path to REST endpoint of service.
        params: Optional parameters for posting a request. Defaults to {F: JSON}.
        ret_json: Optional boolean that returns the response as JSON if True.  
            Default is True.
        token: Optional token to handle security (only required if security is enabled).
            Defaults to ''.
        cookies: Optional arg for cookie object {'agstoken': 'your_token'}.
            Defaults to None.
        proxy: Option to use proxy page to handle security, need to provide
            full path to proxy url. Defaults to None.
    
    Raises:
        NameError: '"{0}" service not found!\n{1}'
    
    Returns:
        The post request.
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
                    params[enums.params.token] = str(token)
        elif token:
            if isinstance(token, Token) and (not token.isAGOL and not token.isAdmin):
                cookies = {enums.cookies.agstoken: str(token)}
            else:
                if enums.params.token not in params:
                    params[enums.params.token] = str(token)

    # auto fill in geometry params if a restapi.Geometry object is passed in (derived from BaseGeometry)
    if params.get(enums.params.geometry) and isinstance(params.get(enums.featureSet.geometry), BaseGeometry):
        geometry = params.get(enums.params.geometry)
        if not enums.geometry.type in params and hasattr(geometry, enums.geometry.type):
            params[enums.geometry.type] = getattr(geometry, enums.geometry.type)
        if not enums.params.inSR in params:
            params[enums.params.inSR] = geometry.getWKID() or geometry.getWKT()

    for pName, p in six.iteritems(params):
        if isinstance(p, dict) or hasattr(p, 'json'):
            params[pName] = json.dumps(p, ensure_ascii=False, cls=RestapiEncoder)
            
    # merge in any kwargs
    params.update(kwargs)
    if F not in params:
        params[enums.params.f] = JSON

    if not token and not proxy:
        proxy = ID_MANAGER.findProxy(service)

    if token:
        if isinstance(token, Token):
            if token.isAGOL or token.isAdmin:
                params[TOKEN] = str(token)
                cookies = None
        elif '.arcgis.com' in service:
            params[TOKEN] = str(token)
        
        # TODO: make sure token is in ID Manager registry

    stream = params.get(F) == 'image'

    if proxy:
        r = do_proxy_request(proxy, service, params, referer)
        ID_MANAGER.proxies[service.split('/rest')[0].lower() + '/rest/services'] = proxy
    else:
        r = requests.post(service, params, headers={'User-Agent': USER_AGENT}, cookies=cookies, verify=False, stream=stream)

    # make sure return
    if r.status_code != 200:
        raise NameError('"{0}" service not found!\n{1}'.format(service, r.raise_for_status()))
    else:
        if ret_json:# is True and params.get(F) in (JSON, PJSON):
            try:
                _json = r.json()
            except:
                return r
            RequestError(_json)
            return munch.munchify(_json)
        else:
            return r


def do_proxy_request(proxy, url, params={}, referer=None, ret_json=True):
    """Makes request against ArcGIS service through a proxy.  This is designed for a
            proxy page that stores access credentials in the configuration to 
            handle authentication. It is also assumed that the proxy is a standard 
            Esri proxy, i.e. retrieved from their repo on GitHub @:
            https://github.com/Esri/resourceproxy
    
    Args:
        proxy: Full url to proxy.
        url: Service url to make request against.
        params: Optional query parameters, user is responsible for passing in the
            proper parameters. Defaults to {}.
        referer: Optional referer, defaults to None.
        ret_json: Option to return as JSON, default is true
    
    Returns:
        The HTTP request.
    """

    frmat = params.get(enums.params.f, enums.params.json)
    if F in params:
        del params[enums.params.f]

    #p = '&'.join('{}={}'.format(k,v) for k,v in six.iteritems(params))

    # probably a better way to do this...
    headers = {'User-Agent': USER_AGENT}
    if referer:
        headers[enums.headers.referer] = referer
    #return requests.post('{}?{}?f={}&{}'.format(proxy, url, frmat, p).rstrip('&'), verify=False, headers=headers)
    return requests.post('{}?{}?f={}'.format(proxy, url, frmat).rstrip('&'), params, verify=False, headers=headers)

def guess_proxy_url(domain):
    """Grade school level hack to see if there is a standard esri proxy available 
            for a domain.

    Arg:
        domain: URL to domain to check for proxy.
    
    Returns:
        The proxy URL, if one is found.
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
        proxy_url = '/'.join([domain, enums.misc.proxy, enums.misc.proxy + ptype])
        r = requests.get(proxy_url)
        try:
            if r.status_code == 400 or r.content:
                return r.url
        except:
            pass
    return None

def validate_name(file_name):
    """Validates an output name by removing special characters.
    
    Arg:
        file_name: The name of the file to be validated.
    
    Returns:
        The path for the file.
    """

    import string
    path = os.sep.join(file_name.split(os.sep)[:-1]) #forward slash in name messes up os.path.split()
    name = fix_encoding(file_name.split(os.sep)[-1])
    root, ext = os.path.splitext(name)
    d = {s: '_' for s in string.punctuation}
    for f,r in six.iteritems(d):
        root = root.replace(f,r)
    return os.path.join(path, '_'.join(root.split()) + ext)

def guess_wkid(wkt):
    """Attempts to guess a well-known ID from a well-known text imput (WKT).

    Arg:
        wkt: Well known text spatial reference
    
    Returns:
        The well-known ID, if one is found.
    """

    if wkt in projections.wkt:
        return projections.wkt[wkt]
    if 'PROJCS' in wkt:
        name = wkt.split('PROJCS["')[1].split('"')[0]
    elif 'GEOGCS' in wkt:
        name = wkt.split('GEOGCS["')[1].split('"')[0]
    if name in projections.names:
        return projections.names[name]
    return 0


def assign_unique_name(fl):
    """Assigns a unique file name.

    Arg:
        fl: Path of file.
    
    Returns:
        The new, unique file name.
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
    """Date items from REST services are reported in milliseconds,
            this function will convert milliseconds to datetime objects.

    Arg:
        mil: Time in milliseconds.

    Returns:
        Datetime object.
    """

    if isinstance(mil, six.string_types):
        mil = int(mil)
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
    """Converts datetime.datetime() object to milliseconds.
    
    Arg:
        date: datetime.datetime() object
    
    Returns:
        Time in milliseconds.
    """

    if isinstance(date, datetime.datetime):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return int((date - epoch).total_seconds() * 1000.0)

def fix_encoding(s):
    """Fixes unicode by treating as ascii and ignoring errors.
    
    Arg:
        s: Unicode string.
    """

    if isinstance(s, six.string_types):
        return s.encode('ascii', 'ignore').decode('ascii')
    return s

def generate_token(url, user, pw, expiration=60, **kwargs):
    """Generates a token to handle ArcGIS Server Security, this is
            different from generating a token from the admin side. Meant
            for external use.

    Args:
        url: URL to services directory or individual map service.
        user: Username credentials for ArcGIS Server.
        pw: Password credentials for ArcGIS Server.
        expiration: Optional arg for time (in minutes) for token lifetime. 
            Max is 100. Defaults to 60.
    
    Returns:
        The token for security.
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
        infoUrl =  url.split('/rest')[0] + suffix
    # print('infoUrl is: "{}"'.format(infoUrl))
    infoResp = do_post(infoUrl)
    is_agol = False
    is_portal = enums.agol.urls.sharingRest != url and fnmatch.fnmatch(url, enums.PORTAL_BASE_PATTERN)
    host = six.moves.urllib.parse.urlparse(url).netloc 
    if AUTH_INFO in infoResp and enums.auth.tokenServicesUrl in infoResp[AUTH_INFO]:
        base = infoResp.get(enums.auth.info, {}).get(enums.auth.tokenServicesUrl)

        is_agol = enums.agol.urls.base in base
        if is_agol:
            base = enums.agol.urls.tokenService
        else:
            if not is_portal and base:
                is_portal = fnmatch.fnmatch(base, enums.PORTAL_BASE_PATTERN)

        global PROTOCOL
        PROTOCOL =  base.split('://')[0]
        print('set PROTOCOL to "{}" from generate token'.format(PROTOCOL))
        try:
            shortLived = infoResp.get(enums.auth.info, {}).get(SHORT_LIVED_TOKEN_VALIDITY) or 60
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

    # headers = {}
    if is_agol:
        if REFERER not in kwargs:
            params[REFERER] = AGOL_BASE
        else:
            params[REFERER] = kwargs.get(REFERER)
        del params[CLIENT]

    elif is_portal:
        params[CLIENT] = REFERER
        params[REFERER] = 'http'
        # headers[]
    
    elif REFERER in kwargs and kwargs.get(CLIENT) == REFERER:
        params[CLIENT] = REFERER
        params[REFERER] = kwargs.get(REFERER)

    resp = do_post(base, params)
    org_resp, portal_resp = None, None
    if is_agol:
        # now call portal sharing
        portal_params = {TOKEN: resp.get(TOKEN)}
        org_resp = do_post(AGOL_PORTAL_SELF, portal_params)
        org_referer = org_resp.get(URL_KEY, '') + ORG_MAPS
        params[REFERER]= org_referer
        resp = do_post(AGOL_TOKEN_SERVICE, params)
        resp['_' + PORTAL_INFO] = org_resp
        # print('PORTAL RESP (AGOL): ', org_resp)

    if is_portal:
        # print('url before: "{}"'.format(url))
        owningPortal = infoResp.get('owningSystemUrl')
        portalBase = owningPortal + '/sharing/rest/portals/self' if owningPortal else get_portal_base(url)
        # print('portal_base is: "{}"'.format(portalBase))
        portal_url = portalBase + '/rest/portals/self'
        # print('portal self url: "{}"'.format(portal_url))
        portal_resp = do_post(portal_url, {TOKEN: resp.get(TOKEN)})
        # print('PORTAL RESP (ENT): ', portal_resp)
        resp['_' + PORTAL_INFO] = portal_resp
        resp[DOMAIN] = get_portal_base(portalBase, root=True)
        
        # get services domain
        serversUrl = portalBase + '/servers'
        serversResp = do_post(serversUrl, { TOKEN: resp.get(TOKEN) }) 
        resp['servers'] = serversResp.get('servers')
    else:
        resp['_' + PORTAL_INFO] = {}

    if '/services/' in url:
        resp[DOMAIN] = url.split('/services/')[0] + '/services'
    elif '/admin/' in url:
        resp[DOMAIN] = url.split('/admin/')[0] + '/admin'
    else:
        if DOMAIN not in resp:
            resp[DOMAIN] = url
    resp[IS_AGOL] = is_agol
    resp[IS_PORTAL] = is_portal
    resp[IS_ADMIN] = isAdmin

    token = Token(resp)
    if is_portal:
        ID_MANAGER._portal_tokens[token.domain] = token
    else:
        ID_MANAGER.tokens[token.domain] = token

    # also register portal or org services domain
    if is_agol:
        if isinstance(org_resp, dict):
            org_id = org_resp.get('id')
            if org_id:
                # print('setting portal token: https://services2.arcgis.com/{}/arcgis/rest/services'.format(org_id))
                token_copy = munch.munchify({})
                token_copy.update(token.json) 
                serv_url =  'https://services2.arcgis.com/{}/arcgis/rest/services'.format(org_id)
                token_copy.domain = serv_url
                ID_MANAGER.tokens[serv_url] = Token(token_copy)

    if is_portal:
        if isinstance(portal_resp, dict):
            servers = resp.get('servers', []) or []
            for serv in servers:
                token_copy = munch.munchify({})
                token_copy.update(token.json)
                server_url = serv.url + '/rest/services'
                token_copy.domain = server_url
                token_copy.adminUrl = serv.adminUrl
                ID_MANAGER.tokens[server_url] = Token(token_copy)

                # check for admin url
                if serv.adminUrl:
                    admin_tok = {}
                    admin_tok.update(token_copy)
                    admin_tok['isAdmin'] = True
                    ID_MANAGER.tokens[serv.adminUrl] = Token(admin_tok)

    return token

def get_portal_base(url, root=False):
    """Gets the portal base URL."""
    if '/home' in url:
        url = url.split('/home')[0]
    if root:
        return url.split('/sharing')[0] 
    else: 
        return url if url.endswith('/sharing') else url.split('/sharing')[0] +  '/sharing'

def generate_elevated_portal_token(server_url, user_token, **kwargs):
    """Generates an elevated portal token.

    Args:
        server_url: URL for the server.
        user_token: User token.
    
    Returns:
        The elevated portal token.
    """

    params = {
        TOKEN: str(user_token) if isinstance(user_token, Token) else user_token,
        "serverURL": server_url,
        EXPIRATION: kwargs.get(EXPIRATION) or 1440,
        F: JSON,
        "request": 'getToken',
        REFERER: 'http'
    }

    # first get portal info
    portalBase = get_portal_base(server_url)
    token_url = portalBase + '/rest/generateToken'
    resp = do_post(token_url, params)
    resp['_' + PORTAL_INFO] = ID_MANAGER._portal_tokens.get(portalBase, {}).get('_' + PORTAL_INFO)

    # set domain and other token props
    if '/services/' in server_url:
        resp[DOMAIN] = server_url.split('/services/')[0] + '/services'
    elif '/admin/' in server_url:
        resp[DOMAIN] = server_url.split('/admin/')[0] + '/admin'
    else:
        resp[DOMAIN] = server_url
    resp[IS_PORTAL] = True
    resp[IS_AGOL] = False
    resp[IS_ADMIN] = False
    token = Token(resp)
    ID_MANAGER.tokens[token.domain] = token
    return token

class NameEncoder(json.JSONEncoder):
    """encoder for restapi objects to make serializeable for JSON"""
    def default(self, o):
        """Encodes object for JSON.
        
        Arg:
            o: Object.
        """
        return o.__repr__()

class JsonGetter(object):
    """Overrides getters to also check its json property."""
    json = {}

    def get(self, name, default=None):
        """Gets an attribute from json.

        Arg:
            name: Name of attribute.
        """
        return self.json.get(name, default)

    def dump(self, out_json_file, indent=2, **kwargs):
        """Dump as JSON file.
        
        Args:
            out_json_file: The path for the output json file.
            indent: Optional arg for amount to indent by in JSON. Default is 2.
        
        Returns:
            The outpath for the JSON.
        """

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
        """Dump as string."""
        if not 'cls' in kwargs:
            kwargs['cls'] = RestapiEncoder
        kwargs['ensure_ascii'] = False
        return json.dumps(self.json, **kwargs)

    def __getitem__(self, name):
        """Dict like access to json definition."""
        if name in self.json:
            return self.json[name]

    def __getattr__(self, name):
        """Gets normal class attributes and those from json response."""
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
    """Base REST Endpoint Object to handle credentials and get JSON response."""
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
        """Inits class with login info for service URL.

        Args:
            url: Service url.
        Below args only required if security is enabled:
            usr: Username credentials for ArcGIS Server.
            pw: Password credentials for ArcGIS Server.
            token: Token to handle security (alternative to usr and pw).
            proxy: Option to use proxy page to handle security, need to provide
                full path to proxy url.
        """
        
        if PROTOCOL:
            self.url = PROTOCOL + '://' + url.split('://')[-1].rstrip('/') if not url.startswith(PROTOCOL) else url.rstrip('/')
        else:
            self.url = 'http://' + url.rstrip('/') if not url.startswith('http') else url.rstrip('/')
        # print('checking url: ', url, fnmatch.fnmatch(url, PORTAL_BASE_PATTERN + '/*'))
        # if not fnmatch.fnmatch(self.url, PORTAL_SERVICES_PATTERN) and fnmatch.fnmatch(self.url, PORTAL_BASE_PATTERN + '/*'):
        #     self.url = get_portal_base(self.url) + '/rest'       
        if not fnmatch.fnmatch(self.url, BASE_PATTERN):
            if not fnmatch.fnmatch(self.url, PORTAL_BASE_PATTERN):
                print('adjusted base url')
                _plus_services = self.url + '/arcgis/rest/services'
                if fnmatch.fnmatch(_plus_services, BASE_PATTERN):
                    self.url = _plus_services
                else:
                    RequestError({'error':{'URL Error': '"{}" is an invalid ArcGIS REST Endpoint!'.format(self.url)}})
        params = {F: JSON}
        for k,v in six.iteritems(kwargs):
            params[k] = v

        # if username and password used, generate fresh token, even if one already exists
        if usr and pw:
            token = generate_token(self.url, usr, pw)
        
        # first try to find token based on domain
        tokenException = None
        if not token:
            # print('no token passed, but is there one?')
            # first check for existing token
            try:
                token = ID_MANAGER.findToken(url)
            except TokenExpired as e:
                tokenException = e
        
        # if still no token, try proxy as last ditch effort
        if not token:
            if not proxy:
                proxy = ID_MANAGER.findProxy(url)
                if not proxy and tokenException:
                    # no token or proxy available, and there is a tokenException.  Throw it now
                    raise tokenException
                
            # print('token is now: {}'.format(token))
        self.token = token
        self._cookie = None
        self._proxy = proxy
        self._referer = referer
        # if not self.token and not self._proxy:
        #     if usr and pw:
        #         self.token = generate_token(self.url, usr, pw)
        #     else:
        #         self.token = ID_MANAGER.findToken(self.url)
        #         if isinstance(self.token, Token) and self.token.isExpired:
        #             raise RuntimeError('Token expired at {}! Please sign in again.'.format(self.token.expires))
        #         elif isinstance(self.token, Token) and not self.token.isExpired:
        #             pass
        #         else:
        #             self.token = None
        # else:
        #     if isinstance(self.token, Token) and self.token.isExpired and self.token.domain in self.url.lower():
        #         raise RuntimeError('Token expired at {}! Please sign in again.'.format(self.token.expires))

        if self.token:
            if isinstance(self.token, Token) and self.token.domain.lower() in url.lower():
                self._cookie = self.token._cookie
            else:
                self._cookie = {AGS_TOKEN: self.token.token if isinstance(self.token, Token) else self.token}
        if (not self.token or not self._cookie) and not self._proxy:
            if self.url in ID_MANAGER.proxies:
                self._proxy = ID_MANAGER.proxies[self.url]

        # fetch url if this is a portal item
        # if portalId:

        # make sure token is passed in query string if agol or portal
        if isinstance(self.token, Token):
            if self.token.get(IS_AGOL) or self.token.get(IS_PORTAL):
                params[TOKEN] = str(self.token)
        self.raw_response = do_post(self.url, params, ret_json=False, token=self.token, cookies=self._cookie, proxy=self._proxy, referer=self._referer)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        self.json = munch.munchify(self.response)
        RequestError(self.json)

    def compatible_with_version(self, version):
        """Checks if ArcGIS Server version is compatible with input version. A
                service is compatible with the version if it is greater than or 
                equal to the input version.

        Arg:
            version: Minimum version compatibility as float (ex: 10.3 or 10.31).
        
        Returns:
            True if the version is compatible, False if not.
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
        """Wrapper for request to automatically pass in credentials."""
        for key, value in six.iteritems({'token': 'token',
            'cookies': '_cookie',
            'proxy': '_proxy',
            'referer': '_referer'
        }):
            if key not in kwargs:
                kwargs[key] = getattr(self, value)

        if 'ret_json' not in kwargs:
            kwargs['ret_json'] = True
        return do_post(*args, **kwargs)

    def refresh(self):
        """Refreshes the service."""
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
    """Mixin to allow convenience methods for grabbing the spatial reference from a service."""
    json = {}

    @classmethod
    def _find_wkid(cls, in_json):
        """Recursivly search for WKID in a dict/json structure.
        """
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
            elif k == CRS and isinstance(v, dict):
                try:
                    return v.get(PROPERTIES, {}).get(NAME, '').split(':')[-1]
                except:
                    return None

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
        """Gets the spatial reference dict."""
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
        elif CRS in self.json:
            resp_d = self.json.get(CRS, {})
        return munch.munchify(resp_d)

    def getSR(self):
        """Returns the spatial reference."""
        sr_dict = self._spatialReference
        sr = self._find_wkid(sr_dict)
        if sr is None:
            if isinstance(sr_dict, dict):
                return sr_dict.get(WKT)
        return sr


    def getWKID(self):
        """Returns the well known id for service spatial reference."""
        return self._find_wkid(self._spatialReference)
##        resp_d = self._spatialReference
##        for key in [LATEST_WKID, WKID]:
##            if key in resp_d:
##                return resp_d[key]

    def getWKT(self):
        """Returns the well known text (if it exists) for a service."""
        return self._spatialReference.get(WKT, '')


class FieldsMixin(object):
    json = {}

    @property
    def OIDFieldName(self):
        """Gets the OID field name if it exists in feature set."""
        if hasattr(self, OBJECTID_FIELD) and getattr(self, OBJECTID_FIELD):
            return getattr(self, OBJECTID_FIELD)

        try:
            return [f.name for f in self.fields if f.type == OID][0]
        except IndexError:
           return None

    @property
    def ShapeFieldName(self):
        """Gets the Shape field name if it exists in feature set."""
        try:
            return [f.name for f in self.fields if f.type == SHAPE][0]
        except IndexError:
           return None

    @property
    def GlobalIdFieldName(self):
        """Gets the Global ID field name if it exists in feature set."""
        if hasattr(self, GLOBALID_FIELD) and getattr(self, GLOBALID_FIELD):
            return getattr(self, GLOBALID_FIELD)

        try:
            return [f.name for f in self.fields if f.type == GLOBALID][0]
        except IndexError:
           return None

    @property
    def fieldLookup(self):
        """Convenience property for field lookups."""
        return {f.name: f for f in self.fields}

    def list_fields(self):
        """Returns a list of field names."""
        return [f.name for f in self.fields]

class FeatureSetBase(JsonGetter, SpatialReferenceMixin, FieldsMixin):
    """Base Class for feature set."""
    @property
    def hasGeometry(self):
        """Returns for if it has geometry."""
        if self.count:
            if self.features[0].get(enums.params.geometry):
                return True
        return False

    @property
    def count(self):
        """Returns total number of records in Cursor (user queried)."""
        return len(self)

    def __getitem__(self, key):
        """Supports grabbing feature by index and json keys by name."""
        try:
            if isinstance(key, int):
                return Feature(self.json.features[key])
            else:
                return Feature(self.json.get(key))
        except:
            return self.json.get(key)

    def __iter__(self):
        for feature in self.features:
            yield Feature(feature)

    def __len__(self):
        return len(self.features)

    def __bool__(self):
        return bool(len(self))

    def __dir__(self):
        return sorted(list(self.__class__.__dict__.keys()) + list(self.json.keys()))

    def __repr__(self):
        return '<{} (count: {})>'.format(self.__class__.__name__, self.count)


class FeatureSet(FeatureSetBase):
    """Class that handles feature sets."""
    _format = ESRI_JSON_FORMAT

    def __init__(self, in_json):
        """Inits Class with input JSON for feature set.

        Arg:
            in_json: Input json response from request.
        
        Raises:
            ValueError: 'Not a valid Feature Set!'
        """

        if isinstance(in_json, six.string_types):
            if not in_json.startswith('{') and os.path.isfile(in_json):
                with open(in_json, 'r') as f:
                    in_json = json.load(f)
            else:
                in_json = json.loads(in_json)
        if isinstance(in_json, self.__class__):
            self.json = in_json.json
        elif isinstance(in_json, dict):
            self.json = munch.munchify(in_json)
        if not all(map(lambda k: k in self.json.keys(), [FIELDS, FEATURES])):
            print(self.json.keys())
            raise ValueError('Not a valid Feature Set!')

    def extend(self, other):
        """Combines features from another FeatureSet with this one.

        Arg:
            other: Other FeatureSet to combine with this one.
        """
        if not isinstance(other, FeatureSet):
            other = FeatureSet(other)
        otherCopy = copy.deepcopy(other)

        # get max oid
        oidF = getattr(self, OID_FIELD_NAME) if hasattr(self, OID_FIELD_NAME) else OBJECTID
        nextOID = max([ft.get(oidF, 0) for ft in iter(self)]) + 1

        if sorted(self.list_fields()) == sorted(other.list_fields()):
            for ft in otherCopy.features:
                if ft.get(oidF) < nextOID:
                    ft.attributes[oidF] = nextOID
                    nextOID += 1
            self.features.extend(otherCopy.features)

    def getEmptyCopy(self):
        """Gets an empty copy of a feature set."""
        fsd = munch.Munch()
        for k,v in six.iteritems(self.json):
            if k == FIELDS:
                fsd[k] = [f for f in self.fields if f and not f.name.lower().startswith('shape')]
            elif k != FEATURES:
                fsd[k] = v
        fsd[FEATURES] = []
        return FeatureSet(fsd)


class GeoJSONFeatureSet(FeatureSetBase):
    """Class that handles Geo JSON feature set."""
    _format = GEOJSON_FORMAT

    def __init__(self, in_json):
        """Inits class with JSON as geo feature set.

        Arg:
            in_json: Input json response from request.
        """

        if isinstance(in_json, six.string_types):
            if not in_json.startswith('{') and os.path.isfile(in_json):
                with open(in_json, 'r') as f:
                    in_json = json.load(f)
            else:
                in_json = json.loads(in_json)
        if isinstance(in_json, self.__class__):
            self.json = in_json.json
        elif isinstance(in_json, dict):
            self.json = munch.munchify(in_json)


    def extend(other):
        """Combines features from another FeatureSet with this one.

        Arg:
            other: Other FeatureSet to combine with this one.
        """

        if not isinstance(other, GeoJSONFeatureSet):
            other = GeoJSONFeatureSet(other)
        otherCopy = copy.deepcopy(other)

        # get max oid
        oidF = getattr(self, OID_FIELD_NAME) if hasattr(self, OID_FIELD_NAME) else OBJECTID
        nextOID = max([ft.get(oidF, 0) for ft in iter(self)]) + 1

        if sorted(self.list_fields()) == sorted(other.list_fields()):
            for ft in otherCopy.features:
                if ft.get(oidF) < nextOID:
                    ft.properties[oidF] = nextOID
                    nextOID += 1
            self.features.extend(otherCopy.features)

    def getEmptyCopy(self):
        """Gets an empty copy of a feature set."""
        fsd = munch.Munch()
        for k,v in six.iteritems(self.json):
            if k != FEATURES:
                fsd[k] = v
        fsd[FEATURES] = []
        return GeoJSONFeatureSet(fsd)


class Feature(JsonGetter):
    """Class that represents a single feature."""
    def __init__(self, feature):
        """Inits the class with a feature.

        Arg:
            feature: Input json for feature.
        """
        
        self.json = munch.munchify(feature)
        self._propsGetter = ATTRIBUTES if ATTRIBUTES in self.json else PROPERTIES
        self._type = GEOJSON if self._propsGetter == PROPERTIES else ESRI_JSON_FORMAT

    def get(self, field, default=None):
        """Returns/gets an attribute from the feature.

        Arg:
            field: Name of field for which to get attribute.
        """

        if field in (self._propsGetter, enums.params.geometry):
            return self.json.get(field, default)
        return self.json.get(self._propsGetter, {}).get(field, default)

    def __repr__(self):
        return self.dumps(indent=2)

    def __str__(self):
        return self.__repr__()

class RelatedRecords(JsonGetter, SpatialReferenceMixin):
    """Class that handles related records response.
    
    Attributes:
        json: JSON object
        geometryType: Type of geometry form JSON.
        spatialReference: Spatial reference from JSON.
    """
    def __init__(self, in_json):
        """Inits class with json for query related records.

        Arg:
            in_json: json response for query related records operation.
        """
        
        self.json = munch.munchify(in_json)
        self.geometryType = self.json.get(enums.geometry.type)
        self.spatialReference = self.json.get(SPATIAL_REFERENCE)

    def list_related_OIDs(self):
        """Returns a list of all related object IDs."""
        return [f.get('objectId') for f in iter(self)]

    def get_related_records(self, oid):
        """Gets the related records for an object id.

        Arg:
            oid: Object ID for related records.
        
        Returns:
            The related records.
        """
        for group in iter(self):
            if oid == group.get('objectId'):
                return [Feature(f) for f in group[RELATED_RECORDS]]

    def toFeatureSet(self):
        """Converts to feature set."""
        features = []
        for group in iter(self):
            features.extend(group[RELATED_RECORDS])
        return FeatureSet({FIELDS: self.json.fields, FEATURES: features})

    def __iter__(self):
        for group in self.json[RELATED_RECORD_GROUPS]:
            yield group

class BaseService(RESTEndpoint, SpatialReferenceMixin):
    """Base class for all services."""
    def __init__(self, url, usr='', pw='', token='', proxy=None, referer=None, **kwargs):
        """Inits class with login info for service.

        Args:
            url: URL of service.
            usr: Username for service. Defaults to ''.
            pw: Password for service. Defaults to ''.
            token: Token for service. Defaults to ''.
            proxy: Optional proxy for service. Defaults to None.
            referer: Optional referer from request, defaults to None.
        """
        
        super(BaseService, self).__init__(url, usr, pw, token, proxy, referer, **kwargs)
        if NAME not in self.json:
            self.name = self.url.split('/')[-2]
        self.name = self.name.split('/')[-1]

    def __repr__(self):
        """String representation with service name."""
        try:
            qualified_name = '/'.join(filter(None, [self.url.split('/services/')[-1].split('/' + self.name)[0], self.name]))
        except:
            qualified_name = self.name
        return '<{}: {}>'.format(self.__class__.__name__, qualified_name)

class OrderedDict2(OrderedDict):
    """Wrapper for OrderedDict."""

    def __repr__(self):
        """We want it to look like a dictionary."""
        return json.dumps(self, indent=2, ensure_ascii=False)

class PortalInfo(JsonGetter):
    """Class that handles portal info."""
    def __init__(self, response):
        """Inits class with response from server.
        
        Arg:
            response: The response from server.
        """
        
        self.json = response
        #super(PortalInfo, self).__init__(response)
        super(JsonGetter, self).__init__()
   

    @property
    def username(self):
        return self.json.get(USER, {}).get(USER_NAME)

    @property
    def fullName(self):
        return self.json.get(USER, {}).get(FULL_NAME)

    @property
    def domain(self):
        if self.json.get(URL_KEY):
            return (self.json.get(URL_KEY, '') + ORG_MAPS).lower()
        else: 
            return self.json.get('portalLocalHostname')

    @property
    def org(self):
        if self.json.get(URL_KEY):
            return self.json.get(URL_KEY)
        else:
            return self.json.get(NAME)

    def __repr__(self):
        return '<PortaInfo: {}>'.format(self.domain)

class Token(JsonGetter):
    """Class to handle token authentication."""
    _portal = None
    def __init__(self, response):
        """Response JSON object from generate_token."""
        self.json = munch.munchify(response)
        super(JsonGetter, self).__init__()
        self._cookie = {AGS_TOKEN: self.token}
        self._portal = self.json.get('_{}'.format(PORTAL_INFO))
        if '_portalInfo' in self.json:
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
        """Boolean value for expired or not."""
        if datetime.datetime.now() > self.time_expires:
            return True
        else:
            return False

    def __str__(self):
        """Returns token as string representation."""
        return self.token

class RequestError(object):
    """Class to handle restapi request errors."""
    def __init__(self, err):
        if 'error' in err:
            raise RuntimeError(json.dumps(err, indent=2, ensure_ascii=False))

class Folder(RESTEndpoint):
    """Class to handle ArcGIS REST Folder."""

    @property
    def name(self):
        """Returns the folder name."""
        return self.url.split('/')[-1]

    def list_services(self):
        """Method to list services."""
        return ['/'.join([s.name, s.type]) for s in self.services]

    def __len__(self):
        """Returns number of services in folder."""
        return len(self.services)

    def __bool__(self):
        """Returns True if services are present."""
        return bool(len(self))

    def __iter__(self):
        for s in self.list_services():
            yield s

class GPJob(JsonGetter):
    """Represents a Geoproccesing Job"""
    def __init__(self, jobInfo):
        self.json = munch.munchify(jobInfo)

    @property 
    def status(self):
        # shorthand for jobStatus
        return self.json.get(JOB_STATUS)

    def __repr__(self):
        return '<GeoprocessingJob "{}" - status: {}>'.format(self.get(JOB_ID), self.status)

class GPResult(JsonGetter):
    """Class to handle GP Result"""
    def __init__(self, result):
        """represents a GPResult object
        
        Args:
            result (dict): result from a GPTask
        """
        # Cast to FeatureSet if recorset 
        if result.get(DATA_TYPE) == GP_RECORDSET_LAYER:
            result[VALUE] = FeatureSet(result.get(VALUE))
        self.json = munch.munchify(result)

    def __repr__(self):
        return '<GPResult "{}">'.format(self.get(PARAM_NAME, 'Unknown'))  

class GPTaskError(JsonGetter):
    def __init__(self, error):
        self.json = munch.munchify(error)
        self.showWarning()

    def showWarning(self):
        if ERROR in self.json:
            warnings.warn('GP Task Failed:\n{}'.format('\n\t'.join(self.json.error.get(DETAILS, []))))

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.json.get(ERROR, {}).get(MESSAGE))

class GPTaskResponse(JsonGetter):
    """Class to handle GP Task Response."""
    def __init__(self, response):
        """Handler for GP Task Response.

        response: JSON response from GP Task execution.
        """
        self._values = {}
        self.json = munch.munchify(response)

        # get values cache
        if isinstance(self.results, dict):
            for key in self.results.keys():
                self.getValue(key)

        elif isinstance(self.results, list):
            for res in self.results:
                self.getValue(res.get(PARAM_NAME))

    def getValue(self, paramName=None):
        """Gets a result value by param name
        
        Args:
            paramName (str, optional): The Parameter Name, if none supplied the first parameter found will be returned. Defaults to None.
        
        Returns:
            [any]: the return value
        """
        result = None
        if self.results:
            if isinstance(self.results, dict):
                if paramName not in self.results:
                    # get first value
                    paramName = list(self.results.keys())[0]

                if paramName in self._values:
                    return self._values[paramName]

                if self.isAsync:
                    url = '/'.join([self.jobUrl, self.results.get(paramName).get(PARAM_URL)])
                    result = GPResult(do_post(url, { F: JSON })).value
            
            elif isinstance(self.results, list):
                if not paramName:
                    paramName = self.results[0].paramName

                if paramName in self._values:
                    return self._values[paramName]

                result = GPResult([r for r in self.results if paramName == r.paramName][0]).value

        if result:
            self._values[paramName] = result
    
        return result

    def print_messages(self):
        """Prints all the GP messages."""
        for msg in self.messages:
            print('Message Type: {}'.format(msg.type))
            print('\tDescription: {}\n'.format(msg.description))

    def __len__(self):
        """Returns length of results."""
        return len(self.results)

    def __getitem__(self, i):
        """Returns result at index, usually will only be 1."""
        return list(self.results.values())[i]

    def __bool__(self):
        """Returns True if results."""
        return bool(len(self))

    def __repr__(self):
        jobId = self.json.get(JOB_ID)
        if jobId:
            return '<{} [{}] ("{}")>'.format(self.__class__.__name__, self.json.get(JOB_STATUS), jobId)
        return '<{}>'.format(self.__class__.__name__)

class GeocodeResult(JsonGetter, SpatialReferenceMixin):
    """Class to handle Reverse Geocode Result."""
    def __init__(self, res_dict, geo_type):
        """Geocode response object.

        Args:
            res_dict: JSON response from geocode request
            geo_type: Type of geocode operation 
                (reverseGeocode|findAddressCandidates|geocodeAddresses).
        """
        RequestError(res_dict)
        super(GeocodeResult, self).__init__()
        self.json = res_dict
        self.type = 'esri_' + geo_type

    @property
    def results(self):
        """Returns list of result objects."""
        if self.type == 'esri_findAddressCandidates':
            return self.candidates
        elif self.type == 'esri_reverseGeocode':
            return [self.address]
        else:
            return self.json.get(LOCATIONS, [])

    @property
    def result(self):
        """Returns the top result."""
        try:
            return self.results[0]
        except IndexError:
            return None

    def __getitem__(self, index):
        """Allows for indexing of results."""
        return self.results[index]

    def __len__(self):
        """Returns count of results."""
        return len(self.results)

    def __iter__(self):
        """Returns an iterator for results (as generator)."""
        for r in self.results:
            yield r

    def __bool__(self):
        """Returns True if results are returned."""
        return bool(len(self))

    def __repr__(self):
        return '<{}: {} match{}>'.format(self.__class__.__name__, len(self), 'es' if len(self) else '')

class EditResult(JsonGetter):
    """Class to handle Edit operation results."""
    def __init__(self, res_dict, feature_id=None):
        """Inits class with response
        
        Args:
            res_dict: Dictionary of response. 
        """
        RequestError(res_dict)
        self.json = munch.munchify(res_dict)

    @staticmethod
    def success_count(l):
        """Returns number of successful attempts."""
        return len([d for d in l if d.get(SUCCESS_STATUS) in (True, TRUE)])

    def summary(self):
        """Prints summary of edit operation."""
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
    """Base geometry obect."""

    def dumps(self, **kwargs):
        """Returns JSON as a string."""
        if 'ensure_ascii' not in kwargs:
            kwargs['ensure_ascii'] = False
        return json.dumps(self.json, **kwargs)


class BaseGeometryCollection(SpatialReferenceMixin):
    """Base Geometry Collection."""
    geometries = []
    json = {GEOMETRIES: []}
    geometryType = NULL

    @property
    def count(self):
        return len(self)

    def dumps(self, **kwargs):
        """Returns JSON as a string."""
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
    """Class to handle Geocode Service."""

    def geocodeAddresses(self, recs, outSR=4326, address_field=''):
        """Geocodes a list of addresses.  If there is a singleLineAddress field 
                present in the geocoding service, the only input required is a 
                list of addresses.  Otherwise, a record set an be passed in for 
                the "recs" parameter.  See formatting example at bottom.
        
        Args:
            recs: JSON object for fields as record set if no SingleLine field 
                available. If singleLineAddress is present a list of full addresses 
                can be passed in.
            outSR: Optional output spatial refrence for geocoded addresses.
            address_field: Name of address field or Single Line address field.
        
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

        Raises:
            ValueError: 'Not a valid input for "recs" parameter!'

        Returns:
            The geocode result.
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
        """Reverse geocodes an address by x, y coordinates.
        
        Args:
            location: Input point object as JSON
            distance: Distance in meters from given location which a matching 
                address will be found. Default is 100.
            outSR: WKID for output address. Default is 4326.
            langCode: Optional language code, default is eng 
                (only used for StreMap Premium locators).
            returnIntersection: Optional boolean, if True, will return an 
                intersection. Defaults to False.
        """

        geo_url = self.url + '/reverseGeocode'
        params = {LOCATION: location,
                  DISTANCE: distance,
                  OUT_SR: outSR,
                  RETURN_INTERSECTION: returnIntersection,
                  F: JSON}

        return GeocodeResult(self.request(geo_url, params), geo_url.split('/')[-1])

    def findAddressCandidates(self, address='', outSR=4326, outFields='*', returnIntersection=False, **kwargs):
        """Finds address candidates for an anddress.

        Args:
            address: Full address (380 New York Street, Redlands, CA 92373).
            outFields: List of fields for output. Default is * for all fields. Will
                accept either list of fields [], or comma separated string.
            outSR: wkid for output address. Defaults to 4326.
            **kwargs: key word arguments to use for Address, City, State, etc 
                fields if no SingleLine field.
            returnIntersection: Optional boolean, if True, will return an 
                intersection. Defaults to False.
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
        """String representation with service name."""
        return '<GeocodeService: {}>'.format('/'.join(self.url.split('/services/')[-1].split('/')[:-1]))
