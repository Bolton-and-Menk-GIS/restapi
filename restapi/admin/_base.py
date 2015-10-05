# WARNING: much of this module is untested, this module makes permanant server configurations.
# Use with caution!
from __future__ import print_function
import sys
import os
import fnmatch
import datetime
import json
import pprint
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import requests
from rest_utils import Token, mil_to_date, date_to_mil, namedTuple, RequestError, objectize
from collections import namedtuple
from dateutil.relativedelta import relativedelta
from decorator import decorator

# Globals
RESTAPI_TOKEN = None
BASE_PATTERN = '*:*/arcgis/admin*'
VERBOSE = True

# VERBOSE is set to true by default, this will echo the status of all operations
#  i.e. reporting an administrative change was successful.  To turn this off, simply
#  change VERBOSE to False.  This can be done like this:
#    VERBOSE = False #because you get this with importing the admin module
#  or:
#    restapi.admin.VERBOSE = False

@decorator
def passthrough(f, *args, **kwargs):
    """decorator to print results of function/method and returns json object

    set the global VERBOSE property to false if you do not want results of
    operations to be echoed during session

    Example to disable print messages:
        restapi.admin.VERBOSE = False  # turns off verbosity
    """
    o = f(*args, **kwargs)
    if isinstance(o, dict) and  VERBOSE is True:
        for k,v in o.iteritems():
            print('{}: {}'.format(k,v))

    return o

def POST(service, params={'f': 'json'}, token='', ret_json=True):
    """Post Request to REST Endpoint through query string, to post
    request with data in body, use requests.post(url, data={k : v}).

    Required:
    service -- full path to REST endpoint of service

    Optional:
    _params -- parameters for posting a request
    token -- token to handle security (only required if security is enabled)
    ret_json -- return the response as JSON.  Default is True.
    """
    for pName, p in params.iteritems():
        if isinstance(p, dict):
            params[pName] = json.dumps(p)

    if not token and RESTAPI_TOKEN and not RESTAPI_TOKEN.isExpired:
        token = RESTAPI_TOKEN
    if token:
        if isinstance(token, Token) and token.isExpired:
            raise RuntimeError('Token expired at {}! Please sign in again.'.format(token.expires))
        params['token'] = token.token if isinstance(token, Token) else token

    if not 'f' in params:
        params['f'] = 'json'

    r = requests.post(service, params, verify=False)

    # make sure return
    if r.status_code != 200:
        raise NameError('"{0}" service not found!\n{1}'.format(service, r.raise_for_status()))
    else:
        RequestError(r.json())
        if ret_json:
            return r.json()
        else:
            return r

def generate_token(server='', usr='', pw='', expiration=60):
    """generates a token for adminstrative functions

    Required:
        server -- domain name
        usr -- username
        pw -- password

    Optional:
        port -- port number. Default is 6080
        expiration -- expiration for token (in minutes)
    """
    server = server.lower().split('/arcgis')[0]
    if not server.startswith('http'):
        server = 'http://' + server

    params = {'username': usr,
              'password': pw,
              'expiration': expiration,
              'client': 'requestip'}

    url = server + '/arcgis/admin/generateToken'.format(server)
    resp = POST(url, params)
    resp['domain'] = url.split('/generateToken')[0]
    token = Token(resp)
    setattr(sys.modules[__name__], 'RESTAPI_TOKEN', token)
    return token

class AdminRESTEndpoint(object):
    """Base REST Endpoint Object to handle credentials and get JSON response

    Required:
        url -- image service url

    Optional (below params only required if security is enabled):
        usr -- username credentials for ArcGIS Server
        pw -- password credentials for ArcGIS Server
        token -- token to handle security (alternative to usr and pw)

    Note:
        If using Microsft ARR (Application Request Routing) as a load balancer for services,you
        may need to append an 'ARRAffinity' cookie to the self._cookie or the self.token attribute.
    """
    def __init__(self, url, usr='', pw='', token=''):
        self.url = 'http://' + url.rstrip('/') if not url.startswith('http') and 'localhost' not in url.lower() else url.rstrip('/')
        if not fnmatch.fnmatch(self.url, BASE_PATTERN):
            _fixer = self.url.split('/arcgis')[0] + '/arcgis/admin'
            if fnmatch.fnmatch(_fixer, BASE_PATTERN):
                self.url = _fixer
            else:
                RequestError({'error':{'URL Error': '"{}" is an invalid ArcGIS REST Endpoint!'.format(self.url)}})
        params = {'f': 'json'}
        self.token = token
        if not self.token:
            if usr and pw:
                self.token = generate_token(self.url, usr, pw)
            else:
                if RESTAPI_TOKEN and not RESTAPI_TOKEN.isExpired and self.token.domain.lower() in url.lower():
                    self.token = RESTAPI_TOKEN
                elif RESTAPI_TOKEN and RESTAPI_TOKEN.isExpired:
                    raise RuntimeError('Token expired at {}! Please sign in again.'.format(token.expires))

        else:
            if isinstance(token, Token) and token.isExpired:
                raise RuntimeError('Token expired at {}! Please sign in again.'.format(token.expires))

        if self.token:
            if isinstance(self.token, Token) and self.token.domain.lower() in url.lower():
                params['token'] = self.token.token
        elif isinstance(self.token, basestring):
            params['token'] = self.token

        self.raw_response = requests.post(self.url, params, verify=False)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        if 'error' in self.response:
            self.print_info()

    def asJSON(self):
        """return actual server response as JSON"""
        return self.response

    def print_info(self):
        """Method to print all properties of service"""
        _print_info(self)

    def refresh(self):
        """refreshes the service properties"""
        self.__init__(self.url, token=self.token)

class BaseDirectory(AdminRESTEndpoint):
    """base class to handle objects in service directory"""
    def __init__(self, url, usr='', pw='', token=''):
        super(BaseDirectory, self).__init__(url, usr, pw, token)
        self._permissionsURL = self.url + '/permissions'

    @property
    def permissions(self):
        """return permissions for service"""
        query_url = self.url + '/permissions'

        perms = POST(query_url, token=self.token)['permissions']
        return [Permission(r) for r in perms]

    @passthrough
    def addPermission(self, principal='', isAllowed=True, private=True):
        """add a permission

        Optional:
            principal -- name of the role whome the permission is being assigned
            isAllowed -- tells if a resource is allowed or denied
            private -- default is True.  Secures service by making private, denies
                public access.  Change to False to allow public access.
        """
        add_url = self._permissionsURL + '/add'
        if principal:
            params = {'principal': principal, 'isAllowed': isAllowed}
            r = POST(add_url, params, token=self.token)

        params = {'principal': 'esriEveryone', 'isAllowed': 'false'}

        if private:
            esriEveryone = POST(add_url, params, token=self.token)
        else:
            params['isAllowed'] = 'true'
            esriEveryone = POST(add_url, params, token=self.token)

        if not principal:
            r = esriEveryone
        return r

    @passthrough
    def hasChildPermissionsConflict(self, principal, permission=None):
        """check if service has conflicts with opposing permissions

        Required:
            principal -- name of role for which to check for permission conflicts

        Optional:
            permission -- JSON permission object

        permission example:
            permission = {"isAllowed": True, "constraint": ""}
        """
        if not permission:
            permission = {"isAllowed": True, "constraint": ""}

        query_url = self.url + '/permissions/hasChildPermissionConflict'
        params = {'principal': principal, 'permission': permission}
        return POST(query_url, params, token=self.token)

class Report(object):
    """Report Object"""
    def __init__(self, kwargs):
        """kwargs = JSON report response"""
        for k,v in kwargs.iteritems():
            setattr(self, k, v)
        objectize(self, '_json')
        self._json = kwargs

    def asJSON(self):
        """get Report back to JSON"""
        return self._json

class ClusterMachine(object):
    """Machine object"""
    def __init__(self, **kwargs):
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

        self._json = kwargs

    def asJSON(self):
        """get machine back as JSON"""
        return self._json

class SSLCertificate(AdminRESTEndpoint):
    """class to handle SSL Certificate"""
    def __init__(self, url, usr='', pw='', token=''):
        super(Machine, self).__init__(url, usr, pw, token)

        for k,v in self.response:
            setattr(self, k, v)

def Machine(AdminRESTEndpoint):
    """class to handle ArcGIS Server Machine"""
    def __init__(self, url, usr='', pw='', token=''):
        super(Machine, self).__init__(url, usr, pw, token)

        for k,v in self.response:
            setattr(self, k, v)

class DataItem(object):
    """Data Item object"""
    def __init__(self, kwargs):
        """kwargs = JSON report response"""
        for k,v in kwargs.iteritems():
            setattr(self, k, v)
        objectize(self, '_json')
        self._json = kwargs

    @passthrough
    def makePrimary(self, machineName):
        """promotes a standby machine to the primary data store machine. The
        existing primary machine is downgraded to a standby machine

        Required:
            machineName -- name of machine to make primary
        """
        query_url = self.url + '/machines/{}/makePrimary'.format(machineName)
        return POST(query_url, token=self.token)

    def validateDataStore(self, machineName):
        """ensures that the data store is valid

        Required:
            machineName -- name of machine to validate data store against
        """
        query_url = self.url + '/machines/{}/validate'.format(machineName)
        return POST(query_url, token=self.token)

    def asJSON(self):
        """get Report back to JSON"""
        return {k:v for k,v in self._json.iteritems() if k != 'url'}

class Permission(object):
    """Permission Object"""
    def __init__(self, kwargs):
        for k,v in kwargs.iteritems():
            setattr(self, k, v)
        objectize(self)
        self._json = kwargs

    def asJSON(self):
        """get object back to JSON"""
        return self._json

class Item(AdminRESTEndpoint):
    """ This resource represents an item that has been uploaded to the server. Various
    workflows upload items and then process them on the server. For example, when
    publishing a GIS service from ArcGIS for Desktop or ArcGIS Server Manager, the
    application first uploads the service definition (.SD) to the server and then
    invokes the publishing geoprocessing tool to publish the service.

    Each uploaded item is identified by a unique name (itemID). The pathOnServer
    property locates the specific item in the ArcGIS Server system directory.

    The committed parameter is set to true once the upload of individual parts is complete.
     """
    def __init__(self, url, usr='', pw='', token=''):
        super(Item, self).__init__(url, usr, pw, token)
        pass

class PrimarySiteAdministrator(AdminRESTEndpoint):
    """Primary Site Administrator object"""
    def __init__(self, url, usr='', pw='', token=''):
        super(PrimarySiteAdministrator, self).__init__(url, usr, pw, token)
        self.disabled = ''
        if 'disabled' in self.response:
            self.disabled = self.response['disabled']

        # in case esri add future params
        for k,v in self.response.iteritems():
            if k != 'disabled':
                setattr(self, k, v)

    @passthrough
    def disable(self):
        """disables the primary site administartor account"""
        query_url = self.url + '/disable'
        return POST(query_url, token=self.token)

    @passthrough
    def enable(self):
        """enables the primary site administartor account"""
        query_url = self.url + '/enable'
        return POST(query_url, token=self.token)

    @passthrough
    def update(self, username, password):
        """updates the primary site administrator account


        Required:
            username -- new username for PSA (optional in REST API, required here
                for your protection)
            password -- new password for PSA
        """
        query_url = self.url + '/update'

        params = {'username': username,
                  'password': password}

        return POST(query_url, params, token=self.token)

    def __bool__(self):
        """returns True if PSA is enabled"""
        return not self.disabled

class RoleStore(object):
    """Role Store object"""
    def __init__(self, url, usr='', pw='', token=''):
        super(UserStore, self).__init__(url, usr, pw, token)
        for k,v in self.response.iteritems():
            setattr(self, k, v)

        self.role_cache = POST(self.url + '/search', token=self.token)['roles']
        self.roles = [namedTuple('Role', r) for r in self.role_cache]

    @passthrough
    def addRole(self, rolename, description=''):
        """adds a role to the role store

        Required:
            rolename -- name of role to add

        Optional:
            description -- optional description for new role
        """
        query_url = self.url + '/add'
        params = {'rolename': rolename,
                  'description': description}

        return POST(query_url, params, token=self.token)

    def getRoles(self, startIndex='', pageSize=''):
        """This operation gives you a pageable view of roles in the role store. It is intended
        for iterating through all available role accounts. To search for specific role accounts
        instead, use the searchRoles() method. <- from Esri help

        Optional:
            startIndex -- zero-based starting index from roles list.
            pageSize -- maximum number of roles to return.
        """
        query_url = self.url + '/getRoles'

        params = {'startIndex': startIndex,
                  'pageSize': pageSize}

        r = POST(query_url, params, token=self.token)['roles']
        return [namedTuple('Role', r) for r in self.role_cache]

    def searchRoles(self, filter='', maxCount=''):
        """search the role store

        Optional:
            filter -- filter string for roles (ex: "editors")
            maxCount -- maximimum number of records to return
        """
        query_url = self.url + '/search'

        params = {'filter': filter,
                  'maxCount': maxCount}

        r = POST(query_url, params, token=self.token)['roles']
        return [namedTuple('Role', r) for r in self.role_cache]

    @passthrough
    def removeRole(self, rolename):
        """removes a role from the role store

        Required:
            rolename -- name of role
        """
        query_url = self.url + '/remove'
        return POST(query_url, {'rolename':rolename}, token=self.token)

    @passthrough
    def updateRole(self, rolename, description=''):
        """updates a role

        Required:
            rolename -- name of the role

        Optional:
            description -- descriptoin of role
        """
        query_url = self.url + '/update'

        params = {'rolename': rolename,
                  'description': description}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getRolesForUser(self, username, filter='', maxCount=10):
        """returns the privilege associated with a user

        Required:
            privilege -- name of privilege (ADMINISTER | PUBLISH)
        """
        query_url = self.url + '/getRolesForUser'
        params = {'username': username,
                  'filter': filter,
                  'maxCount': maxCount}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getUsersWithinRole(self, rolename, filter='', maxCount=10):
        """get all user accounts to whom this role has been assigned

        Required:
            rolename -- name of role

        Optional:
            filter -- optional filter to be applied to the resultant user set
            maxCount -- maximum number of results to return
        """
        query_url = self.url + '/getUsersWithinRole'
        params = {'rolename': username,
                  'filter': filter,
                  'maxCount': maxCount}

        return POST(query_url, params, token=self.token)

    @passthrough
    def addUsersToRole(self, rolename, users):
        """assign a role to multiple users with a single action

        Required:
            rolename -- name of role
            users -- list of users or comma separated list
        """
        query_url = self.url + '/addUsersToRole'

        if isinstance(users, (list, tuple)):
            users = ','.join(map(str, users))

        params = {'rolename': rolename,
                  'users': users}

        return POST(query_url, params, token=self.token)

    @passthrough
    def removeUsersFromRole(self, rolename, users):
        """removes a role assignment from multiple users.

        Required:
            rolename -- name of role
            users -- list or comma separated list of user names
        """
        query_url = self.url + '/removeUsersFromRole'

        if isinstance(users, (list, tuple)):
            users = ','.join(map(str, users))

        params = {'rolename': rolename,
                  'users': users}

        return _POST(query_url, params, token=self.token)

    @passthrough
    def assignPrivilege(self, rolename, privilege='ACCESS'):
        """assign administrative acess to ArcGIS Server

        Required:
            rolename -- name of role
            privilege -- administrative capability to assign (ADMINISTER | PUBLISH | ACCESS)
        """
        query_url -- self.url + '/assignPrivilege'

        params = {'rolename': rolename,
                  'privilege': privilege.upper()}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getPrivilegeForRole(self, rolename):
        """gets the privilege associated with a role

        Required:
            rolename -- name of role
        """
        query_url = self.url + '/getPrivilege'
        return POST(query_url, {'rolename':rolename}, token=self.token)

    @passthrough
    def getRolesByPrivilege(self, privilege):
        """returns the privilege associated with a user

        Required:
            privilege -- name of privilege (ADMINISTER | PUBLISH)
        """
        query_url = self.url + '/getRolesByPrivilege'
        return POST(query_url, {'privilege': privilege.upper()}, token=self.token)

    def __len__(self):
        """return number of Roles"""
        return len(self.roles)

    def __nonzero__(self):
        """returns True if there are roles"""
        return bool(len(self))

    def __iter__(self):
        """make iterable"""
        for role in self.roles:
            yield role

    def __getitem__(self, i):
        """allows indexing of roles"""
        return self.roles[i]

class UserStore(AdminRESTEndpoint):
    """User Store object"""
    def __init__(self, url, usr='', pw='', token=''):
        super(UserStore, self).__init__(url, usr, pw, token)
        for k,v in self.response.iteritems():
            setattr(self, k, v)

        self.user_cache = POST(self.url + '/search', token=self.token)['users']
        self.users = [namedTuple('User', u) for u in self.user_cache]

    @passthrough
    def addUser(self, username, password, fullname='', description='', email=''):
        """adds a user account to user store

        Requred:
            username -- username for new user
            password -- password for new user

        Optional:
            fullname -- full name of user
            description -- description for user
            email -- email address for user account
        """
        query_url = self.url + '/add'
        params = {'username': username,
                  'password': password,
                  'fullname': fullname,
                  'description': description,
                  'email': email}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getUsers(self, startIndex='', pageSize=''):
        """get all users in user store, intended for iterating over all user accounts

        Optional:
            startIndex -- zero-based starting index from roles list.
            pageSize -- maximum number of roles to return.
        """
        query_url = self.url + '/getUsers'

        params = {'startIndex': startIndex,
                  'pageSize': pageSize}

        r = POST(query_url, params, token=self.token)
        return [namedTuple('User', u) for u in r['users']]

    def searchUsers(self, filter='', maxCount=''):
        """search the user store, returns User objects

        Optional:
            filter -- filter string for users (ex: "john")
            maxCount -- maximimum number of records to return
        """
        query_url = self.url + '/search'

        params = {'filter': filter,
                  'maxCount': maxCount}

        r = POST(query_url, params, token=self.token)
        return [namedTuple('User', u) for u in r['users']]


    @passthrough
    def removeUser(self, username):
        """removes a user from the user store

        Required:
            username -- name of user to remove
        """
        query_url = self.url + '/remove'
        return POST(query_url, {'username':username}, token=self.token)

    @passthrough
    def updateUser(self, username, password, fullname='', description='', email=''):
        """updates a user account in the user store

        Requred:
            username -- username for new user
            password -- password for new user

        Optional:
            fullname -- full name of user
            description -- description for user
            email -- email address for user account
        """
        query_url = self.url + '/update'
        params = {'username': username,
                  'password': password,
                  'fullname': fullname,
                  'description': description,
                  'email': email}

        return POST(query_url, params, token=self.token)

    @passthrough
    def assignRoles(self, username, roles):
        """assign role to user to inherit permissions of role

        Required:
            username -- name of user
            roles -- list or comma separated list of roles
        """
        query_url = self.url + '/assignRoles'

        if isinstance(roles, (list, tuple)):
            roles = ','.join(map(str, roles))

        params = {'username': username,
                  'roles': roles}

        return POST(query_url, params, token=self.token)

    @passthrough
    def removeRoles(self, username, rolenames):
        """removes roles that have been previously assigned to a user account, only
        supported when role store supports reads and writes

        Required:
            username -- name of the user
            roles -- list or comma separated list of role names
        """
        query_url = self.url + '/removeRoles'

        if isinstance(roles, (list, tuple)):
            roles = ','.join(map(str, roles))

        params = {'username': username,
                  'roles': roles}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getPrivilegeForUser(self, username):
        """gets the privilege associated with a role

        Required:
            username -- name of user
        """
        query_url = self.url + '/getPrivilege'
        return POST(query_url, {'username': username}, token=self.token)

    def __len__(self):
        """return number of Users"""
        return len(self.users)

    def __nonzero__(self):
        """returns True if there are users"""
        return bool(len(self))

    def __iter__(self):
        """make iterable"""
        for user in self.users:
            yield user

    def __getitem__(self, i):
        """allows indexing of users"""
        return self.users[i]

class DataStore(AdminRESTEndpoint):
    """class to handle Data Store operations"""
    def __init__(self, url, usr='', pw='', token=''):
        super(DataStore, self).__init__(url, usr, pw, token)
        for k,v in self.response.iteritems():
            setattr(self, k, v)
        self.url = self.url.split('/config')[0]
        self.items = self.getItems()

    @passthrough
    def config(self):
        """return configuratoin properties"""
        return POST(self.url + '/config', token=self.token)

    # not available in ArcGIS REST API out of the box, included here to refresh data store cache
    def getItems(self):
        """returns a refreshed list of all data items"""
        items = []
        for it in self.getRootItems():
            items += self.findItems(it)
        return items

    @passthrough
    def registerItem(self, item):
        """registers an item with the data store

        Required:
            item -- JSON representation of new data store item to register

        Example:
            item={
            	"path": "/fileShares/folder_shared", //a unique path on the server
            	"type": "folder", //as this is a file share
            	"clientPath": null, //not needed as this is a shared folder
            	"info": {
            		"path": "\\\\server\\data\\rest_data", //path to the share
            		"dataStoreConnectionType": "shared" //this is a shared folder
            		}
            	}
        """
        if self.validateItem(item):
            query_url = self.url + '/registerItem'
            return POST(query_url, params={'item': item}, token=self.token)

        return None

    @passthrough
    def unregisterItem(self, itemPath):
        """unregisters an item with the data store

        Required:
            itemPath -- path to data item to unregister (DataItem.path)
        """
        query_url = self.url + '/registerItem'
        return POST(query_url, {'itemPath': itemPath}, token=self.token)

    def findItems(self, parentPath, ancestorPath='', types='', id=''):
        """search through items registered in data store

        Required:
            parentPath -- path of parent under which to find items

        Optional:
            ancestorPath -- path of ancestor which to find items
            types -- filter for the type of items to search
            id -- filter to search the ID of the item
        """
        query_url = self.url + '/findItems'
        params = {'parentPath': parentPath,
                  'ancestorPath': ancestorPath,
                  'types': types,
                  'id': id}

        ds_items = POST(query_url, params, token=self.token)['items']
        for d in ds_items:
            d['url'] = '{}/items{}'.format(self.url, d['path'])

        return [DataItem(d) for d in ds_items]

    def validateItem(self, item):
        """validates a data store item

        Required:
            item -- JSON representation of new data store item to validate
        """
        query_url = self.url + '/validateDataItem'
        r = POST(query_url, {'item': item}, token=self.token)
        if 'status' in r and r['status'] == 'success':
            return True
        else:
            pprint.pprint(r)
            return False

    @passthrough
    def validateAllDataItems(self):
        """validates all data items in data store.  Warning, this operation can be
        VERY time consuming, depending on how many items are registered with the
        data store
        """
        return POST(self.url + '/validateAllDataItems', token=self.token)

    def computeRefCount(self, path):
        """get the total number of references to a given data item that exists on
        the server.  Can be used to determine if a data resource can be safely
        deleted or taken down for maintenance.

        Required:
            path -- path to resource on server (DataItem.path)
        """
        query_url = self.url + '/computeTotalRefCount'
        r  = passthrough(POST(query_url, {'path': path}, token=self.token))
        return int(r['totalRefCount'])

    def getRootItems(self):
        """method to get all data store items at the root"""
        return POST(self.url + '/items', token=self.token)['rootItems']

    @passthrough
    def startMachine(self, dataItem, machineName):
        """starts the database instance running on the data store machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self.url + '/items/{}/machines/{}/start'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def stopMachine(self, dataItem, machineName):
        """starts the database instance running on the data store machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self.url + '/items/{}/machines/{}/stop'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def removeMachine(self, dataItem, machineName):
        """removes a standby machine from the data store, this operation is not
        supported on the primary data store machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self.url + '/items/{}/machines/{}/remove'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def makePrimary(self, dataItem, machineName):
        """promotes a standby machine to the primary data store machine. The
        existing primary machine is downgraded to a standby machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to make primary
        """
        query_url = self.url + '/items/{}/machines/{}/makePrimary'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    def validateDataStore(self, dataItem, machineName):
        """ensures that the data store is valid

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self.url + '/items/{}/machines/{}/validate'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def updateDatastoreConfig(self, datastoreConfig={}):
        """update data store configuration.  Can use this to allow or block
        automatic copying of data to server at publish time

        Optional:
            datastoreConfig -- JSON object representing datastoreConfiguration.  if none
                supplied, it will default to disabling copying data locally to the server.
        """
        query_url = self.url + '/config/update'
        if not datastoreConfig:
            datastoreConfig = '{"blockDataCopy":"true"}'
        return POST(query_url, {'datastoreConfig': datastoreConfig}, token=self.token)

    def __len__(self):
        """return number of items"""
        return len(self.items)

    def __nonzero__(self):
        """returns True if there are roles"""
        return bool(len(self))

    def __iter__(self):
        """make iterable"""
        for item in self.items:
            yield item

    def __getitem__(self, i):
        """allows indexing of roles"""
        return self.items[i]

class Cluster(AdminRESTEndpoint):
    """class to handle Cluster object"""
    def __init__(self, url, usr='', pw='', token=''):
        super(Cluster, self).__init__(url, usr, pw, token)

        for k,v in self.response.iteritems():
            setattr(self, k, v)

    @property
    def machines(self):
        """list all server machines participating in the cluster"""
        return [Machine(**r) for r in POST(self.url + '/machines', token=self.token)]

    @property
    def services(self):
        """get a list of all services in the cluster"""
        return [namedTuple('ServiceProperties', r) for r in POST(self.url + '/services', token=self.token)['services']]

    @passthrough
    def start(self):
        """starts the cluster"""
        return POST(self.url + '/start', token=self.token)

    @passthrough
    def stop(self):
        """stops the cluster"""
        return POST(self.url + '/stop', token=self.token)

    @passthrough
    def delete(self):
        """deletes the cluster configuration.  All machines in cluster will be stopped
        and returened to pool of registered machines.  All GIS services in cluster are
        stopped
        """
        return POST(self.url + '/delete', token=self.token)

    @passthrough
    def editProtocol(self, clusterProtocol):
        """edits the cluster protocol.  Will restart the cluster with updated protocol.
         The clustering protocol defines a channel which is used by server machines within
         a cluster to communicate with each other. A server machine will communicate with
         its peers information about the status of objects running within it for load
         balancing and default tolerance.

        ArcGIS Server supports the TCP clustering protocols where server machines communicate
        with each other over a TCP channel (port).

        Required:
            clusterProtocol -- JSON object representing the cluster protocol TCP port

        Example:
            clusterProtocol = {"tcpClusterPort":"4014"}
        """
        query_url = self.url + '/editProtocol'
        params = {'clusterProtocol': clusterProtocol}

        return POST(query_url, params, token=self.token)

    @passthrough
    def addMachines(self, machineNames):
        """add machines to cluster.  Machines need to be registered with the site
        before they can be added.

        Required:
            machineNames -- list or comma-separated list of machine names

        Examples:
            machineNames= "SERVER2.DOMAIN.COM,SERVER3.DOMAIN.COM"
        """
        query_url = self.url + '/machines/add'
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return POST(query_url, {'machineNames': machineNames}, token=self.token)

    @passthrough
    def removeMachines(self, machineNames):
        """remove machine names from cluster

        Required:
            machineNames -- list or comma-separated list of machine names

        Examples:
            machineNames= "SERVER2.DOMAIN.COM,SERVER3.DOMAIN.COM"
        """
        query_url = self.url + '/machines/remove'
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return POST(query_url, {'machineNames': machineNames}, token=self.token)


class Folder(BaseDirectory):
    """class to handle simple folder objects"""
    def __init__(self, url, usr='', pw='', token=''):
        super(Folder, self).__init__(url, usr, pw, token)
        for k,v in self.response.iteritems():
            setattr(self, k, v)
        objectize(self)

    def __str__(self):
        """folder name"""
        return self.folderName

    def list_services(self):
        """list services within folder"""
        return ['.'.join([s.serviceName, s.type]) for s in self.services]

    def iter_services(self):
        """iterate through Service Objects"""
        for service in self.services:
            serviceUrl = '.'.join(['/'.join([self.url, service.serviceName]), service.type])
            yield Service(serviceUrl, token=self.token)

    @passthrough
    def delete(self):
        """deletes the folder"""
        query_url = self.url + '/deleteFolder'
        return POST(query_url, token=self.token)

    @passthrough
    def edit(self, description, webEncrypted):
        """edit a folder

        Required:
            description -- folder description
            webEncrypted -- boolean to indicate if the servies are accessible over SSL only.
        """
        query_url = self.url + '/editFolder'
        params = {'description': description, 'webEncrypted': webEncrypted}
        return POST(query_url, params, token=self.token)

    def report(self):
        """return a list of service report object"""

        reps = POST(self.url + '/report', token=self.token)['reports']
        return [Report(rep) for rep in reps]

    def __len__(self):
        """return number of services in folder"""
        return len(self.services)

    def __nonzero__(self):
        """return True if services are present"""
        return bool(len(self))

class Service(BaseDirectory):
    """Class to handle inernal ArcGIS Service instance"""
    def __init__(self, url, usr='', pw='', token=RESTAPI_TOKEN):
        super(Service, self).__init__(url, usr, pw, token)

        for k,v in self.response.iteritems():
            if k not in ['extensions', 'status']:
                setattr(self, k, v)
        objectize(self, ['extensions'])
        self.fullName = '.'.join([self.serviceName, self.type])

    @property
    def extensions(self):
        """return list of custom server object extensions that are registered with the server"""
        return[namedTuple('Extension', r) for r in POST(self.url, token=self.token)['extensions']]

    @property
    def status(self):
        """return status JSON object for service"""
        return POST(self._servicesURL + '/status', token=self.token)

    @passthrough
    def start(self):
        """starts the service"""
        r = {}
        if self.configuredState.lower() == 'stopped':
            r = POST(self.url + '/start', token=self.token)
            if 'success' in r:
                print('started: {}'.format(self.fullName))
            self.refresh()
        else:
            print('"{}" is already started!'.format(self.fullName))
        return r

    @passthrough
    def stop(self):
        """stops the service"""
        r = {}
        if self.configuredState.lower() == 'started':
            r = POST(self.url + '/stop', token=self.token)
            if 'success' in r:
                print('stoppedd: {}'.format(self.fullName))
            self.refresh()
        else:
            print('"{}" is already stopped!'.format(self.fullName))
        return r

    @passthrough
    def edit(self, serviceJSON={}, **kwargs):
        """edit the service, properties that can be edited vary by the service type

        Optional
            serviceJSON -- JSON representation of service with edits
            kwargs -- list of keyword arguments, you can use these if there are just a
                few service options that need to be updated.  It will grab the rest of
                the service info by default.
        """
        if not serviceJSON:
            serviceJSON = self.response

        # update by kwargs
        for k,v in kwargs.iteritems():
            serviceJSON[k] = v
        params = {'service': serviceJSON}
        r = POST(self.url + '/edit', params, token=self.token)
        self.refresh()
        return r

    @passthrough
    def delete(self):
        """deletes the service, proceed with caution"""
        r = POST(self.url + '/delete', token=self.token)
        self.response = None
        self.url = None
        return r

    def itemInfo(self):
        """get service metadata"""
        query_url = self.url + '/iteminfo'
        return namedTuple('ItemInfo', POST(query_url, token=self.token))

    @passthrough
    def editItemInfo(self, itemInfo, thumbnailFile=None):
        """edit the itemInfo for service

        Required:
            itemInfo -- JSON itemInfo objet representing metadata

        Optional:
            thumbnailFile -- path to optional thumbnail image
        """
        query_url = self.url + '/iteminfo/edit'
        if thumbnailFile and os.path.exists(thumbnailFile):
            # use mimetypes to guess "content_type"
            import mimetypes
            known = mimetypes.types_map
            common = mimetypes.common_types
            ext = os.path.splitext(thumbnailFile)[-1].lower()
            content_type = 'image/jpg'
            if ext in known:
                content_type = known[ext]
            elif ext in common:
                content_type = common[ext]

            # make multi-part encoded file
            files = {'thumbnail': (os.path.basename(thumbnailFile), open(thumbnailFile, 'rb'), content_type)}
        else:
            files = ''

        params = {'serviceItemInfo': json.dumps(itemInfo) if isinstance(itemInfo, dict) else itemInfo,
                  'token': self.token.token if isinstance(self.token, Token) else self.token,
                  'f': 'json'}

        return requests.post(query_url, params, files=files, verify=False).json()

    @passthrough
    def uploadItemInfo(self, folder, file):
        """uploads a file associated with the item information the server; placed in directory
        specified by folder parameter

        folder -- name of the folder to which the file will be uploaded
        file -- full path to file to be uploaded to server
        """
        query_url = self.url + '/iteminfo/upload'
        return POST(query_url, {'folder': folder, 'file':file}, token=self.token)

    @passthrough
    def deleteItemInformation(self):
        """deletes information about the service, configuration is not changed"""
        query_url = self.url + '/iteminfo/delete'
        return POST(query_url, token=self.token)

    def manifest(self):
        """get service manifest.  This  documents the data and other resources that define the
        service origins and power the service"""
        query_url = self.url + 'iteminfo/manifest/manifest.json'
        return namedTuple('ServiceManifest', POST(query_url, token=self.token))

    def statistics(self):
        """return service statistics object"""
        class ServiceStatistics(object):
            """service statistics"""
            def __init__(self, **kwargs):
                for k,v in kwargs.iteritems():
                    setattr(self, k, v)
                objectize(self)

        return ServiceStatistics(**POST(self.url + '/statistics', token=self.token))

class Site(AdminRESTEndpoint):
    def __init__(self, url, usr='', pw='', token=RESTAPI_TOKEN):
        super(Site, self).__init__(url, usr, pw, token)
        pass

class ArcServerAdmin(AdminRESTEndpoint):
    """Class to handle internal ArcGIS Server instance"""
    def __init__(self, url, usr='', pw='', token=''):
        #possibly redundant validation...
        if not 'arcgis' in url.lower():
            url += '/arcgis'
        url = url.split('/arcgis')[0] + '/arcgis/admin/services'
        super(ArcServerAdmin, self).__init__(url, usr, pw, token)
        self._adminURL = self.url.split('/arcgis')[0] + '/arcgis/admin'
        self._clusterURL = self._adminURL + '/clusters'
        self._dataURL = self._adminURL + '/data'
        self._extensionsURL = self._adminURL + '/types/extensions'
        self._infoURL = self._adminURL + '/info'
        self._kmlURL = self._adminURL + '/kml'
        self._logsURL = self._adminURL + '/logs'
        self._machinesURL = self._adminURL + '/machines'
        self._securityURL = self._adminURL + '/security'
        self._servicesURL = self._adminURL + '/services'
        self._siteURL = self._adminURL + '/site'
        self._sysetemURL = self._adminURL + '/system'
        self._uploadsURL = self._adminURL + '/uploads'
        self._usagereportsURL = self._adminURL + '/usagereports'

        for k,v in self.response.iteritems():
            if k != 'services':
                setattr(self, k, v)

        self.services = self.list_services()

    #----------------------------------------------------------------------
    # general methods and properties

    @property
    def clusters(self):
        """get a list of cluster objects"""
        r = POST(self._clusterURL, token=self.token)
        return [namedTuple('AGSCluster', d) for d in r['clusters']]

    @property
    def types(self):
        """get a list of all server service types and extensions (types)"""
        r = POST(self._servicesURL + '/types', token=self.token)
        return [namedTuple('Type', d) for d in r['types']]

    @property
    def psa(self):
        """returns Primary Site Administrator object"""
        query_url = self._securityURL + '/psa'
        return PrimarySiteAdministrator(POST(query_url, token=self.token))

    @property
    def roleStore(self):
        """returns RoleStore object"""
        return RoleStore(self._securityURL + '/roles', token=self.token)

    @property
    def userStore(self):
        """returns a UserStore object"""
        return UserStore(self._securityURL + '/users', token=self.token)

    @property
    def dataStore(self):
        """returns a DataStore object"""
        return DataStore(self._dataURL + '/config', token=self.token)

    @property
    def publicKey(self):
        """This resource returns the public key of the server that can be
        used by a client application (or script) to encrypt data sent to
        the server using the RSA algorithm for public-key encryption. In
        addition to encrypting the sensitive parameters, the client is
        also required to send to the server an additional flag encrypted
        with value set to true.
        """
        r = POST(self.url + '/publicKey', token=self.token)
        return namedTuple('PublicKey', r)

    def cluster(self, clusterName):
        """returns a Cluster object

        Required:
            clusterName -- name of cluster to connect to
        """
        return Cluster(POST(self._clusterURL + '/{}'.format(clusterName), token=self.token))

    def list_services(self):
        """list of fully qualified service names"""
        services = ['/'.join([self._servicesURL,
                    '.'.join([serv['serviceName'], serv['type']])])
                    for serv in self.response['services']]

        for f in self.folders:
            folder = Folder(self._servicesURL + '/{}'.format(f), token=self.token)
            for service in folder.list_services():
                services.append('{}/{}/{}'.format(self._servicesURL, folder, service))

        return services

    def iter_services(self):
        """iterate through Service Objects"""
        for serviceName in self.services:
            yield self.service(serviceName)


    #----------------------------------------------------------------------
    # clusters
    @passthrough
    def createCluster(self, clusterName, machineNames, topCluserPort):
        """create a new cluster on ArcGIS Server Site

        Required:
            clusterName -- name of new cluster
            machineNames -- comma separated string of machine names or list
            topClusterPort -- TCP port number used by all servers to communicate with eachother
        """
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        params = {'clusterName': clusterName,
                  'machineNames': machineNames,
                  'topClusterPort': topCluserPort}

        return POST(self._clusterURL + '/create', params, token=self.token)

    def getAvailableMachines(self):
        """list all server machines that don't participate in a cluster and are
        available to be added to a cluster (i.e. registered with server"""
        query_url = self.url.split('/clusters')[0] + '/clusters/getAvailableMachines'
        return POST(query_url, token=self.token)['machines']

    @passthrough
    def startCluster(self, clusterName):
        """starts a cluster

        Required:
            clusterName -- name of cluster to start
        """
        self._clusterURL + '/{}/start'.format(clusterName)
        return POST(query_url, token=self.token)

    @passthrough
    def stopCluster(self, clusterName):
        """stops a cluster

        Required:
            clusterName -- name of cluster to start
        """
        self._clusterURL + '/{}/stop'.format(clusterName)
        return POST(query_url, token=self.token)

    @passthrough
    def editProtocol(self, clusterName, clusterProtocol):
        """edits the cluster protocol.  Will restart the cluster with updated protocol.
         The clustering protocol defines a channel which is used by server machines within
         a cluster to communicate with each other. A server machine will communicate with
         its peers information about the status of objects running within it for load
         balancing and default tolerance.

        ArcGIS Server supports the TCP clustering protocols where server machines communicate
        with each other over a TCP channel (port).

        Required:
            clusterName -- name of cluster
            clusterProtocol -- JSON object representing the cluster protocol TCP port

        Example:
            clusterProtocol = {"tcpClusterPort":"4014"}
        """
        query_url = self._clusterURL + '/{}/editProtocol'.format(clusterName)
        params = {'clusterProtocol': clusterProtocol}

        return POST(query_url, params, token=self.token)

    @passthrough
    def deleteCluster(self, clusterName):
        """delete a cluster

        clusterName -- cluster to be deleted
        """
        query_url = self._clusterURL + '/{}/delete'.format(clusterName)
        POST(query_url, {'clusterName': clusterName}, token=self.token)

    def getMachinesInCluster(self, clusterName):
        """list all server machines participating in a cluster

        Required:
            clusterName -- name of cluster
        """
        query_url = self._clusterURL + '/{}/machines'.format(clusterName)
        return [ClusterMachine(**r) for r in POST(query_url, token=self.token)]

    def getServicesInCluster(self, clusterName):
        """get a list of all services in a cluster

        Required:
            clusterName -- name of cluster to search for services
        """
        query_url = self._clusterURL+ '{}/services'.format(clusterName)
        resp = POST(query_url, token=self.token)
        return [namedTuple('ServiceProperties', r) for r in resp['services']]

    @passthrough
    def addMachinesToCluster(self, clusterName, machineNames):
        """adds new machines to site.  Machines must be registered beforehand

        Required:
            cluster -- cluster name
            machineNames -- comma separated string of machine names or list
        """
        query_url = self._clusterURL + '{}/add'.format(clusterName)
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return POST(query_url, {'machineNames': machineNames}, token=self.token)

    @passthrough
    def removeMachinesFromCluster(self, clusterName, machineNames):
        """remove machine names from cluster

        Required:
            clusterName -- name of cluster
            machineNames -- list or comma-separated list of machine names

        Examples:
            machineNames= "SERVER2.DOMAIN.COM,SERVER3.DOMAIN.COM"
        """
        query_url = self._clusterURL + '/{}/machines/remove'.format(clusterName)
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return POST(query_url, {'machineNames': machineNames}, token=self.token)

    #----------------------------------------------------------------------
    # data store.  To use all data store methods connect to data store
    # example:
    # ags = restapi.admin.ArcServerAdmin(url, usr, pw)
    # ds = ags.dataStore <- access all data store methods through ds object

    @passthrough
    def config(self):
        """return configuratoin properties"""
        return POST(self._dataURL + '/config', token=self.token)

    # not available in ArcGIS REST API, included here to refresh data store cache
    def getDataItems(self):
        """returns a refreshed list of all data items"""
        items = []
        for it in self.getRootItems():
            items += self.findDataItems(it)
        return items

    @passthrough
    def registerDataItem(self, item):
        """registers an item with the data store

        Required:
            item -- JSON representation of new data store item to register

        Example:
            item={
            	"path": "/fileShares/folder_shared", //a unique path on the server
            	"type": "folder", //as this is a file share
            	"clientPath": null, //not needed as this is a shared folder
            	"info": {
            		"path": "\\\\server\\data\\rest_data", //path to the share
            		"dataStoreConnectionType": "shared" //this is a shared folder
            		}
            	}
        """
        if self.validateItem(item):
            query_url = self._dataURL + '/registerItem'
            return POST(query_url, {'item': item}, token=self.token)

        return None

    @passthrough
    def unregisterDataItem(self, itemPath):
        """unregisters an item with the data store

        Required:
            itemPath -- path to data item to unregister (DataItem.path)
        """
        query_url = self._dataURL + '/registerItem'
        return POST(query_url, {'itemPath': itemPath}, token=self.token)

    def findDataItems(self, parentPath, ancestorPath='', types='', id=''):
        """search through items registered in data store

        Required:
            parentPath -- path of parent under which to find items

        Optional:
            ancestorPath -- path of ancestor which to find items
            types -- filter for the type of items to search
            id -- filter to search the ID of the item
        """
        query_url = self._dataURL + '/findItems'
        params = {'parentPath': parentPath,
                  'ancestorPath': ancestorPath,
                  'types': types,
                  'id': id}

        ds_items = POST(query_url, params, token=self.token)['items']
        for d in ds_items:
            d['url'] = '{}/items{}'.format(self._dataURL, d['path'])

        return [DataItem(d) for d in ds_items]

    def validateDataItem(self, item):
        """validates a data store item

        Required:
            item -- JSON representation of new data store item to validate
        """
        query_url = self._dataURL + '/validateDataItem'
        r = POST(query_url, {'item': item}, token=True)
        if 'status' in r and r['status'] == 'success':
            return True
        else:
            pprint.pprint(r)
            return False

    @passthrough
    def validateAllDataItems(self):
        """validates all data items in data store.  Warning, this operation can be
        VERY time consuming, depending on how many items are registered with the
        data store
        """
        return POST(self._dataURL + '/validateAllDataItems', token=self.token)

    def computeRefCount(self, path):
        """get the total number of references to a given data item that exists on
        the server.  Can be used to determine if a data resource can be safely
        deleted or taken down for maintenance.

        Required:
            path -- path to resource on server (DataItem.path)
        """
        query_url = self._dataURL + '/computeTotalRefCount'
        r  = passthrough(POST(query_url, {'path': path}, token=self.token))
        return int(r['totalRefCount'])

    def getRootItems(self):
        """method to get all data store items at the root"""
        return POST(self._dataURL + '/items', token=self.token)['rootItems']

    @passthrough
    def startDataStoreMachine(self, dataItem, machineName):
        """starts the database instance running on the data store machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self._dataURL + '/items/{}/machines/{}/start'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def stopDataStoreMachine(self, dataItem, machineName):
        """starts the database instance running on the data store machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self._dataURL + '/items/{}/machines/{}/stop'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def removeDataStoreMachine(self, dataItem, machineName):
        """removes a standby machine from the data store, this operation is not
        supported on the primary data store machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self._dataURL + '/items/{}/machines/{}/remove'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def makeDataStorePrimaryMachine(self, dataItem, machineName):
        """promotes a standby machine to the primary data store machine. The
        existing primary machine is downgraded to a standby machine

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to make primary
        """
        query_url = self._dataURL + '/items/{}/machines/{}/makePrimary'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    def validateDataStore(self, dataItem, machineName):
        """ensures that the data store is valid

        Required:
            dataItem -- name of data item (DataItem.path)
            machineName -- name of machine to validate data store against
        """
        query_url = self._dataURL + '/items/{}/machines/{}/validate'.format(dataItem, machineName)
        return POST(query_url, token=self.token)

    @passthrough
    def updateDatastoreConfig(self, datastoreConfig={}):
        """update data store configuration.  Can use this to allow or block
        automatic copying of data to server at publish time

        Optional:
            datastoreConfig -- JSON object representing datastoreConfiguration.  if none
                supplied, it will default to disabling copying data locally to the server.
        """
        query_url = self._dataURL + '/config/update'
        if not datastoreConfig:
            datastoreConfig = '{"blockDataCopy":"true"}'
        return POST(query_url, {'datastoreConfig': datastoreConfig}, token=self.token)

    #----------------------------------------------------------------------
    # LOGS
    @passthrough
    def logSettings(self):
        """returns log settings"""
        query_url = self._logsURL + '/settings'
        return namedTuple('LogSettings', POST(query_url, token=self.token)['settings'])

    @passthrough
    def editLogSettings(self, logLevel='WARNING', logDir=None, maxLogFileAge=90, maxErrorReportsCount=10):
        """edits the log settings

        logLevel -- type of log [OFF, SEVERE, WARNING, INFO, FINE, VERBOSE, DEBUG]
        logDir -- destination file path for root of log directories
        maxLogFileAge -- number of days for server to keep logs.  Default is 90.
        maxErrorReportsCount -- maximum number of error report files per machine
        """
        query_url = self._logsURL + '/settings/edit'
        if not logDir:
            logDir = r'C:\\arcgisserver\logs'

        params = {'logLevel': logLevel,
                  'logDir': logDir,
                  'maxLogFileAge': maxLogFileAge,
                  'maxErrorReportsCount': maxErrorReportsCount}

        return POST(query_url, params, token=self.token)

    def queryLogs(self, startTime='', endTime='', sinceLastStarted=False, level='WARNING', filter=None, pageSize=1000):
        """query all log reports accross an entire site

        Optional:
            startTime -- most recent time to query.  Leave blank to start from now
            endTime -- oldest time to query
            sinceLastStart -- boolean to only return records since last time server
                was started.
            level -- log level [SEVERE, WARNING, INFO, FINE, VERBOSE, DEBUG].  Default is WARNING.
            filter -- Filtering is allowed by any combination of services, server components, GIS
                server machines, or ArcGIS Data Store machines. The filter accepts a semi-colon
                delimited list of filter definitions. If any definition is omitted, it defaults to all.
            pageSize -- max number of records to return, default is 1000

        startTime and endTime examples:
             as datetime:  datetime.datetime(2015, 7, 30)
             as a string: "2011-08-01T15:17:20,123"
             in milliseconds:  1312237040123  #can use restapi.rest_utils.date_to_mil(datetime.datetime.now())
                                              # to get time in milliseconds

        filter examples:
            Specific service logs on a specific machine:

            {"services": ["System/PublishingTools.GPServer"], "machines": ["site2vm0.domain.com"]}

            Only server logs on a specific machine:

            {"server": "*", "machines": ["site2vm0.domain.com"]}

            All services on all machines and only REST logs:

            "services": "*", "server": ["Rest"]
        """
        if isinstance(startTime, datetime.datetime):
            startTime = date_to_mil(startTime)

        #if not endTime:
        #    # default to 1 week ago
        #    endTime = date_to_mil(datetime.datetime.now() - relativedelta(days=7))

        elif isinstance(endTime, datetime.datetime):
            endTime = date_to_mil(endTime)

        if filter is None or not isinstance(filter, dict):
            filter = {"server": "*",
                      "services": "*",
                      "machines":"*" }

        query_url = self._logsURL + '/query'
        params = {'startTime': startTime,
                  'endTime': endTime,
                  'sinceLastStarted': sinceLastStarted,
                  'level': level,
                  'filter': json.dumps(filter) if isinstance(filter, dict) else filter,
                  'pageSize': pageSize
                  }

        r = POST(query_url, params, token=self.token)

        class Log(object):
            """class to handle Log Report instance"""
            def __init__(self, resp):
                """resp: JSON for log reports request"""
                for k,v in resp.iteritems():
                    if k not in ('startTime', 'endTime'):
                        setattr(self, k, v)
                self.startTime = mil_to_date(resp['startTime'])
                self.endTime = mil_to_date(resp['endTime'])
                objectize(self)

            def __getitem__(self, index):
                """allows for indexing of log files"""
                return self.logMessages[index]

            def __iter__(self):
                """return logMessages as generator"""
                for log in self.logMessages:
                    yield log

            def __len__(self):
                """get number of log messages returned by query"""
                return len(self.logMessages)

            def __nonzero__(self):
                """returns True if log messages were returned"""
                return bool(len(self))

        return Log(r)

    @passthrough
    def countErrorReports(self, machines='All'):
        """counts the number of error reports on each machine

        Optional:
            machines -- machine names to count error reports on.  Default is All
        """
        return POST(self._logsURL + 'countErrorReports', token=self.token)

    @passthrough
    def cleanLogs(self):
        """clean all log reports. Proceed with caution, cannot be undone!"""
        return POST(self._logsURL + '/clean', token=self.token)
    #----------------------------------------------------------------------
    # SECURITY

    # USERS ------------------------------
    @passthrough
    def addUser(self, username, password, fullname='', description='', email=''):
        """adds a user account to user store

        Requred:
            username -- username for new user
            password -- password for new user

        Optional:
            fullname -- full name of user
            description -- description for user
            email -- email address for user account
        """
        query_url = self._securityURL + '/users/add'
        params = {'username': username,
                  'password': password,
                  'fullname': fullname,
                  'description': description,
                  'email': email}

        return POST(query_url, params, token=self.token)

    def getUsers(self, startIndex='', pageSize=''):
        """get all users in user store, intended for iterating over all user accounts

        Optional:
            startIndex -- zero-based starting index from roles list. Default is 0.
            pageSize -- maximum number of roles to return. Default is 10.
        """
        query_url = self._securityURL + '/users/getUsers'

        params = {'startIndex': startIndex,
                  'pageSize': pageSize}

        r = POST(query_url, params, token=self.token)
        return [namedTuple('User', u) for u in r['users']]

    def searchUsers(self, filter='', maxCount=''):
        """search the user store, returns UserStore object

        Optional:
            filter -- filter string for users (ex: "john")
            maxCount -- maximimum number of records to return
        """
        query_url = self._securityURL + '/users/search'

        params = {'filter': filter,
                  'maxCount': maxCount}

        r = POST(query_url, params, token=self.token)
        return [namedTuple('User', u) for u in r['users']]

    @passthrough
    def removeUser(self, username):
        """removes a user from the user store

        Required:
            username -- name of user to remove
        """
        query_url = self._securityURL + '/users/remove'
        return POST(query_url, {'username':username}, token=self.token)

    @passthrough
    def updateUser(self, username, password, fullname='', description='', email=''):
        """updates a user account in the user store

        Requred:
            username -- username for new user
            password -- password for new user

        Optional:
            fullname -- full name of user
            description -- description for user
            email -- email address for user account
        """
        query_url = self._securityURL + '/users/update'
        params = {'username': username,
                  'password': password,
                  'fullname': fullname,
                  'description': description,
                  'email': email}

        return POST(query_url, params, token=self.token)

    @passthrough
    def assignRoles(self, username, roles):
        """assign role to user to inherit permissions of role

        Required:
            username -- name of user
            roles -- list or comma separated list of roles
        """
        query_url = self._securityURL + '/users/assignRoles'

        if isinstance(roles, (list, tuple)):
            roles = ','.join(map(str, roles))

        params = {'username': username,
                  'roles': roles}

        return POST(query_url, params, token=self.token)

    @passthrough
    def removeRoles(self, username, rolenames):
        """removes roles that have been previously assigned to a user account, only
        supported when role store supports reads and writes

        Required:
            username -- name of the user
            roles -- list or comma separated list of role names
        """
        query_url = self._securityURL + '/users/removeRoles'

        if isinstance(roles, (list, tuple)):
            roles = ','.join(map(str, roles))

        params = {'username': username,
                  'roles': roles}

        return _POST(query_url, params, token=self.token)

    @passthrough
    def getPrivilegeForUser(self, username):
        """gets the privilege associated with a role

        Required:
            username -- name of user
        """
        query_url = self._securityURL + '/users/getPrivilege'
        return POST(query_url, {'username': username}, token=self.token)

    # ROLES -----------------------------------------
    @passthrough
    def addRole(self, rolename, description=''):
        """adds a role to the role store

        Required:
            rolename -- name of role to add

        Optional:
            description -- optional description for new role
        """
        query_url = self._securityURL + '/roles/add'
        params = {'rolename': rolename,
                  'description': description}

        return POST(query_url, params, token=self.token)

    def getRoles(self, startIndex='', pageSize=''):
        """This operation gives you a pageable view of roles in the role store. It is intended
        for iterating through all available role accounts. To search for specific role accounts
        instead, use the searchRoles() method. <- from Esri help

        Optional:
            startIndex -- zero-based starting index from roles list.
            pageSize -- maximum number of roles to return.
        """
        query_url = self._securityURL + '/roles/getRoles'

        params = {'startIndex': startIndex,
                  'pageSize': pageSize}

        r = POST(query_url, params, token=self.token)['roles']
        return [namedTuple('Role', r) for r in self.role_cache]

    def searchRoles(self, filter='', maxCount=''):
        """search the role store

        Optional:
            filter -- filter string for roles (ex: "editors")
            maxCount -- maximimum number of records to return
        """
        query_url = self._securityURL + '/roles/search'

        params = {'filter': filter,
                  'maxCount': maxCount}

        r = POST(query_url, params, token=self.token)['roles']
        return [namedTuple('Role', r) for r in self.role_cache]

    @passthrough
    def removeRole(self, rolename):
        """removes a role from the role store

        Required:
            rolename -- name of role
        """
        query_url = self._securityURL + '/roles/remove'
        return POST(query_url, {'rolename':rolename}, token=self.token)

    @passthrough
    def updateRole(self, rolename, description=''):
        """updates a role

        Required:
            rolename -- name of the role

        Optional:
            description -- descriptoin of role
        """
        query_url = self._securityURL + '/roles/update'

        params = {'rolename': rolename,
                  'description': description}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getRolesForUser(self, username, filter='', maxCount=10):
        """returns the privilege associated with a user

        Required:
            privilege -- name of privilege (ADMINISTER | PUBLISH)
        """
        query_url = self._securityURL + '/roles/getRolesForUser'
        params = {'username': username,
                  'filter': filter,
                  'maxCount': maxCount}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getUsersWithinRole(self, rolename, filter='', maxCount=10):
        """get all user accounts to whom this role has been assigned

        Required:
            rolename -- name of role

        Optional:
            filter -- optional filter to be applied to the resultant user set
            maxCount -- maximum number of results to return
        """
        query_url = self._securityURL + '/roles/getUsersWithinRole'
        params = {'rolename': username,
                  'filter': filter,
                  'maxCount': maxCount}

        return POST(query_url, params, token=self.token)

    @passthrough
    def addUsersToRole(self, rolename, users):
        """assign a role to multiple users with a single action

        Required:
            rolename -- name of role
            users -- list of users or comma separated list
        """
        query_url = self._securityURL + '/roles/addUsersToRole'

        if isinstance(users, (list, tuple)):
            users = ','.join(map(str, users))

        params = {'rolename': rolename,
                  'users': users}

        return POST(query_url, params, token=self.token)

    @passthrough
    def removeUsersFromRole(self, rolename, users):
        """removes a role assignment from multiple users.

        Required:
            rolename -- name of role
            users -- list or comma separated list of user names
        """
        query_url = self._securityURL + '/roles/removeUsersFromRole'

        if isinstance(users, (list, tuple)):
            users = ','.join(map(str, users))

        params = {'rolename': rolename,
                  'users': users}

        return _POST(query_url, params, token=self.token)

    @passthrough
    def assignPrivilege(self, rolename, privilege='ACCESS'):
        """assign administrative acess to ArcGIS Server

        Required:
            rolename -- name of role
            privilege -- administrative capability to assign (ADMINISTER | PUBLISH | ACCESS)
        """
        query_url -- self._securityURL + '/roles/assignPrivilege'

        params = {'rolename': rolename,
                  'privilege': privilege.upper()}

        return POST(query_url, params, token=self.token)

    @passthrough
    def getPrivilegeForRole(self, rolename):
        """gets the privilege associated with a role

        Required:
            rolename -- name of role
        """
        query_url = self._securityURL + '/roles/getPrivilege'
        return POST(query_url, {'rolename':rolename}, token=self.token)

    @passthrough
    def getRolesByPrivilege(self, privilege):
        """returns the privilege associated with a user

        Required:
            privilege -- name of privilege (ADMINISTER | PUBLISH)
        """
        query_url = self._securityURL + '/roles/getRolesByPrivilege'
        return POST(query_url, {'privilege': privilege.upper()}, token=self.token)

    # GENERAL SECURITY ------------------------------
    @passthrough
    def securityConfig(self):
        """returns the security configuration as JSON

        http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Security_Configuration/02r3000001t9000000/
        """
        return POST(self._securityURL + '/config', token=self.token)

    @passthrough
    def updateSecurityConfig(self, securityConfig):
        """updates the security configuration on ArcGIS Server site.  Warning:
        This operation will cause the SOAP and REST service endpoints to be
        redeployed (with new configuration) on every server machine in the site.
        If the authentication tier is GIS_SERVER, then the ArcGIS token service
        is started on all server machines.

        Required:
            securityConfig -- JSON object for security configuration.

        Example:
            securityConfig={
                  "Protocol": "HTTP_AND_HTTPS",
          		  "authenticationTier": "GIS_SERVER",
                  "allowDirectAccess": "true",
                  "virtualDirsSecurityEnabled": "false",
                  "allowedAdminAccessIPs": ""
                	}
        """
        query_url = self._securityURL + '/config/update'

        params = {'securityConfig': json.dumps(securityConfig)
                    if isinstance(securityConfig, dict) else securityConfig}

        return POST(query_url, params, token=self.token)

    @passthrough
    def updateIdentityStore(self, userStoreConfig, roleStoreConfig):
        """Updates the location and properties for the user and role store in your ArcGIS Server site.

        While the GIS server does not perform authentication when the authentication tier selected is
        WEB_ADAPTOR, it requires access to the role store for the administrator to assign privileges to
        the roles. This operation causes the SOAP and REST service endpoints to be redeployed (with the
        new configuration) on every server machine in the site, and therefore this operation must be
        used judiciously.

        http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Update_Identity_Store/02r3000001s0000000/

        Required:
            userStoreConfig -- JSON object representing user store config
            roleStoreConfig -- JSON object representing role store config

        Examples:
       	    userStoreConfig={
               "type": "LDAP",
               "properties": {
                 "adminUserPassword": "aaa",
                 "adminUser": "CN=aaa,ou=users,ou=ags,dc=example,dc=com",
                 "ldapURLForUsers": "ldap://xxx:10389/ou=users,ou=ags,dc=example,dc=com",
                 "usernameAttribute": "cn",
                 "failOverLDAPServers": "hostname1:10389,hostname2:10389"
              }

             roleStoreConfig={
               "type": "LDAP",
               "properties": {
                  "ldapURLForRoles": "ldap://xxx:10389/ou=roles,ou=ags,dc=example,dc=com",
                  "adminUserPassword": "aaa",
                  "adminUser": "CN=aaa,ou=users,ou=ags,dc=example,dc=com",
                  "memberAttributeInRoles": "uniquemember",
                  "ldapURLForUsers": "ldap://xxx:10389/ou=users,ou=ags,dc=example,dc=com",
                  "rolenameAttribute": "cn",
                  "usernameAttribute": "cn",
                  "failOverLDAPServers": "hostname1:10389,hostname2:10389"
                }
        """
        query_url = self._securityURL + '/config/updateIdentityStore'
        params = {'userStoreConfig': json.dumps(userStoreConfig)
                    if isinstance(userStoreConfig, dict) else userStoreConfig,
                  'roleStoreConfig': json.dumps(roleStoreConfig)
                    if isinstance(roleStoreConfig, dict) else roleStoreConfig}

        return POST(query_url, params, token=self.token)

    @passthrough
    def testIdentityStore(self, userStoreConfig, roleStoreConfig):
        """tests the connection to the input user and role store

        Required:
            userStoreConfig -- JSON object representing user store config
            roleStoreConfig -- JSON object representing role store config

        Examples:
            userStoreConfig={
                "type": "LDAP",
                "properties": {
                    "ldapURLForUsers": "ldap://server/dc=example,dc=com???(|(objectClass=userProxy)(objectClass=user))?",
                    "ldapURLForRoles": "ldap://server/dc=example,dc=com???(&(objectClass=group))?",
                    "memberAttributeInRoles": "member",
                    "usernameAttribute": "name",
                    "rolenameAttribute": "name",
                    "adminUser": "cn=admin,cn=users,dc=example,dc=com",
                    "adminUserPassword": "admin"
                }

            roleStoreConfig={
                "type": "BUILTIN",
                "properties": {}
            }
        """
        query_url = self._securityURL + '/config/testIdentityStore'
        params = {'userStoreConfig': json.dumps(userStoreConfig)
                    if isinstance(userStoreConfig, dict) else userStoreConfig,
                  'roleStoreConfig': json.dumps(roleStoreConfig)
                    if isinstance(roleStoreConfig, dict) else roleStoreConfig}

        return POST(query_url, params, token=self.token)

    # TOKENS -----------------------------------------
    @passthrough
    def tokens(self):
        """returns the token configuration with the server, can use updatetoken()
        to change the shared secret key or valid token durations"""
        return POST(self._securityURL + '/tokens', token=self.token)

    @passthrough
    def updateTokenConfig(self, tokenManagerConfig):
        """update the token configuration

        Required:
            tokenManagerConfig -- JSON object for token configuration

        Example:
            tokenManagerConfig={
                    "type": "BUILTIN",
                    "properties": {
                        "sharedKey": "secret.passphrase",
                        "longTimeout": "2880",
                        "shortTimeout": "120"
                    }
                }
        """
        query_url = self._securityURL + '/tokens/update'

        params = {'securityConfig': json.dumps(tokenManagerConfig)
                    if isinstance(tokenManagerConfig, dict) else tokenManagerConfig}

        return POST(query_url, params, token=self.token)

    # PRIMARY SITE ADMINISTRATOR ------------------------------
    @passthrough
    def disablePSA(self):
        """disables the primary site administartor account"""
        query_url = self._securityURL + '/psa/disable'
        return POST(query_url, token=self.token)

    @passthrough
    def enablePSA(self):
        """enables the primary site administartor account"""
        query_url = self._securityURL + '/psa/enable'
        return POST(query_url, token=self.token)

    @passthrough
    def updatePSA(self, username, password):
        """updates the primary site administrator account


        Required:
            username -- new username for PSA (optional in REST API, required here
                for your protection)
            password -- new password for PSA
        """
        query_url = self._securityURL + '/psa/update'

        params = {'username': username,
                  'password': password}

        return POST(query_url, params, token=self.token)

    #----------------------------------------------------------------------
    # services
    def get_service_url(self, wildcard='*', asList=False):
        """method to return a service url

        Optional:
            wildcard -- wildcard used to grab service name (ex "moun*featureserver")
            asList -- default is false.  If true, will return a list of all services
                matching the wildcard.  If false, first match is returned.
        """
        if '*' in wildcard:
            if not '.' in wildcard:
                wildcard += '.*'
            if wildcard == '*':
                return self.services[0]
            else:
                if asList:
                    return [s for s in self.services if fnmatch.fnmatch(s, wildcard)]
            for s in self.services:
                if fnmatch.fnmatch(s, wildcard):
                    return s
        else:
            if asList:
                return [s for s in self.services if wildcard.lower() in s.lower()]
            for s in self.services:
                if wildcard.lower() in s.lower():
                    return s
        print('"{0}" not found in services'.format(wildcard))
        return None

    def folder(self, folderName):
        """administer folder

        folderName -- name of folder to connect to
        """
        query_url = self._servicesURL + '/{}'.format(folderName)
        return Folder(query_url, token=self.token)

    def service(self, service_name_or_wildcard):
        """return a restapi.admin.Service() object

        service_name_or_wildcard -- name of service or wildcard
        """
        service_url = self.get_service_url(service_name_or_wildcard, False)
        if service_url:
            return Service(service_url, token=self.token)
        else:
            raise NameError(service_name_or_wildcard)

    def getPermissions(self, resource):
        """return permissions for folder or service

        Required:
            resource -- name of folder or folder/service

        resource example:
            folder = 'Projects'

            service = 'Projects/HighwayReconstruction.MapServer'
        """
        query_url = self._servicesURL + '/{}/permissions'.format(resource)

        perms = POST(query_url, token=self.token)['permissions']
        return [Permission(r) for r in perms]

    @passthrough
    def addPermission(self, resource, principal='', isAllowed=True, private=True):
        """add a permission

        Required:
            resource -- name of folder or folder/service

        Optional:
            principal -- name of the role whome the permission is being assigned
            isAllowed -- tells if a resource is allowed or denied
            private -- default is True.  Secures service by making private, denies
                public access.  Change to False to allow public access.

        resource example:
            folder = 'Projects'

            service = 'Projects/HighwayReconstruction.MapServer'
        """
        add_url = self._servicesURL + '/{}/permissions/add'.format(resource)
        if principal:
            params = {'principal': principal, 'isAllowed': isAllowed}
            r = POST(add_url, params, token=self.token)

        params = {'principal': 'esriEveryone', 'isAllowed': 'false'}

        if private:
            esriEveryone = POST(add_url, params, token=self.token)
        else:
            params['isAllowed'] = 'true'
            esriEveryone = POST(add_url, params, token=self.token)

        if not principal:
            r = esriEveryone
        return r

    @passthrough
    def hasChildPermissionsConflict(self, resource, principal, permission=None):
        """check if service has conflicts with opposing permissions

        Required:
            resource -- name of folder or folder/service
            principal -- name of role for which to check for permission conflicts

        Optional:
            permission -- JSON permission object

        resource example:
            folder = 'Projects'

            service = 'Projects/HighwayReconstruction.MapServer'

        permission example:
            permission = {"isAllowed": True, "constraint": ""}
        """
        if not permission:
            permission = {"isAllowed": True, "constraint": ""}

        query_url = self._servicesURL + '/{}/permissions/hasChildPermissionConflict'.format(resource)
        params = {'principal': principal, 'permission': permission}
        return POST(query_url, params, token=self.token)

    @passthrough
    def cleanPermissions(self, principal):
        """cleans all permissions assigned to role (principal).  Useful when a role has
        been deleted

        principal -- name of role to delete permisssions
        """
        query_url = self._permissionsURL + '/clean'
        return POST(query_url, {'principal': principal}, token=self.token)

    @passthrough
    def createFolder(self, folderName, description=''):
        """creates a new folder in the root directory.  ArcGIS server only supports
        single folder hierachy

        Required:
            folderName -- name of new folder

        Optional:
            description -- description of folder
        """
        query_url = self._servicesURL + '/createFolder'
        params = {'folderName': folderName, 'description': description}
        return POST(query_url, params, token=self.token)

    @passthrough
    def deleteFolder(self, folderName):
        """deletes a folder in the root directory.

        folderName -- name of new folder
        """
        query_url = self._servicesURL + '{}/deleteFolder'.format(folderName)
        return POST(query_url, token=self.token)

    @passthrough
    def editFolder(self, folderName, description, webEncrypted):
        """edit a folder

        Required:
            folderName -- name of folder to edit
            description -- folder description
            webEncrypted -- boolean to indicate if the servies are accessible over SSL only.
        """
        query_url = self._servicesURL + '/{}/editFolder'.format(folderName)
        params = {'description': description, 'webEncrypted': webEncrypted}
        return POST(query_url, params, token=self.token)

    def extensions(self):
        """return list of custom server object extensions that are registered with the server"""
        return[namedTuple('Extension', r) for r in POST(self._extensionsURL, token=self.token)['extensions']]

    @passthrough
    def registerExtension(self, id):
        """regesters a new server object extension.  The .SOE file must first e uploaded to
        the server using the restapi.admin.Service.uploadDataItem() method

        id -- itemID of the uploaded .SOE file
        """
        query_url = self._extensionsURL + '/register'
        return POST(query_url, {'id': id}, token=self.token)

    @passthrough
    def unregisterExtension(self, extensionFileName):
        """unregister a server object extension

        extensionFileName -- name of .SOE file to unregister
        """
        query_url = self._extensionsURL + '/unregister'
        return POST(query_url, {'extensionFileName': extensionFileName}, token=self.token)

    @passthrough
    def updateExtension(self, id):
        """updates extensions that have previously been registered with server

        id -- itemID of the uploaded .SOE file
        """
        return POST(self._extensionsURL + '/update', {'id': id}, token=self.token)

    @passthrough
    def federate(self):
        """federates ArcGIS Server with Portal for ArcGIS.  Imports services to make them available
        for portal.
        """
        return POST(self._servicesURL + '/federate', token=self.token)

    @passthrough
    def unfederate(self):
        """unfederate ArcGIS Server from Portal for ArcGIS. Removes services from Portal"""
        return POST(self._servicesURL + '/unfederate', token=self.token)

    @passthrough
    def startServices(self, servicesAsJSON={}, folderName='', serviceName='', type=''):
        """starts service or all services in a folder

        Optional:
            servicesAsJSON --list of services as JSON (example below)

        *the following parameters are options to run on an individual folder (not valid params of the REST API)

            folderName -- name of folder to start all services. Leave blank to start at root
            serviceName -- name of service to start. Leave blank to start all in folder
            type -- type of service to start (note: choosing MapServer will also stop FeatureServer):
                valdid types: MapServer|GPServer|NAServer|GeocodeServer|ImageServer


        servicesAsJSON example:
            {
                "services": [
                    {
                        "folderName": "",
                        "serviceName": "SampleWorldCities",
                        "type": "MapServer"
                    },
                    {
                        "folderName": "Watermain",
                        "serviceName": "CheckFireHydrants",
                        "type": "GPServer"
                    }
                ]
            }
        """
        query_url = self._servicesURL + '/startServices'
        if servicesAsJSON and isinstance(servicesAsJSON, dict):
            pass

        elif folderName:
            servicesAsJSON = {'services': []}
            folder = Folder(self._servicesURL + '/{}'.format(folderName), token=self.token)
            if not serviceName and not type:
                for serv in folder.services:
                    serv_json = serv.asJSON()
                    serv_json.pop('description')
                    if serv_json['type'] != 'FeatureServer':
                        servicesAsJSON['services'].append(serv_json)
            elif serviceName and not type:
                try:
                    serv_json = [s.asJSON() for s in folder.services if s.name.lower() == serviceName.lower()][0]
                    serv_json.pop('description')
                    servicesAsJSON.append(serv_json)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service named: "{}"'.format(serviceName)})
            elif type and not serviceName:
                try:
                    serv_json = [s.asJSON() for s in folder.services if s.type.lower() == type.lower()][0]
                    serv_json.pop('description')
                    servicesAsJSON.append(serv_json)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service types: "{}"'.format(serviceName)})

        if not servicesAsJSON or servicesAsJSON == {'services': []}:
            return RequestError({'error': 'no services specified!'})

        params = {'services': json.dumps(servicesAsJSON) if isinstance(servicesAsJSON, dict) else servicesAsJSON}
        return POST(query_url, params, token=self.token)

    @passthrough
    def stopServices(self, servicesAsJSON={}, folderName='', serviceName='', type=''):
        """stops service or all services in a folder

        Optional:
            servicesAsJSON --list of services as JSON (example below)

        *the following parameters are options to run on an individual folder (not valid params of the REST API)

            folderName -- name of folder to start all services. Leave blank to start at root
            serviceName -- name of service to start. Leave blank to start all in folder
            type -- type of service to start (note: choosing MapServer will also stop FeatureServer):
                valdid types: MapServer|GPServer|NAServer|GeocodeServer|ImageServer


        servicesAsJSON example:
            {
                "services": [
                    {
                        "folderName": "",
                        "serviceName": "SampleWorldCities",
                        "type": "MapServer"
                    },
                    {
                        "folderName": "Watermain",
                        "serviceName": "CheckFireHydrants",
                        "type": "GPServer"
                    }
                ]
            }
        """
        query_url = self._servicesURL + '/stopServices'
        if servicesAsJSON and isinstance(servicesAsJSON, dict):
            pass

        elif folderName:
            servicesAsJSON = {'services': []}
            folder = Folder(self._servicesURL + '/{}'.format(folderName), token=self.token)
            if not serviceName and not type:
                for serv in folder.services:
                    serv_json = serv.asJSON()
                    serv_json.pop('description')
                    if serv_json['type'] != 'FeatureServer':
                        servicesAsJSON['services'].append(serv_json)
            elif serviceName and not type:
                try:
                    serv_json = [s.asJSON() for s in folder.services if s.name.lower() == serviceName.lower()][0]
                    serv_json.pop('description')
                    servicesAsJSON.append(serv_json)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service named: "{}"'.format(serviceName)})
            elif type and not serviceName:
                try:
                    serv_json = [s.asJSON() for s in folder.services if s.type.lower() == type.lower()][0]
                    serv_json.pop('description')
                    servicesAsJSON.append(serv_json)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service types: "{}"'.format(serviceName)})

        if not servicesAsJSON or servicesAsJSON == {'services': []}:
            return RequestError({'error': 'no services specified!'})

        params = {'services': json.dumps(servicesAsJSON) if isinstance(servicesAsJSON, dict) else servicesAsJSON}
        return POST(query_url, params, token=self.token)

    def report(self):
        """return a list of service report objects"""

        reps = POST(self.url + '/report', token=self.token)['reports']
        return [Report(rep) for rep in reps]

    #----------------------------------------------------------------------
    # Site
    @passthrough
    def createSite(self, username, password, configStoreConnection='', directories='',
                   cluster='', logsSettings='', runAsync=True):
        """create a new ArcGIS Server Site

        Required:
            username -- name of administrative account used by site (can be changed later)
            password -- credentials for administrative account
            configStoreConnection -- JSON object representing the connection to the config store
            directories -- JSON object representing a collection of server directories to create.  By
                default the server directories will be created locally.
            cluster -- JSON object for optional cluster configuration.  By default cluster will be called
                "default" with the first available port numbers starting at 4004.

        Optional:
            logsSettings -- optional log settings
            runAsync -- flag to indicate if operation needs to ran asynchronously

        Examples:
            configStoreConnection = {
                    	"type": "FILESYSTEM", //only supported value for this property
                    	"connectionString": "/net/server/share/config-store",
                    	"class": "com.esri.arcgis.discovery.admin.store.file.FSConfigStore", //default class name for FILESYSTEM type
                    	"status": "Ready"
                        }

            directories = {
                        	"directories":
                        	[
                        		{
                        			"name": "mycache",
                        			"physicalPath": "\\\\server\\arcgisserver\\mycache",
                        			"directoryType": "CACHE",
                        			"cleanupMode": "NONE",
                        			"maxFileAge": 0,
                        			"description": "Used by service configurations to read/write cached tiles.",
                        			"virtualPath": "/rest/directories/mycache"
                        		},
                        		{
                        			"name": "myjobs",
                        			"physicalPath": "\\\\server\\arcgisserver\\myjobs",
                        			"directoryType": "JOBS",
                        			"cleanupMode": "NONE",
                        			"maxFileAge": 0,
                        			"description": "Used to store GP jobs.",
                        			"virtualPath": "/rest/directories/myjobs"
                        		}
                        	]
                        }

            cluster = {
                          "clusterName": "MapsCluster",
                          "clusterProtocol": {
                            "type": "TCP",
                            "tcpClusterPort": "4014",
                          }
                          "machineNames": [ "SERVER1.DOMAIN.COM", "SERVER2.DOMAIN.COM"]
                        }

            logsSettings = { "settings": {
                              "logDir": "C:\\arcgisserver\\logs\\"
                              "logLevel": "INFO",
                              "maxLogFileAge": 90,
                              "maxErrorReportsCount": 10
                            }}
        """
        query_url = self._adminURL + '/createNewSite'
        params = {'username': username,
                  'password': password,
                  'configStoreConnection': configStoreConnection,
                  'directories': directories,
                  'cluster': cluster}
        return POST(query_url, params, token=self.token)
