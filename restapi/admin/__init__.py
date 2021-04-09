# WARNING: much of this module is untested, this module makes permanent server configurations.
# Use with caution!
from __future__ import print_function
import sys
import os
import fnmatch
import datetime
import json
from collections import namedtuple
from ..rest_utils import Token, mil_to_date, date_to_mil, RequestError, IdentityManager, JsonGetter, \
    generate_token, ID_MANAGER, do_request, SpatialReferenceMixin, parse_url, get_portal_base, requestClient, \
    get_request_method, get_request_client, TokenExpired
from ..decorator import decorator
import munch
from .._strings import *
import requests
from .. import enums

import six
from six.moves import reload_module
from six.moves import urllib

# Globals
BASE_PATTERN = '*:*/arcgis/*admin*'
AGOL_ADMIN_BASE_PATTERN = 'http*://*/rest/admin/services*'
VERBOSE = True

# VERBOSE is set to true by default, this will echo the status of all operations
#  i.e. reporting an administrative change was successful.  To turn this off, simply
#  change VERBOSE to False.  This can be done like this:
#    VERBOSE = False #because you get this with importing the admin module
#  or:
#    restapi.admin.VERBOSE = False

__all__ = ['ArcServerAdmin', 'Service', 'Folder', 'Cluster', 'do_request',
           'generate_token', 'VERBOSE', 'mil_to_date', 'date_to_mil',
           'AGOLAdmin', 'AGOLFeatureService', 'AGOLFeatureLayer', 'AGOLMapService']


@decorator
def passthrough(f, *args, **kwargs):
    """Decorator to print results of function/method and returns json object.

    Args:
        f: Function/method.

    Set the global VERBOSE property to false if you do not want results of
    operations to be echoed during session.

    Example to disable print messages:
        restapi.admin.VERBOSE = False  # turns off verbosity
    """
    o = f(*args, **kwargs)
    if isinstance(o, dict) and  VERBOSE is True:
        print(json.dumps(o, indent=2))
    return o


class AdminRESTEndpoint(JsonGetter):
    """Base REST Endpoint Object to handle credentials and get JSON response

    Attributes:
        url: URL for image service.
        token: URL token.
    """
    def __init__(self, url, usr='', pw='', token='', client=None):
        """Inits class with credentials.

        Args:
            url: Image service url.
        Below args only required if security is enabled:
            usr: Username credentials for ArcGIS Server.
            pw: Password credentials for ArcGIS Server.
            token: Token to handle security (alternative to usr and pw).

        Raises:
            RuntimeError: 'Token expired at {}! Please sign in again.'
            RuntimeError: 'No token found, please try again with credentials'
            TypeError: 'Token expired at {}! Please sign in again.'
        """

        self.url = 'http://' + url.rstrip('/') if not url.startswith('http') \
                    and 'localhost' not in url.lower() else url.rstrip('/')

        self.token = token
        self.client = get_request_client(client)

        # check for portal stuff first!
        parsed = parse_url(url)
        if ('/sharing' in url or '/home' in url) and parsed.netloc != enums.agol.urls.base:
            portalBase = get_portal_base(url)
            self.url = portalBase
            if not isinstance(token, Token):
                infoResp = do_request(portalBase + '/rest/info', client=self.client)
                tokUrl = infoResp.get(enums.auth.info, {}).get(enums.auth.tokenServicesUrl)
                self.check_for_token(tokUrl, usr, pw, token)

        elif not fnmatch.fnmatch(self.url, BASE_PATTERN):
            _fixer = self.url.split('/arcgis')[0] + '/arcgis/admin'
            if fnmatch.fnmatch(_fixer, BASE_PATTERN):
                self.url = _fixer.lower()
            else:
                return RequestError({'error':{'URL Error': '"{}" is an invalid ArcGIS REST Endpoint!'.format(self.url)}})
        self.url = self.url.replace('/services//', '/services/') # cannot figure out where extra / is coming from in service urls
        params = {'f': 'json'}

        if self.token:
            if isinstance(token, six.string_types):
                try:
                    found_token = ID_MANAGER.findToken(token)
                    if found_token:
                        self.token = found_token
                except TokenExpired:
                    self.token = None
                except:
                    raise
            if isinstance(token, Token) and token.isExpired and not all([usr, pw]):
                raise ('Token expired at {}! Please sign in again.'.format(token.expires))
        if not self.token:
            self.check_for_token(self.url, usr, pw, self.token)

        if self.token:
            if isinstance(self.token, Token):
                params['token'] = self.token.token
            elif isinstance(self.token, six.string_types):
                params['token'] = self.token
            else:
                raise TypeError('Token <{}> of {} must be Token object or String!'.format(self.token, type(self.token)))

        # validate protocol
        if isinstance(self.token, Token):
            self.url = self.token.domain.split('://')[0] + '://' + self.url.split('://')[-1]

        resource_url = self.url
        if self.url.endswith('/sharing'):
            resource_url = self.url + '/rest/portals/self'

        request_method = get_request_method(resource_url, params, client=self.client)
        self.raw_response = request_method(resource_url, params=params)
        self.elapsed = self.raw_response.elapsed
        self.response = self.raw_response.json()
        self.json = munch.munchify(self.response)

    def check_for_token(self, url, usr=None, pw=None, token=None):
        if not self.token:
            if usr and pw:
                self.token = generate_token(url, usr, pw, client=self.client)
            else:
                self.token = ID_MANAGER.findToken(self.url)
                if self.token and self.token.isExpired:
                    raise TokenExpired('Token expired at {}! Please sign in again.'.format(token.expires))
                elif self.token is None:
                    raise RuntimeError('No token found, please try again with credentials')

        return self.token

    def request(self, *args, **kwargs):
        """Wrapper for request to automatically pass in credentials."""
        if 'token' not in kwargs:
            kwargs['token'] = self.token
        return do_request(*args, **kwargs)

    def refresh(self):
        """Refreshes the service properties."""
        self.__init__(self.url, token=self.token)


class BaseDirectory(AdminRESTEndpoint):
    """Class to handle objects in service directory.

    See AdminRESTEndpoint class for arguments.
    """

    @property
    def _permissionsURL(self):
        return self.url + '/permissions'

    @property
    def permissions(self):
        """Returns permissions for service."""
        perms = self.request(self._permissionsURL).get(PERMISSIONS, [])
        return [Permission(r) for r in perms]

    @passthrough
    def addPermission(self, principal='', isAllowed=True, private=True):
        """Adds a permission.

        Args:
            principal: Optional name of the role whome the permission is being
                assigned. Defaults to ''.
            isAllowed: Optional boolean, tells if a resource is allowed or denied.
                Defaults to True.
            private: Optional boolean, default is True. Secures service by making
                private, denies public access. Change to False to allow public access.

        Returns:
            A list of the added permissions.
        """
        method = POST
        add_url = self._permissionsURL + '/add'
        added_permissions = []
        if principal:
            params = {PRINCIPAL: principal, IS_ALLOWED: isAllowed}
            r = self.request(add_url, params, method=method)

            for k,v in six.iteritems(params):
                r[k] = v
            added_permissions.append(r)

        if principal != ESRI_EVERYONE:
            params = {PRINCIPAL: ESRI_EVERYONE, IS_ALLOWED: FALSE}

            if private:
                r = self.request(add_url, params, method=method)
            else:
                params[IS_ALLOWED] = TRUE
                r = self.request(add_url, params, method=method)

            for k,v in six.iteritems(params):
                r[k] = v
            added_permissions.append(r)

        return added_permissions

    @passthrough
    def hasChildPermissionsConflict(self, principal, permission=None):
        """Checks if service has conflicts with opposing permissions.

        Args:
            principal: Name of role for which to check for permission conflicts.
            permission: Optional JSON permission object. Defaults to None.

        permission example:
            permission: {"isAllowed": True, "constraint": ""}

        Returns:
            Post request.
        """

        if not permission:
            permission = {IS_ALLOWED: True, CONSTRAINT: ""}

        query_url = self._permissionsURL + '/hasChildPermissionConflict'
        params = {PRINCIPAL: principal, PERMISSION: permission}
        return self.request(query_url, params)

    def report(self):
        """Returns a report for resource."""
        return [Report(r) for r in self.request(self.url + '/report')['reports']]


class BaseResource(JsonGetter):
    """Base resource class.

    Attribute:
        json: JSON object from input JSON.
    """

    def __init__(self, in_json):
        """inits class with json object.

        Args:
            in_json: Input JSON object.
        """

        self.json = munch.munchify(in_json)
        super(BaseResource, self).__init__()


class EditableResource(JsonGetter):
    """Class that handles editable resources."""
    def __getitem__(self, name):
        """Dict like access to json definition."""
        if name in self.json:
            return self.json[name]

    def __getattr__(self, name):
        """Gets normal class attributes and json abstraction at object level."""
        try:
            # it is a class attribute
            return object.__getattribute__(self, name)
        except AttributeError:
            # it is in the json definition
            if name in self.json:
                return self.json[name]
            elif name =='client':
                pass
            else:
                raise AttributeError(name)

    def __setattr__(self, name, value):
        """Properly sets attributes for class as well as json abstraction."""
        # make sure our value is a Munch if dict
        if isinstance(value, (dict, list)) and name != 'response':
            value = munch.munchify(value)
        try:
            # set existing class property, check if it exists first
            object.__getattribute__(self, name)
            object.__setattr__(self, name, value)
        except AttributeError:
            # set in json definition
            if name in self.json:
                self.json[name] = value
            elif name == 'client':
                pass
            else:
               raise AttributeError(name)

class Report(BaseResource):
    pass

class ClusterMachine(BaseResource):
    pass

class Permission(BaseResource):
    pass

class SSLCertificate(AdminRESTEndpoint):
    """Class to handle SSL Certificate."""
    pass

class Machine(AdminRESTEndpoint):
    """Class to handle ArcGIS Server Machine."""
    pass

class DataItem(BaseResource):
    """Class that handles data items."""
    @passthrough
    def makePrimary(self, machineName):
        """Promotes a standby machine to the primary data store machine. The
                existing primary machine is downgraded to a standby machine

        Args:
            machineName: Name of machine to make primary.

        Returns:
            Post request.
        """

        query_url = self.url + '/machines/{}/makePrimary'.format(machineName)
        return self.request(query_url, method=POST)

    def validateDataStore(self, machineName):
        """Ensures that the data store is valid.

        Args:
            machineName: Name of machine to validate data store against.
        """

        query_url = self.url + '/machines/{}/validate'.format(machineName)
        return self.request(query_url, method=POST)


class Item(AdminRESTEndpoint):
    """This resource represents an item that has been uploaded to the server. Various
            workflows upload items and then process them on the server. For example,
            when publishing a GIS service from ArcGIS for Desktop or ArcGIS Server
            Manager, the application first uploads the service definition (.SD)
            to the server and then invokes the publishing geoprocessing tool to
            publish the service.

    Each uploaded item is identified by a unique name (itemID). The pathOnServer
            property locates the specific item in the ArcGIS Server system directory.

    The committed parameter is set to true once the upload of individual parts is complete.
    """

    def __init__(self, url, usr='', pw='', token=''):
        """Inits class with credentials for server.

        Args:
            url: Server URL.
            usr: Username for login.
            pw: Password for login.
            token: Token for URL/login.
        """

        super(Item, self).__init__(url, usr, pw, token)
        pass

class PrimarySiteAdministrator(AdminRESTEndpoint):
    """Primary Site Administrator object."""

    @passthrough
    def disable(self):
        """Disables the primary site administartor account."""
        query_url = self.url + '/disable'
        return self.request(query_url, method=POST)

    @passthrough
    def enable(self):
        """Enables the primary site administartor account."""
        query_url = self.url + '/enable'
        return self.request(query_url, method=POST)

    @passthrough
    def update(self, username, password):
        """Updates the primary site administrator account.

        Args:
            username: New username for PSA (optional in REST API, required here
                for your protection).
            password: New password for PSA.
        """
        query_url = self.url + '/update'

        params = {'username': username,
                  'password': password}

        return self.request(query_url, params, method=POST)

    def __bool__(self):
        """Returns True if PSA is enabled."""
        return not self.disabled

class RoleStore(AdminRESTEndpoint):
    """Role Store object."""

    @property
    def specialRoles(self):
        return self.request(self.url + '/specialRoles').get('specialRoles')

    @passthrough
    def addRole(self, rolename, description=''):
        """Adds a role to the role store.

        Args:
            rolename: Name of role to add.
            description: Optional description for new role.
        """

        query_url = self.url + '/add'
        params = {
            'rolename': rolename,
            'description': description or rolename,
        }

        return self.request(query_url, params, method=POST)

    def getRoles(self, startIndex='', pageSize=1000):
        """This operation gives you a pageable view of roles in the role store.
                It is intended for iterating through all available role accounts.
                To search for specific role accounts instead, use the searchRoles()
                method. <- from Esri help

        Args:
            startIndex: Optional, zero-based starting index from roles list.
            pageSize: Optional maximum number of roles to return. Default is 1000.
        """
        query_url = self.url + '/getRoles'

        params = {'startIndex': startIndex,
                  'pageSize': pageSize}

        return self.request(query_url, params, method=POST)

    def searchRoles(self, filter='', maxCount=''):
        """Searches the role store.

        Args:
            filter: Optional filter string for roles (ex: "editors").
            maxCount: Optional aximimum number of records to return.
        """
        query_url = self.url + '/search'

        params = {'filter': filter,
                  'maxCount': maxCount}

        return self.request(query_url, params, method=POST)

    @passthrough
    def removeRole(self, rolename):
        """Removes a role from the role store.

        Args:
            rolename : Name of role.
        """

        query_url = self.url + '/remove'
        return self.request({'rolename':rolename}, method=POST)

    @passthrough
    def updateRole(self, rolename, description=''):
        """Updates a role.

        Args:
            rolename: Name of the role.
            description: Optional description of role.
        """

        query_url = self.url + '/update'

        params = {'rolename': rolename,
                  'description': description}

        return self.request(query_url, params, method=POST)

    @passthrough
    def getRolesForUser(self, username, filter='', maxCount=100):
        """Returns the privilege associated with a user.

        Args:
            username: Name of user.
            filter: Optional filter to applied to resultant role set.
            maxCount: Optional max number of roles to return. Defaults to 100.
        """

        query_url = self.url + '/getRolesForUser'
        params = {'username': username,
                  'filter': filter,
                  'maxCount': maxCount}

        return self.request(query_url, params, method=POST)

    @passthrough
    def getUsersWithinRole(self, rolename, filter='', maxCount=100):
        """Gets all user accounts to whom this role has been assigned.

        Args:
            rolename: Name of role.
            filter: Optional filter to be applied to the resultant user set.
            maxCount: Maximum number of results to return. Defaults to 100.
        """

        query_url = self.url + '/getUsersWithinRole'
        params = {'rolename': rolename,
                  'filter': filter,
                  'maxCount': maxCount}

        return self.request(query_url, params, method=POST)

    @passthrough
    def addUsersToRole(self, rolename, users):
        """Assigns a role to multiple users with a single action.

        Args:
            rolename: Name of role.
            users: List of users or comma separated list.
        """

        query_url = self.url + '/addUsersToRole'

        if isinstance(users, (list, tuple)):
            users = ','.join(map(str, users))

        params = {'rolename': rolename,
                  'users': users}

        return self.request(query_url, params, method=POST)

    @passthrough
    def removeUsersFromRole(self, rolename, users):
        """Removes a role assignment from multiple users.

        Args:
            rolename: Name of role.
            users : List or comma separated list of user names.
        """

        query_url = self.url + '/removeUsersFromRole'

        if isinstance(users, (list, tuple)):
            users = ','.join(map(str, users))

        params = {'rolename': rolename,
                  'users': users}

        return self.request(query_url, params, method=POST)

    @passthrough
    def assignPrivilege(self, rolename, privilege='ACCESS'):
        """Assigns administrative acess to ArcGIS Server.

        Args:
            rolename: Name of role.
            privilege: Administrative capability to assign
                (ADMINISTER | PUBLISH | ACCESS). Defaults to 'ACCESS'.
        """

        query_url = self.url + '/assignPrivilege'

        params = {'rolename': rolename,
                  'privilege': privilege.upper()}

        return self.request(query_url, params, method=POST)

    @passthrough
    def getPrivilegeForRole(self, rolename):
        """Gets the privilege associated with a role.

        Args:
            rolename: Name of role.
        """

        query_url = self.url + '/getPrivilege'
        return self.request(query_url, {'rolename':rolename}, method=POST)

    @passthrough
    def getRolesByPrivilege(self, privilege):
        """Returns the privilege associated with a user.

        Args:
            privilege: Name of privilege (ADMINISTER | PUBLISH).
        """

        query_url = self.url + '/getRolesByPrivilege'
        return self.request(query_url, {'privilege': privilege.upper()}, method=POST)

    def __iter__(self):
        """Makes iterable."""
        for role in self.getRoles():
            yield role


class UserStore(AdminRESTEndpoint):
    """User Store object."""

    @passthrough
    def addUser(self, username, password, fullname='', description='', email=''):
        """Adds a user account to user store.

        Args:
            username: Username for new user.
            password: Password for new user.
            fullname: Optional full name of user.
            description: Optional description for user.
            email: Optional email address for user account.
        """

        query_url = self.url + '/add'
        params = {'username': username,
                  'password': password,
                  'fullname': fullname,
                  'description': description,
                  'email': email}

        return self.request(query_url, params, method=POST)

    @passthrough
    def getUsers(self, startIndex='', pageSize=''):
        """Gets all users in user store, intended for iterating over all user
                accounts

        Args:
            startIndex: Optional zero-based starting index from roles list.
            pageSize: Optional size for page.
        """
        query_url = self.url + '/getUsers'

        params = {'startIndex': startIndex,
                  'pageSize': pageSize}

        return self.request(query_url, params, method=POST)

    def searchUsers(self, filter='', maxCount=''):
        """Searches the user store, returns User objects.

        Args:
        filter: Optional filter string for users (ex: "john").
        maxCount: Maximimum number of records to return.
        """

        query_url = self.url + '/search'

        params = {'filter': filter,
                  'maxCount': maxCount}

        return self.request(query_url, params, method=POST)

    @passthrough
    def removeUser(self, username):
        """Removes a user from the user store.

        Args:
            username: Name of user to remove.
        """

        query_url = self.url + '/remove'
        return self.request(query_url, {'username':username}, method=POST)

    @passthrough
    def updateUser(self, username, password, fullname='', description='', email=''):
        """updates a user account in the user store

        Args:
            username: Username for new user.
            password: Password for new user.
            fullname: Optional full name of user.
            description: Optional description for user.
            email: Optional email address for user account.
        """

        query_url = self.url + '/update'
        params = {
            'username': username,
            'password': password
        }
        opts = {
            'fullname': fullname,
            'description': description,
            'email': email
        }
        for k,v in six.iteritems(opts):
            if v:
                params[k] = v

        return self.request(query_url, params, method=POST)

    @passthrough
    def assignRoles(self, username, roles):
        """Assigns role to user to inherit permissions of role.

        Args:
            username: Name of user.
            roles: List or comma separated list of roles.
        """
        query_url = self.url + '/assignRoles'

        if isinstance(roles, (list, tuple)):
            roles = ','.join(map(str, roles))

        params = {'username': username,
                  'roles': roles}

        return self.request(query_url, params, method=POST)

    @passthrough
    def removeRoles(self, username, rolenames):
        """Removes roles that have been previously assigned to a user account,
                only supported when role store supports reads and writes.

        Args:
            username: Name of the user.
            roles: List or comma separated list of role names.
        """

        query_url = self.url + '/removeRoles'

        if isinstance(roles, (list, tuple)):
            roles = ','.join(map(str, roles))

        params = {'username': username,
                  'roles': roles}

        return self.request(query_url, params, method=POST)

    @passthrough
    def getPrivilegeForUser(self, username):
        """Gets the privilege associated with a role.

        Args:
            username: Name of user.
        """

        query_url = self.url + '/getPrivilege'
        params = {'username': username}
        return self.request(query_url, params, method=POST)

    def __iter__(self):
        """Makes iterable."""
        for user in self.getUsers():
            yield user

class DataStore(AdminRESTEndpoint):
    """Class to handle Data Store operations"""

    @passthrough
    def config(self):
        """Returns configuration properties."""
        return self.request(self.url + '/config')

    # not available in ArcGIS REST API out of the box, included here to refresh data store cache
    def getItems(self):
        """Returns a refreshed list of all data items."""
        items = []
        for it in self.getRootItems():
            items += self.findItems(it)
        return items

    @passthrough
    def registerItem(self, item):
        """Registers an item with the data store.

        Args:
            item: JSON representation of new data store item to register.

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
            return self.request(query_url, params={'item': item}, method=POST)

        return None

    @passthrough
    def unregisterItem(self, itemPath, force=True):
        """Unregisters an item with the data store.

        Args:
            itemPath: Path to data item to unregister (DataItem.path).
            force: Added at 10.4, must be set to True.
        """

        query_url = self.url + '/unregisterItem'
        return self.request(query_url, {'itemPath': itemPath, 'force': force}, method=POST)

    def findItems(self, parentPath, ancestorPath='', types='', id=''):
        """Searches through items registered in data store.

        Args:
            parentPath: Path of parent under which to find items.
            ancestorPath: Optional path of ancestor which to find items.
            types: Optional filter for the type of items to search.
            id: Optional filter to search the ID of the item.

        Returns:
            Data items under the parent.
        """

        query_url = self.url + '/findItems'
        params = {'parentPath': parentPath,
                  'ancestorPath': ancestorPath,
                  'types': types,
                  'id': id}

        ds_items = self.request(query_url, params)['items']
        for d in ds_items:
            d['url'] = '{}/items{}'.format(self.url, d['path'])

        return [DataItem(d) for d in ds_items]

    def validateItem(self, item):
        """Validates a data store item.

        Args:
            item: JSON representation of new data store item to validate.

        Returns:
            Boolean, True if item is validated.
        """

        query_url = self.url + '/validateDataItem'
        r = self.request(query_url, {'item': item}, method=POST)
        if 'status' in r and r['status'] == 'success':
            return True
        else:
            print(json.dumps(r, indent=2, sort_keys=True))
            return False

    @passthrough
    def validateAllDataItems(self):
        """Validates all data items in data store.  Warning, this operation can be
                VERY time consuming, depending on how many items are registered
                with the data store.
        """

        return self.request(self.url + '/validateAllDataItems', method=POST)

    def computeRefCount(self, path):
        """Returns the total number of references to a given data item that exists
                on the server. Can be used to determine if a data resource can
                be safely deleted or taken down for maintenance.

        Args:
            path: Path to resource on server (DataItem.path).
        """

        query_url = self.url + '/computeTotalRefCount'
        r  = passthrough(self.request(query_url, {'path': path}))
        return int(r['totalRefCount'])

    def getRootItems(self):
        """Methods to get all data store items at the root."""
        return self.request(self.url + '/items')['rootItems']

    @passthrough
    def startMachine(self, dataItem, machineName):
        """Starts the database instance running on the data store machine.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to validate data store against.
        """

        query_url = self.url + '/items/{}/machines/{}/start'.format(dataItem, machineName)
        return self.request(query_url, method=POST)

    @passthrough
    def stopMachine(self, dataItem, machineName):
        """Starts the database instance running on the data store machine.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to validate data store against.
        """

        query_url = self.url + '/items/{}/machines/{}/stop'.format(dataItem, machineName)
        return self.request(query_url, method=POST)

    @passthrough
    def removeMachine(self, dataItem, machineName):
        """Removes a standby machine from the data store, this operation is not
                supported on the primary data store machine.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to validate data store against.
        """
        query_url = self.url + '/items/{}/machines/{}/remove'.format(dataItem, machineName)
        return self.request(query_url, method=POST)

    @passthrough
    def makePrimary(self, dataItem, machineName):
        """Promotes a standby machine to the primary data store machine. The
                existing primary machine is downgraded to a standby machine.

        Required:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to make primary.
        """
        query_url = self.url + '/items/{}/machines/{}/makePrimary'.format(dataItem, machineName)
        return self.request(query_url, method=POST)

    def validateDataStore(self, dataItem, machineName):
        """Ensures that the data store is valid.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to validate data store against.
        """

        query_url = self.url + '/items/{}/machines/{}/validate'.format(dataItem, machineName)
        return self.request(query_url, method=POST)

    @passthrough
    def updateDatastoreConfig(self, datastoreConfig={}):
        """Updates data store configuration. Can use this to allow or block
                automatic copying of data to server at publish time.

        Args:
            datastoreConfig: Optional JSON object representing datastoreConfiguration.
                If none supplied, it will default to disabling copying data locally
                to the server. Defaults to {}.
        """

        query_url = self.url + '/config/update'
        if not datastoreConfig:
            datastoreConfig = '{"blockDataCopy":"true"}'
        return self.request(query_url, {'datastoreConfig': datastoreConfig}, method=POST)

    def __iter__(self):
        """Makes iterable."""
        for item in self.getItems():
            yield item

    def __repr__(self):
        return '<ArcGIS DataStore>'

class Cluster(AdminRESTEndpoint):
    """Class to handle Cluster object."""

    @property
    def machines(self):
        """Returns all server machines participating in the cluster."""
        return [Machine(**r) for r in self.request(self.url + '/machines')]

    @property
    def services(self):
        """Gets a list of all services in the cluster."""
        return self.request(self.url + '/services')['services']

    @passthrough
    def start(self):
        """Starts the cluster."""
        return self.request(self.url + '/start', method=POST)

    @passthrough
    def stop(self):
        """Stops the cluster"""
        return self.request(self.url + '/stop', method=POST)

    @passthrough
    def delete(self):
        """Deletes the cluster configuration. All machines in cluster will be
                stopped and returened to pool of registered machines. All GIS
                services in cluster are stopped.
        """

        return self.request(self.url + '/delete', method=POST)

    @passthrough
    def editProtocol(self, clusterProtocol):
        """Edits the cluster protocol.  Will restart the cluster with updated protocol.
                The clustering protocol defines a channel which is used by server
                machines within a cluster to communicate with each other. A server
                machine will communicate with its peers information about the
                status of objects running within it for load balancing and default
                tolerance.

        ArcGIS Server supports the TCP clustering protocols where server machines communicate
        with each other over a TCP channel (port).

        Args:
            clusterProtocol: JSON object representing the cluster protocol TCP port.

        Example:
            clusterProtocol = {"tcpClusterPort":"4014"}
        """

        query_url = self.url + '/editProtocol'
        params = {'clusterProtocol': clusterProtocol}

        return self.request(query_url, params, method=POST)

    @passthrough
    def addMachines(self, machineNames):
        """Adds machines to cluster. Machines need to be registered with the site
        before they can be added.

        Args:
            machineNames: List or comma-separated list of machine names.

        Examples:
            machineNames= "SERVER2.DOMAIN.COM,SERVER3.DOMAIN.COM"
        """

        query_url = self.url + '/machines/add'
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return self.request(query_url, {'machineNames': machineNames}, method=POST)

    @passthrough
    def removeMachines(self, machineNames):
        """Removes machine names from cluster.

        Args:
            machineNames: List or comma-separated list of machine names.

        Examples:
            machineNames= "SERVER2.DOMAIN.COM,SERVER3.DOMAIN.COM"
        """

        query_url = self.url + '/machines/remove'
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return self.request(query_url, {'machineNames': machineNames}, method=POST)


class Folder(BaseDirectory):
    """Class to handle simple folder objects."""

    def __str__(self):
        """Folder name"""
        return self.folderName

    def list_services(self):
        """Returns services within folder."""
        return ['.'.join([s.serviceName, s.type]) for s in self.services]

    def iter_services(self):
        """Iterates through folder and returns Service Objects."""
        for service in self.services:
            serviceUrl = '.'.join(['/'.join([self.url, service.serviceName]), service.type])
            yield Service(serviceUrl)

    @passthrough
    def delete(self):
        """Deletes the folder."""
        query_url = self.url + '/deleteFolder'
        return self.request(query_url, method=POST)

    @passthrough
    def edit(self, description, webEncrypted):
        """Edits a folder.

        Args:
            description: Folder description.
            webEncrypted: Boolean to indicate if the servies are accessible
                over SSL only.
        """

        query_url = self.url + '/editFolder'
        params = {'description': description, 'webEncrypted': webEncrypted}
        return self.request(query_url, params, method=POST)

    def __getitem__(self, i):
        """Gets service by index"""
        return self.services[i]

    def __iter__(self):
        """Iterates through list of services."""
        for s in self.services:
            yield s

    def __len__(self):
        """Returns number of services in folder."""
        return len(self.services)

    def __nonzero__(self):
        """Returns True if services are present."""
        return bool(len(self))

class Service(BaseDirectory, EditableResource):
    """Class to handle internal ArcGIS Service instance all service properties
            are accessed through the service's json property.  To get full list
            print() Service.json or Service.print_info().

    Attributes:
        fullName: List of full URL name.
        serviceName: Service name that is derived from fullName.
    """

    url = None
    raw_response = None
    response = None
    token = None
    fullName = None
    elapsed = None
    serviceName = None
    json = {}

    def __init__(self, url, usr='', pw='', token='', client=None):
        """Initializes with json definition plus additional attributes.

        Args:
            url: URL.
            usr: Username for login.
            pw: Password for login.
            token: Token for URL login.
        """

        super(Service, self).__init__(url, usr, pw, token, client=client)
        self.fullName = self.url.split('/')[-1]
        self.serviceName = self.fullName.split('.')[0]

    @property
    def name(self):
        """property alias for serviceName"""
        return self.serviceName

    @property
    def enabledExtensions(self):
        """Returns list of enabled extensions, not available out of the box in
                the REST API.
        """

        return [e.typeName for e in self.extensions if str(e.enabled).lower() == 'true']

    @property
    def disabledExtensions(self):
        """Returns list of disabled extensions, not available out of the box
                in the REST API.
        """

        return [e.typeName for e in self.extensions if str(e.enabled).lower() == 'false']

    @property
    def status(self):
        """Returns status JSON object for service."""
        return munch.munchify(self.request(self.url + '/status'))

    @passthrough
    def enableExtensions(self, extensions):
        """Enables an extension, this operation is not available through REST API
                out of the box.


        Args:
            extensions: Name of extension(s) to enable.  Valid options are:

            NAServer|MobileServer|KmlServer|WFSServer|SchematicsServer|FeatureServer|WCSServer|WMSServer

        Returns:
            A dictionary containing the statuses of the extensions.
        """

        if isinstance(extensions, six.string_types):
            extensions = extensions.split(';')
        editJson = self.response
        exts = [e for e in editJson['extensions'] if e['typeName'].lower() in map(lambda x: x.lower(), extensions)]
        status = {}
        for ext in exts:
            if ext['enabled'] in ('true', True):
                status[ext['typeName']] = 'Already Enabled!'
            else:
                ext['enabled'] = 'true'
                status[ext['typeName']] = 'Enabled'

        if 'Enabled' in status.values():
            retStatus =  self.edit(editJson)
            for k,v in six.iteritems(retStatus):
                status[k] = v

        return status

    @passthrough
    def disableExtensions(self, extensions):
        """Disables an extension, this operation is not available through REST API
                out of the box.

        Args:
            extensions: Name of extension(s) to disable.  Valid options are:

            NAServer|MobileServer|KmlServer|WFSServer|SchematicsServer|FeatureServer|WCSServer|WMSServer

        Returns:
            A dictionary containing the statuses of the extensions.
        """

        if isinstance(extensions, six.string_types):
            extensions = extensions.split(';')
        editJson = self.response
        exts = [e for e in editJson['extensions'] if e['typeName'].lower() in map(lambda x: x.lower(), extensions)]
        status = {}
        for ext in exts:
            if ext['enabled'] in ('false', False):
                status[ext['typeName']] = 'Already Disabled!'
            else:
                ext['enabled'] = 'false'
                status[ext['typeName']] = 'Disabled'

        if 'Disabled' in status.values():
            retStatus =  self.edit(editJson)
            for k,v in six.iteritems(retStatus):
                status[k] = v

        return status

    @passthrough
    def start(self):
        """Starts the service."""
        r = {}
        if self.configuredState.lower() == 'stopped':
            r = self.request(self.url + '/start', method=POST)
            if 'success' in r:
                print('started: {}'.format(self.fullName))
            self.refresh()
        else:
            print('"{}" is already started!'.format(self.fullName))
        return r

    @passthrough
    def stop(self):
        """Stops the service."""
        r = {}
        if self.configuredState.lower() == 'started':
            r = self.request(self.url + '/stop', method=POST)
            if 'success' in r:
                print('stoppedd: {}'.format(self.fullName))
            self.refresh()
        else:
            print('"{}" is already stopped!'.format(self.fullName))
        return r

    @passthrough
    def restart(self):
        """Restarts the service."""
        verb = VERBOSE
        VERBOSE = False
        self.stop()
        self.start()
        VERBOSE = verb
        return {'status': 'success'}

    @passthrough
    def edit(self, serviceJSON={}, **kwargs):
        """Edits the service, properties that can be edited vary by the service
                type.

        Args:
            serviceJSON: Optional JSON representation of service with edits.
                Defaults to {}.
            kwargs: List of keyword arguments, you can use these if there are just a
                few service options that need to be updated.  It will grab the rest of
                the service info by default.
        """
        if not serviceJSON:
            serviceJSON = self.json

        # update by kwargs
        for k,v in six.iteritems(kwargs):
            serviceJSON[k] = v
        params = {'service': serviceJSON}
        r = self.request(self.url + '/edit', params, method=POST)
        self.refresh()

    @passthrough
    def delete(self):
        """Deletes the service, proceed with caution."""
        r = self.request(self.url + '/delete', method=POST)
        self.response = None
        self.url = None
        return r

    def itemInfo(self):
        """Gets service metadata."""
        query_url = self.url + '/iteminfo'
        return self.request(query_url)

    @passthrough
    def editItemInfo(self, itemInfo, thumbnailFile=None):
        """Edits the itemInfo for service.

        Args:
            itemInfo: JSON itemInfo objet representing metadata.
            thumbnailFile: Path to optional thumbnail image, defaults to None.
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

        return self.request(query_url, params, files=files, method=POST).json()

    @passthrough
    def uploadItemInfo(self, folder, file):
        """Uploads a file associated with the item information the server;
                placed in directory specified by folder parameter.

        Args:
            folder: Name of the folder to which the file will be uploaded.
            file: Full path to file to be uploaded to server.
        """

        query_url = self.url + '/iteminfo/upload'
        return self.request(query_url, {'folder': folder, 'file':file}, method=POST)

    @passthrough
    def deleteItemInformation(self):
        """Deletes information about the service, configuration is not changed."""
        query_url = self.url + '/iteminfo/delete'
        return self.request(query_url, method=POST)

    def manifest(self):
        """Gets service manifest. This  documents the data and other resources
                that define the service origins and power the service."""
        query_url = self.url + '/iteminfo/manifest/manifest.json'
        return BaseResource(self.request(query_url))

    def statistics(self):
        """Returns service statistics object."""
        return munch.munchify(**self.request(self.url + '/statistics'))

    #**********************************************************************************
    #
    # helper methods not available out of the box
    def getExtension(self, extension):
        """Returns an extension by name.

        Args:
            extension: Name of extension (not case sensitive).
        """

        try:
            return [e for e in self.extensions if e.typeName.lower() == extension.lower()][0]
        except IndexError:
            return None

    def setExtensionProperties(self, extension, **kwargs):
        """Helper method to set extension properties by name and keyword arguments.

        Args:
            extension: Name of extension (not case sensitive).
            **kwargs: Optional keyword arguments to set properties for.

        example:
            # set capabilities for feature service extension
            Service.setExtensionProperties('featureserver', capabilities:'Create,Update,Delete')
        """

        ext = self.getExtension(extension)
        if ext is not None:
            for k,v in six.iteritems(kwargs):
                if k in ext:
                    setattr(ext, k, v)

            self.edit()

    def __repr__(self):
        """Shows service name."""
        if self.url is not None:
            return '<Service: {}>'.format(self.url.split('/')[-1])



class ArcServerAdmin(AdminRESTEndpoint):
    """Class to handle internal ArcGIS Server instance.

    Attributes:
        service_cache: List of the service cache.
        psa: Primary Site Administrator object
        roleStore: Store of the roles for server.
        userStore: Store of the users for server.
        dataStore: Store of data from server.
    """

    def __init__(self, url, usr='', pw='', token=''):
        """Inits class with login info/credentials.

        Args:
            url: URL for server.
            usr: Username for login.
            pw: Password for login.
            token: Token for URL/login.
        """

        #possibly redundant validation...
        if not 'arcgis' in url.lower():
            url += '/arcgis'
        url = url.split('/arcgis')[0] + '/arcgis/admin/services'
        super(ArcServerAdmin, self).__init__(url, usr, pw, token)
        self._serverRoot = self.url.split('/arcgis')[0] + '/arcgis'
        self._adminURL = self._serverRoot + '/admin'
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
        self.service_cache = []
        self.psa = PrimarySiteAdministrator(self._securityURL + '/psa')
        self.roleStore = RoleStore(self._securityURL + '/roles')
        self.userStore = UserStore(self._securityURL + '/users')
        self.dataStore =  DataStore(self._dataURL)

    #----------------------------------------------------------------------
    # general methods and properties
    @property
    def machines(self):
        """Returns machines."""
        return munch.munchify(self.request(self._machinesURL))

    @property
    def clusters(self):
        """Gets a list of cluster objects."""
        return self.request(self._clusterURL)

    @property
    def types(self):
        """Gets a list of all server service types and extensions (types)."""
        return self.request(self._servicesURL + '/types')

    @property
    def publicKey(self):
        """This resource returns the public key of the server that can be
                used by a client application (or script) to encrypt data sent to
                the server using the RSA algorithm for public-key encryption. In
                addition to encrypting the sensitive parameters, the client is
                also required to send to the server an additional flag encrypted
                with value set to true.
        """
        return self.request(self.url + '/publicKey')

    def cluster(self, clusterName):
        """Returns a Cluster object.

        Args:
            clusterName: Name of cluster to connect to.
        """

        return Cluster(self.request(self._clusterURL + '/{}'.format(clusterName)))

    def list_services(self):
        """Returns a list of fully qualified service names."""
        services = ['/'.join([self._servicesURL,
                    '.'.join([serv['serviceName'], serv['type']])])
                    for serv in self.response['services']]

        for f in self.folders:
            folder = Folder(self._servicesURL + '/{}'.format(f))
            for service in folder.list_services():
                services.append('/'.join(map(str, [self._servicesURL, folder, service])))

        self.service_cache = services
        return services

    def iter_services(self):
        """Iterates through Service Objects."""
        if not self.service_cache:
            self.list_services()
        for serviceName in self.service_cache:
            yield self.service(serviceName)

    def rehydrateServices(self):
        """Reloads response to get updated service list."""
        self.refresh()
        return self.list_services()

    #----------------------------------------------------------------------
    # clusters
    @passthrough
    def createCluster(self, clusterName, machineNames, topCluserPort):
        """Creates a new cluster on ArcGIS Server Site.

        Args:
            clusterName: Name of new cluster.
            machineNames: Comma separated string of machine names or list.
            topClusterPort: TCP port number used by all servers to communicate with eachother.
        """

        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        params = {'clusterName': clusterName,
                  'machineNames': machineNames,
                  'topClusterPort': topCluserPort}

        return self.request(self._clusterURL + '/create', params, method=POST)

    def getAvailableMachines(self):
        """Lists all server machines that don't participate in a cluster and are
                available to be added to a cluster (i.e. registered with server.
        """

        query_url = self.url.split('/clusters')[0] + '/clusters/getAvailableMachines'
        return self.request(query_url)['machines']

    @passthrough
    def startCluster(self, clusterName):
        """Starts a cluster.

        Args:
            clusterName: Name of cluster to start.
        """

        self._clusterURL + '/{}/start'.format(clusterName)
        return self.request(query_url, method=POST)

    @passthrough
    def stopCluster(self, clusterName):
        """Stops a cluster.

        Args:
            clusterName: Name of cluster to stop.
        """

        self._clusterURL + '/{}/stop'.format(clusterName)
        return self.request(query_url, method=POST)

    @passthrough
    def editProtocol(self, clusterName, clusterProtocol):
        """Edits the cluster protocol.  Will restart the cluster with updated protocol.
                The clustering protocol defines a channel which is used by server
                machines within a cluster to communicate with each other. A server
                machine will communicate with its peers information about the
                status of objects running within it for load balancing and default
                tolerance.

        ArcGIS Server supports the TCP clustering protocols where server machines communicate
                with each other over a TCP channel (port).

        Args:
            clusterName: Name of cluster.
            clusterProtocol: JSON object representing the cluster protocol TCP port.

        Example:
            clusterProtocol: {"tcpClusterPort":"4014"}
        """

        query_url = self._clusterURL + '/{}/editProtocol'.format(clusterName)
        params = {'clusterProtocol': clusterProtocol}

        return self.request(query_url, params, method=POST)

    @passthrough
    def deleteCluster(self, clusterName):
        """Deletes a cluster.

        Args:
            clusterName: Cluster to be deleted.
        """

        query_url = self._clusterURL + '/{}/delete'.format(clusterName)
        self.request(query_url, {'clusterName': clusterName}, method=POST)

    def getMachinesInCluster(self, clusterName):
        """Returns a list all server machines participating in a cluster.

        Args:
            clusterName: Name of cluster.
        """

        query_url = self._clusterURL + '/{}/machines'.format(clusterName)
        return [ClusterMachine(r) for r in self.request(query_url)]

    def getServicesInCluster(self, clusterName):
        """Gets a list of all services in a cluster.

        Args:
            clusterName: Name of cluster to search for services.
        """

        query_url = self._clusterURL+ '{}/services'.format(clusterName)
        return self.request(query_url).get('services', [])

    @passthrough
    def addMachinesToCluster(self, clusterName, machineNames):
        """Adds new machines to site. Machines must be registered beforehand.

        Args:
            cluster: Cluster name.
            machineNames: Comma separated string of machine names or list.
        """

        query_url = self._clusterURL + '{}/add'.format(clusterName)
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return self.request(query_url, {'machineNames': machineNames}, method=POST)

    @passthrough
    def removeMachinesFromCluster(self, clusterName, machineNames):
        """Removes machine names from cluster.

        Args:
            clusterName: Name of cluster.
            machineNames: List or commaseparated list of machine names.

            Examples:
                machineNames: "SERVER2.DOMAIN.COM,SERVER3.DOMAIN.COM"
        """

        query_url = self._clusterURL + '/{}/machines/remove'.format(clusterName)
        if isinstance(machineNames, (list, tuple)):
            machineNames = ','.join(machineNames)

        return self.request(query_url, {'machineNames': machineNames}, method=POST)

    #----------------------------------------------------------------------
    # data store.  To use all data store methods connect to data store
    # example:
    # ags = restapi.admin.ArcServerAdmin(url, usr, pw)
    # ds = ags.dataStore <- access all data store methods through ds object

    @passthrough
    def config(self):
        """Returns configuration properties."""
        return self.request(self._dataURL + '/config')

    # not available in ArcGIS REST API, included here to refresh data store cache
    def getDataItems(self):
        """Returns a refreshed list of all data items."""
        items = []
        for it in self.getRootItems():
            items += self.findDataItems(it)
        return items

    @passthrough
    def registerDataItem(self, item):
        """Registers an item with the data store.

        Args:
            item: JSON representation of new data store item to register.

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
        return self.dataStore.registerItem(item)

    @passthrough
    def unregisterDataItem(self, itemPath):
        """Unregisters an item with the data store.

        Args:
            itemPath: Path to data item to unregister (DataItem.path).
        """

        return self.dataStore.unregisterItem(itemPath)

    def findDataItems(self, parentPath, ancestorPath='', types='', id=''):
        """Searches through items registered in data store.

        Args:
            parentPath: Path of parent under which to find items.
            ancestorPath: Optional path of ancestor which to find items.
            types: Optional filter for the type of items to search.
            id: Optional filter to search the ID of the item.
        """

        return self.dataStore.findItems(parentPath, ancestorPath, types, id)

    def validateDataItem(self, item):
        """Validates a data store item.

        Args:
            item: JSON representation of new data store item to validate.
        """

        return self.dataStore.validateItem(item)

    @passthrough
    def validateAllDataItems(self):
        """Validates all data items in data store.  Warning, this operation can be
                VERY time consuming, depending on how many items are registered
                with the data store.
        """

        return self.dataStore.validateAllDataItems()

    def computeRefCount(self, path):
        """Returns the total number of references to a given data item that
                exists on the server. Can be used to determine if a data resource
                can be  safely deleted or taken down for maintenance.

        Args:
            path: Path to resource on server (DataItem.path).
        """

        return self.dataStore.computeRefCount(path)

    def getRootItems(self):
        """Method to return all data store items at the root."""
        return self.dataStore.getRootItems()

    @passthrough
    def startDataStoreMachine(self, dataItem, machineName):
        """Starts the database instance running on the data store machine.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to validate data store against.
        """

        return self.dataStore.startMachine(dataItem, machineName)

    @passthrough
    def stopDataStoreMachine(self, dataItem, machineName):
        """Stops the database instance running on the data store machine.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to validate data store against.
        """

        return self.dataStore.stopMachine(dataItem, machineName)

    @passthrough
    def removeDataStoreMachine(self, dataItem, machineName):
        """Removes a standby machine from the data store, this operation is not
                supported on the primary data store machine.

        Args:
            dataItem: Name of data item (ex: enterpriseDatabases).
            machineName: Name of machine to remove.
        """

        return self.dataStore.removeMachine(dataItem, machineName)

    @passthrough
    def makeDataStorePrimaryMachine(self, dataItem, machineName):
        """Promotes a standby machine to the primary data store machine. The
                existing primary machine is downgraded to a standby machine.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to make primary.
        """

        return self.dataStore.makePrimary(dataItem, machineName)

    def validateDataStore(self, dataItem, machineName):
        """Ensures that the data store is valid.

        Args:
            dataItem: Name of data item (DataItem.path).
            machineName: Name of machine to validate data store against.
        """

        return self.dataStore.validateDataStore(dataItem, machineName)

    @passthrough
    def updateDatastoreConfig(self, datastoreConfig={}):
        """Updates data store configuration. Can use this to allow or block
                automatic copying of data to server at publish time.

        Args:
            datastoreConfig: Optional JSON object representing
                datastoreConfiguration. If none supplied, it will default to
                disabling copying data locally to the server.
        """

        return self.dataStore.updateDatastoreConfig(datastoreConfig)

    @passthrough
    def copyDataStore(self, other):
        """Copies data store from one data store to another.

        Returns:
            A list of the results.
        """

        if not isinstance(other, (self.__class__, DataStore)):
            raise TypeError('type: {} is not supported!'.format(type(other)))
        if isinstance(other, self.__class__):
            other = other.dataStore

        # existing items to skip duplicates
        ds = self.dataStore
        existing = []
        for item in ds:
            if item.type == 'folder':
                existing.append(item.info.path)
            elif item.type == 'egdb':
                existing.append(item.info.connectionString)

        # iterate through data store
        global VERBOSE
        results = []

        for d in other:
            if d.type in ['egdb', 'folder']:
                source = d.info.connectionString if d.type == 'egdb' else d.info.path
                if source not in existing:
                    ni = {
                        'path': d.path,
                        'type': d.type,
                        'info': d.info
                    }
                    if d.type == 'folder':
                        ni['clientPath'] = source
                    try:
                        st = ds.registerItem(ni)
                    except Exception as e:
                        st = { 'status': 'error', 'message': str(e) }
                    ni['result'] = st
                    results.append(ni)
                    if VERBOSE:
                        print(json.dumps(ni))
                else:
                    if VERBOSE:
                        print('skipping existing item: "{}" (type: {})'.format(d.path, d.type))

        return results

    #----------------------------------------------------------------------
    # LOGS
    @passthrough
    def logSettings(self):
        """Returns log settings."""
        query_url = self._logsURL + '/settings'
        return self.request(query_url).get('settings', [])

    @passthrough
    def editLogSettings(self, logLevel='WARNING', logDir=None, maxLogFileAge=90, maxErrorReportsCount=10):
        """Edits the log settings.

        Args:
            logLevel: Type of log [OFF, SEVERE, WARNING, INFO, FINE, VERBOSE, DEBUG].
                Default is 'WARNING'.
            logDir: Destination file path for root of log directories.
                Default is None.
            maxLogFileAge: Number of days for server to keep logs. Default is 90.
            maxErrorReportsCount: Maximum number of error report files per machine.
                Default is 10.
        """

        query_url = self._logsURL + '/settings/edit'
        if not logDir:
            logDir = r'C:\\arcgisserver\logs'

        params = {'logLevel': logLevel,
                  'logDir': logDir,
                  'maxLogFileAge': maxLogFileAge,
                  'maxErrorReportsCount': maxErrorReportsCount}

        return self.request(query_url, params, method=POST)

    def queryLogs(self, startTime='', endTime='', sinceLastStarted=False, level='WARNING', filter=None, pageSize=1000):
        """Queries all log reports accross an entire site.

        Args:*
            startTime: Optional arg for most recent time to query. Leave blank
                to start from now.
            endTime: Optional arg for oldest time to query. Defaults to ''.
            sinceLastStarted: Optional boolean to only return records since last
                time server was started. Defaults to False.
            level: Optional arg for log level [SEVERE, WARNING, INFO, FINE, VERBOSE, DEBUG].
                Default is 'WARNING'.
            filter: Optional filter. Filtering is allowed by any combination of
                services, server components, GIS server machines, or ArcGIS Data
                Store machines. The filter accepts a semi:colon delimited list
                of filter definitions. If any definition is omitted, it
                defaults to all.
            pageSize: Optional max number of records to return, default is 1000.

        startTime and endTime examples:
            as datetime:  datetime.datetime(2015, 7, 30)
            as a string: "201108:01T15:17:20,123"
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
        #    endTime = date_to_mil(datetime.datetime.now() - datetime.timedelta(days=7))

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

        r = self.request(query_url, params)

        class LogQuery(JsonGetter):
            """Class to handle LogQuery Report instance.

            Attribute:
                json: JSON response.
            """

            def __init__(self, resp):
                """Inits class with JSON response.

                Args:
                    resp: JSON for log reports request
                """

                self.json = resp

            @property
            def getStartTime(self):
                return mil_to_date(self.startTime)

            @property
            def getEndTime(self):
                return mil_to_date(self.endTime)

            def __getitem__(self, index):
                """Allows for indexing of log files."""
                return self.logMessages[index]

            def __iter__(self):
                """Returns logMessages as generator."""
                for log in self.logMessages:
                    yield log

            def __len__(self):
                """Returns number of log messages returned by query."""
                return len(self.logMessages)

            def __bool__(self):
                """Returns True if log messages were returned."""
                return bool(len(self))

        return LogQuery(r)

    @passthrough
    def countErrorReports(self, machines='All'):
        """Counts the number of error reports on each machine.

        Args:
            machines: Optional machine names to count error reports on.
                Default is All.
        """

        return self.request(self._logsURL + 'countErrorReports', method=POST)

    @passthrough
    def cleanLogs(self):
        """Cleans all log reports. Proceed with caution, cannot be undone!"""
        return self.request(self._logsURL + '/clean', method=POST)
    #----------------------------------------------------------------------
    # SECURITY

    # USERS ------------------------------
    @passthrough
    def addUser(self, username, password, fullname='', description='', email=''):
        """Adds a user account to user store.

        Args:
            username: Username for new user.
            password: Password for new user.
            fullname: Optional full name of user.
            description: Optional description for user.
            email: Optional email address for user account.
        """

        return self.userStore.addUser(username, password, fullname, description, email)

    def getUsers(self, startIndex='', pageSize=1000):
        """Gets all users in user store, intended for iterating over all user
                accounts.

        Args:
            startIndex: Optional, zero-based starting index from user list.
                Default is 0.
            pageSize: Optional max number of users. Default is 1000.
        """

        return self.userStore.getUsers(startIndex, pageSize)

    def searchUsers(self, filter='', maxCount=''):
        """Searches the user store, returns UserStore object.

        Args:
            filter: Optional filter string for users (ex: "john").
                Default is ''.
            maxCount: Optional maximimum number of records to return. Default is ''.
        """

        return self.userStore.searchUsers(filter, maxCount)

    @passthrough
    def removeUser(self, username):
        """Removes a user from the user store.

        Args:
            username: Name of user to remove.
        """

        return self.userStore.removeUser(username)

    @passthrough
    def updateUser(self, username, password, fullname='', description='', email=''):
        """updates a user account in the user store

        Args:
            username: Username for new user.
            password: Password for new user.
            fullname: Optional full name of user.
            description: Optional description for user.
            email: Optional email address for user account.
        """

        return self.userStore.updateUser(username, password, fullname, description, email)

    @passthrough
    def assignRoles(self, username, roles):
        """Assigns role to user to inherit permissions of role.

        Args:
            username: Name of user.
            roles: List or comma separated list of roles.
        """

        return self.userStore.assignRoles(username, roles)

    @passthrough
    def removeRoles(self, username, rolenames):
        """Removes roles that have been previously assigned to a user account,
                only supported when role store supports reads and writes.

        Args:
            username: Name of the user.
            roles: List or comma separated list of role names.
        """

        return self.userStore.removeRoles(username, rolenames)

    @passthrough
    def getPrivilegeForUser(self, username):
        """Gets the privilege associated with a role.

        Args:
            username: Name of user.
        """

        return self.userStore.getPrivilegeForUser(username)

    # ROLES -----------------------------------------
    @passthrough
    def copyRoleStore(self, other):
        """Copies a role store into another.

        Returns:
            A list of the results.
        """

        if not isinstance(other, (self.__class__, RoleStore)):
            raise TypeError('type: {} is not supported!'.format(type(other)))
        if isinstance(other, self.__class__):
            other = other.roleStore

        # iterate through data store
        global VERBOSE
        results = []
        rs = self.roleStore
        existing = [r.get(ROLENAME) for r in rs.getRoles().get(ROLES, [])]
        for role in other.getRoles().get(ROLES, []):
            rn = role.get(ROLENAME)
            if rn not in existing:
                res = {rn: self.addRole(**role)}
                results.append(res)

                # now assign privileges
                if res.get(rn, {}).get(STATUS) == SUCCESS:
                    priv = other.getPrivilegeForRole(rn).get(PRIVILEGE)
                    if priv:
                        rs.assignPrivilege(rn, priv)

                    # now add users to role
                    users = other.getUsersWithinRole(rn).get(USERS, [])
                    if users:
                        user_res = rs.addUsersToRole(rn, users)
                        res.get(rn, {})['add_user_result'] = user_res

                if VERBOSE:
                    print(json.dumps(res))

            else:
                res = {rn: {STATUS: 'Role already exists'}}
                results.append(res)
                if VERBOSE:
                    print(json.dumps(res))


        return results

    @passthrough
    def addRole(self, rolename, description='', **kwargs):
        """Adds a role to the role store.

        Args:
            rolename: Name of role to add.
            description: Optional description for new role.
        """

        return self.roleStore.addRole(rolename, description, **kwargs)

    def getRoles(self, startIndex='', pageSize=1000):
        """This operation gives you a pageable view of roles in the role store.
                It is intended for iterating through all available role accounts.
                To search for specific role accounts instead, use the searchRoles()
                method. <- from Esri help

        Args:
            startIndex: Optional zero-based starting index from roles list.
            pageSize: Optional maximum number of roles to return.
        """

        return self.roleStore.getRoles(startIndex, pageSize)

    def searchRoles(self, filter='', maxCount=''):
        """Searches the role store.

        Args:
            filter: Optional filter string for roles (ex: "editors").
            maxCount: Optional maximimum number of roles to return.
        """

        return self.roleStore.searchRoles(filter, maxCount)

    @passthrough
    def removeRole(self, rolename):
        """Removes a role from the role store.

        Args:
            rolename: Name of role.
        """

        return self.roleStore.removeRole(rolename)

    @passthrough
    def updateRole(self, rolename, description=''):
        """Updates a role.

        Args:
            rolename: Name of the role
            description: Optional description of role.
        """

        return self.roleStore.updateRole(rolename, description)

    @passthrough
    def getRolesForUser(self, username, filter='', maxCount=10):
        """Returns the roles associated with a user.

        Args:
            username: Name of user.
            filter: Optional filter.
            maxCount: Optional maximum count of roles to return. Defaults to 10.
        """

        return self.roleStore.getRolesForUser(username, filter, maxCount)

    @passthrough
    def getUsersWithinRole(self, rolename, filter='', maxCount=100):
        """Returns all user accounts to whom this role has been assigned.

        Args:
            rolename: Name of role.
            filter: Optional filter to be applied to the resultant user set.
            maxCount: Optional maximum number of results to return.
                Defaults to 100.
        """

        return self.roleStore.getUsersWithinRole(rolename, filter, maxCount)

    @passthrough
    def addUsersToRole(self, rolename, users):
        """Assigns a role to multiple users with a single action.

        Args:
            rolename: Name of role.
            users: List of users or comma separated list.
        """

        return self.roleStore.addUsersToRole(rolename, users)

    @passthrough
    def removeUsersFromRole(self, rolename, users):
        """Removes a role assignment from multiple users.

        Args:
            rolename: Name of role.
            users: List or comma separated list of user names.
        """

        return self.roleStore.removeUsersFromRole(rolenameme, users)

    @passthrough
    def assignPrivilege(self, rolename, privilege='ACCESS'):
        """Assigns administrative acess to ArcGIS Server.

        Args:
            rolename: Name of role.
            privilege: Administrative capability to assign
                (ADMINISTER | PUBLISH | ACCESS). Defaults to 'ACCESS'.
        """

        return self.roleStore.assignPrivilege(rolename, privilege)

    @passthrough
    def getPrivilegeForRole(self, rolename):
        """Returns the privilege associated with a role.

        Args:
            rolename: Name of role.
        """

        return self.roleStore.getPrivilegeForRole(rolename)

    @passthrough
    def getRolesByPrivilege(self, privilege):
        """Returns the roles associated with a privlege.

        Args:
            privilege: Name of privilege (ADMINISTER | PUBLISH).
        """

        return self.roleStore.getRolesByPrivilege(privilege)

    # GENERAL SECURITY ------------------------------
    @passthrough
    def securityConfig(self):
        """Returns the security configuration as JSON.

        http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Security_Configuration/02r3000001t9000000/
        """

        return self.request(self._securityURL + '/config')

    @passthrough
    def updateSecurityConfig(self, securityConfig):
        """Updates the security configuration on ArcGIS Server site. Warning:
                This operation will cause the SOAP and REST service endpoints
                to be redeployed (with new configuration) on every server machine
                in the site. If the authentication tier is GIS_SERVER, then the
                ArcGIS token service is started on all server machines.

        Args:
            securityConfig: JSON object for security configuration.

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

        return self.request(query_url, params, method=POST)

    @passthrough
    def updateIdentityStore(self, userStoreConfig, roleStoreConfig):
        """Updates the location and properties for the user and role store in
                your ArcGIS Server site.

        While the GIS server does not perform authentication when the authentication
                tier selected is WEB_ADAPTOR, it requires access to the role store
                for the administrator to assign privileges to the roles. This operation
                causes the SOAP and REST service endpoints to be redeployed
                (with the new configuration) on every server machine in the site,
                and therefore this operation must be used judiciously.

        http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Update_Identity_Store/02r3000001s0000000/

        Args:
            userStoreConfig: JSON object representing user store config.
            roleStoreConfig: JSON object representing role store config.

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

        return self.request(query_url, params, method=POST)

    @passthrough
    def testIdentityStore(self, userStoreConfig, roleStoreConfig):
        """Tests the connection to the input user and role store.

        Args:
            userStoreConfig: JSON object representing user store config.
            roleStoreConfig: JSON object representing role store config.

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

        return self.request(query_url, params, method=POST)

    # TOKENS -----------------------------------------
    @passthrough
    def tokens(self):
        """Returns the token configuration with the server, can use updatetoken()
                to change the shared secret key or valid token durations.
        """

        return self.request(self._securityURL + '/tokens')

    @passthrough
    def updateTokenConfig(self, tokenManagerConfig):
        """Updates the token configuration.

        Args:
            tokenManagerConfig: JSON object for token configuration.

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

        return self.request(query_url, params, method=POST)

    # PRIMARY SITE ADMINISTRATOR ------------------------------
    @passthrough
    def disablePSA(self):
        """Disables the primary site administrator account."""
        query_url = self._securityURL + '/psa/disable'
        return self.request(query_url, method=POST)

    @passthrough
    def enablePSA(self):
        """Enables the primary site administrator account."""
        query_url = self._securityURL + '/psa/enable'
        return self.request(query_url, method=POST)

    @passthrough
    def updatePSA(self, username, password):
        """Updates the primary site administrator account.


        Args:
            username: New username for PSA (optional in REST API, required here
                for your protection).
            password: New password for PSA.
        """

        query_url = self._securityURL + '/psa/update'

        params = {'username': username,
                  'password': password}

        return self.request(query_url, params, method=POST)

    #----------------------------------------------------------------------
    # services
    def get_service_url(self, wildcard='*', asList=False):
        """Returns a service url.

        Args:
            wildcard: Optional wildcard used to grab service name.
                (ex "moun*featureserver")
            asList: Optional boolean, default is false. If true, will return a
                list of all services matching the wildcard. If false, first match
                is returned.
        """

        if not self.service_cache:
            self.list_services()
        if '*' in wildcard:
            if not '.' in wildcard:
                wildcard += '.*'
            if wildcard == '*':
                return self.service_cache[0]
            else:
                if asList:
                    return [s for s in self.service_cache if fnmatch.fnmatch(s, wildcard)]
            for s in self.service_cache:
                if fnmatch.fnmatch(s, wildcard):
                    return s
        else:
            if asList:
                return [s for s in self.service_cache if wildcard.lower() in s.lower()]
            for s in self.service_cache:
                if wildcard.lower() in s.lower():
                    return s
        print('"{0}" not found in services'.format(wildcard))
        return None

    def folder(self, folderName):
        """Administers folder. Returns Folder object.

        folderName: Name of folder to connect to.
        """

        query_url = self._servicesURL + '/{}'.format(folderName)
        return Folder(query_url)

    def service(self, service_name_or_wildcard):
        """Returns a restapi.admin.Service() object.

        Args:
            service_name_or_wildcard: Name of service or wildcard.
        """

        val_url = six.moves.urllib.parse.urlparse(service_name_or_wildcard)
        if all([val_url.scheme, val_url.netloc, val_url.path]):
            service_url = service_name_or_wildcard
        else:
            service_url = self.get_service_url(service_name_or_wildcard, False)
        if service_url:
            return Service(service_url, client=self.client)
        else:
            print('No Service found matching: "{}"'.format(service_name_or_wildcard))
            return None

    def getPermissions(self, resource):
        """Returns permissions for folder or service.

        Args:
            resource: Name of folder or folder/service.

        resource example:
            folder = 'Projects'

            service = 'Projects/HighwayReconstruction.MapServer'
        """

        query_url = self._servicesURL + '/{}/permissions'.format(resource)

        perms = self.request(query_url)['permissions']
        return [Permission(r) for r in perms]

    @passthrough
    def addPermission(self, resource, principal='', isAllowed=True, private=True):
        """Adds a permission.

        Args:
            resource: Name of folder or folder/service.
            principal: Optional name of the role whom the permission is being
                assigned.
            isAllowed: Optional boolean, tells if a resource is allowed or denied.
                Default is True.
            private: Optional boolean. Default is True. Secures service by making
                private, denies public access. Change to False to allow public
                access.

        resource example:
            folder = 'Projects'

            service = 'Projects/HighwayReconstruction.MapServer'

        Returns:
            A list of the added permissions.
        """

        add_url = self._servicesURL + '/{}/permissions/add'.format(resource)
        added_permissions = []
        if principal:
            params = {PRINCIPAL: principal, IS_ALLOWED: isAllowed}
            r = self.request(add_url, params, method=POST)
            for k,v in six.iteritems(params):
                r[k] = v
            params.append(r)

        if principal != ESRI_EVERYONE:
            params = {PRINCIPAL: ESRI_EVERYONE, IS_ALLOWED: FALSE}

            if private:
                r = self.request(add_url, params, method=POST)
            else:
                params[IS_ALLOWED] = TRUE
                r = self.request(add_url, params, method=POST)

            for k,v in six.iteritems(params):
                r[k] = v
            added_permissions.append(r)

        return added_permissions

    @passthrough
    def hasChildPermissionsConflict(self, resource, principal, permission=None):
        """Checks if service has conflicts with opposing permissions.

        Args:
            resource: Name of folder or folder/service.
            principal: Name of role for which to check for permission conflicts.
            permission: Optional JSON permission object. Defaults to None.

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
        return self.request(query_url, params)

    @passthrough
    def cleanPermissions(self, principal):
        """Cleans all permissions assigned to role (principal). Useful when a
                role has been deleted.

        Args:
            principal: Name of role to delete permisssions.
        """

        query_url = self._permissionsURL + '/clean'
        return self.request(query_url, {'principal': principal}, method=POST)

    @passthrough
    def createFolder(self, folderName, description=''):
        """Creates a new folder in the root directory.  ArcGIS server only supports
                single folder hierachy

        Args:
            folderName: Name of new folder.
            description: Optional description of folder.
        """

        query_url = self._servicesURL + '/createFolder'
        params = {'folderName': folderName, 'description': description}
        return self.request(query_url, params, method=POST)

    @passthrough
    def deleteFolder(self, folderName):
        """Deletes a folder in the root directory.

        folderName: Name of new folder.
        """

        query_url = self._servicesURL + '{}/deleteFolder'.format(folderName)
        return self.request(query_url, method=POST)

    @passthrough
    def editFolder(self, folderName, description, webEncrypted):
        """Edits a folder

        Args:
            folderName: Name of folder to edit.
            description: Folder description.
            webEncrypted: Boolean to indicate if the servies are accessible
                over SSL only.
        """

        query_url = self._servicesURL + '/{}/editFolder'.format(folderName)
        params = {'description': description, 'webEncrypted': webEncrypted}
        return self.request(query_url, params, method=POST)

    def extensions(self):
        """Returns list of custom server object extensions that are registered with the server."""
        return self.request(self._extensionsURL).get('extensions', [])

    @passthrough
    def registerExtension(self, id):
        """Registers a new server object extension. The .SOE file must first be
                uploaded to the server using the restapi.admin.Service.uploadDataItem()
                method.

        Args:
            id: itemID of the uploaded .SOE file.
        """

        query_url = self._extensionsURL + '/register'
        return self.request(query_url, {'id': id}, method=POST)

    @passthrough
    def unregisterExtension(self, extensionFileName):
        """Unregisters a server object extension.

        Args:
            extensionFileName: Name of .SOE file to unregister.
        """

        query_url = self._extensionsURL + '/unregister'
        return self.request(query_url, {'extensionFileName': extensionFileName},
            method=POST)

    @passthrough
    def updateExtension(self, id):
        """Updates extensions that have previously been registered with server.

        id: itemID of the uploaded .SOE file.
        """

        return self.request(self._extensionsURL + '/update', {'id': id}, method=POST)

    @passthrough
    def federate(self):
        """Federates ArcGIS Server with Portal for ArcGIS.  Imports services to
                make them available for portal.
        """

        return self.request(self._servicesURL + '/federate', method=POST)

    @passthrough
    def unfederate(self):
        """Unfederates ArcGIS Server from Portal for ArcGIS. Removes services
                from Portal.
        """

        return self.request(self._servicesURL + '/unfederate', method=POST)

    @passthrough
    def startServices(self, servicesAsJSON={}, folderName='', serviceName='', type=''):
        """Starts service or all services in a folder.

        Args:
            servicesAsJSON: Optional list of services as JSON (example below)

        *the following arguments are options to run on an individual folder
        (not valid args of the REST API):

            folderName: Optional name of folder to start all services. Leave
                blank to start at root.
            serviceName: Optional name of service to start. Leave blank to start
                all in folder.
            type: Optional type of service to start
                (note: choosing MapServer will also stop FeatureServer).
                valid types: MapServer|GPServer|NAServer|GeocodeServer|ImageServer


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
            folder = Folder(self._servicesURL + '/{}'.format(folderName))
            if not serviceName and not type:
                for serv in folder.services:
                    serv.pop(DESCRIPTION)
                    if serv.get(TYPE) != 'FeatureServer':
                        servicesAsJSON['services'].append(serv)
            elif serviceName and not type:
                try:
                    serv = [s for s in folder.services if s.get(NAME).lower() == serviceName.lower()][0]
                    serv.pop(DESCRIPTION)
                    servicesAsJSON.append(serv)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service named: "{}"'.format(serviceName)})
            elif type and not serviceName:
                try:
                    serv = [s for s in folder.services if s.type.lower() == type.lower()][0]
                    serv.pop(DESCRIPTION)
                    servicesAsJSON.append(serv)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service types: "{}"'.format(serviceName)})

        if not servicesAsJSON or servicesAsJSON == {'services': []}:
            return RequestError({'error': 'no services specified!'})

        params = {'services': json.dumps(servicesAsJSON) if isinstance(servicesAsJSON, dict) else servicesAsJSON}
        return self.request(query_url, params, method=POST)

    @passthrough
    def stopServices(self, servicesAsJSON={}, folderName='', serviceName='', type=''):
        """Stops service or all services in a folder.

        Args:
            servicesAsJSON: Optional list of services as JSON (example below).

        *the following arguments are options to run on an individual folder
        (not valid args of the REST API):

            folderName: Optional name of folder to start all services. Leave
                blank to start at root.
            serviceName: Optional name of service to start. Leave blank to start
                all in folder.
            type: Optional type of service to start
                (note: choosing MapServer will also stop FeatureServer).
                valid types: MapServer|GPServer|NAServer|GeocodeServer|ImageServer


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
            folder = Folder(self._servicesURL + '/{}'.format(folderName))
            if not serviceName and not type:
                for serv in folder.services:
                    serv.pop(DESCRIPTION)
                    if serv.get(TYPE) != 'FeatureServer':
                        servicesAsJSON['services'].append(serv)
            elif serviceName and not type:
                try:
                    serv = [s for s in folder.services if s.get(NAME).lower() == serviceName.lower()][0]
                    serv.pop(DESCRIPTION)
                    servicesAsJSON.append(serv)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service named: "{}"'.format(serviceName)})
            elif type and not serviceName:
                try:
                    serv = [s for s in folder.services if s.type.lower() == type.lower()][0]
                    serv.pop(DESCRIPTION)
                    servicesAsJSON.append(serv)
                except IndexError:
                    RequestError({'error': 'Folder "{}" has no service types: "{}"'.format(serviceName)})

        if not servicesAsJSON or servicesAsJSON == {'services': []}:
            return RequestError({'error': 'no services specified!'})

        params = {'services': json.dumps(servicesAsJSON) if isinstance(servicesAsJSON, dict) else servicesAsJSON}
        return self.request(query_url, params, method=POST)

    @passthrough
    def restartServices(self, servicesAsJSON={}, folderName='', serviceName='', type=''):
        """Restarts service or all services in a folder.

        Args:
            servicesAsJSON: Optional list of services as JSON (example below).

        *the following arguments are options to run on an individual folder
        (not valid args of the REST API):

            folderName: Optional name of folder to start all services. Leave
                blank to start at root.
            serviceName: Optional name of service to start. Leave blank to
                start all in folder.
            type: Optional type of service to start
                (note: choosing MapServer will also stop FeatureServer).
                valid types: MapServer|GPServer|NAServer|GeocodeServer|ImageServer


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

        self.stopServices(servicesAsJSON, folderName, serviceName, type)
        self.startServices(servicesAsJSON, folderName, serviceName, type)
        return {'status': 'success'}

    def report(self):
        """Returns a list of service report objects."""

        reps = self.request(self.url + '/report')['reports']
        return [Report(rep) for rep in reps]

    #----------------------------------------------------------------------
    # Site
    @passthrough
    def createSite(self, username, password, configStoreConnection='', directories='',
                   cluster='', logsSettings='', runAsync=True):
        """Creates a new ArcGIS Server Site.

        Args:
            username: Name of administrative account used by site
                (can be changed later).
            password: Credentials for administrative account.
            configStoreConnection: JSON object representing the connection to
                the config store.
            directories: JSON object representing a collection of server
                directories to create. By default the server directories
                will be created locally.
            cluster: JSON object for optional cluster configuration. By default
                cluster will be called. "default" with the first available port
                numbers starting at 4004.
            logsSettings: Optional log settings.
            runAsync: Optional boolean to indicate if operation needs to ran
                asynchronously. Defaults to True.

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
        return self.request(query_url, params, method=POST)

    @passthrough
    def deleteSite(self, f=JSON):
        """Deletes the site configuration and releases all server resources. Warning,
                this is an unrecoverable operation, use with caution.

        Args:
            f: Format for response (html|json). Default is JSON.
        """

        return self.request(self._adminURL + '/deleteSite', {F: f}, method=POST)

    @passthrough
    def exportSite(self, location=None, f=JSON):
        """Exports the site configuration to a location specified by user.

        Args:
            location: Optional path to a folder accessible to the server where
                the exported site configuration will be written. If a location
                is not specified, the server writes the exported site configuration
                file to directory owned by the server and returns a virtual path
                (an HTTP URL) to that location from where it can be downloaded.
            f: Optional format for response (html|json). Defaults to JSON.
        """

        url = self._serverRoot + '/exportSite'
        params = {
            LOCATION: location,
            F: f
        }
        return self.request(url, params, method=POST)

    def generate_token(self, usr, pw, expiration=60):
        """Returns a token to handle ArcGIS Server Security, this is
                different from generating a token from the admin side. Meant
                for external use.

        Args:
            user: Username credentials for ArcGIS Server.
            pw: Password credentials for ArcGIS Server.
            expiration: Optional time (in minutes) for token lifetime. Max is 100.
                Defaults to 60.
        """

        global generate_token
        return generate_token(usr, pw, expiration, client=self.client)

    def importSite(self,  location=None, f=JSON):
        """This operation imports a site configuration into the currently
                running site. Importing a site means replacing all site
                configurations. Warning, this operation is computationally
                expensive and can take a long time to complete.

        Args:
            location: A file path to an exported configuration or an ID
                referencing the stored configuration on the server.
            f: Optional format for response (html|json). Defaults to JSON.
        """

        url = self._serverRoot + '/importSite'
        params = {
            LOCATION: location,
            F: f
        }
        return self.request(url, params, method=POST)

    def joinSite(self, adminURL, username, password, f):
        """This is used to connect a server machine to an existing site. This is
                considered a "push" mechanism, in which a server machine pushes its
                configuration to the site. For the operation to be successful,
                you need to provide an account with administrative privileges to
                the site.

        Args:
            adminURL: The site URL of the currently live site. This is typically
                the Administrator Directory URL of one of the server machines
                of a site.
            username: The name of an administrative account for the site.
            password: The password of the administrative account.
            f: Optional format for response (html|json).
        """

        url = self._adminURL + '/joinSite'
        params = {
            ADMIN_URL: adminURL,
            USER_NAME: username,
            PASSWORD: password,
            F: f
        }
        return self.request(url, params, method=POST)

    def publicKey(self, f=JSON):
        """Returns the public key of the server that can be used by a client
                application (or script) to encrypt data sent to the server
                using the RSA algorithm for public-key encryption. In addition
                to encrypting the sensitive parameters, the client is also
                required to send to the server an additional flag encrypted with
                value set to true.

        Args:
            f: Format for response, if json it is wrapped in a Munch object.
                (html|json). Defaults to JSON.
        """

        url = self._adminURL + '/publicKey'
        return munch.munchify(self.request(url, {F: f}))

    def __len__(self):
        """Gets number of services."""
        if not self.service_cache:
            self.list_services()
        return len(self.service_cache)

    def __iter__(self):
        """Generator for service iteration."""
        if not self.service_cache:
            self.list_services()
        for s in self.service_cache:
            yield s

    def __getitem__(self, i):
        """Allows for service indexing."""
        if not self.service_cache:
            self.list_services()
        return self.service_cache[i]

    def __repr__(self):
        # return '<{}: {}>'.format(self.__class__.__name__, self.token.domain.split('//')[1].split(':')[0])
        return '<{}: "{}">'.format(self.__class__.__name__, parse_url(self.url).netloc.split(':')[0])


class AGOLAdminInitializer(AdminRESTEndpoint):
    """Class that handles initalizing AGOL Admin."""
    def __init__(self, url, usr='', pw='', token='', client=None):
        """Inits class with login info.

         Args:
            url: URL for server.
            usr: Username for login.
            pw: Password for login.
            token: Token for URL/login.
        """

        if '/admin/' not in url.lower():
            url = url.split('/rest/')[0] + '/rest/admin/' + url.split('/rest/')[-1]
        super(AGOLAdminInitializer, self).__init__(url, usr, pw, token, client)


class Portal(AdminRESTEndpoint):
    __servers = []

    @property
    def _servers(self):
        return self.getServers()

    def getServers(self):
        if not self.__servers:
            servers_url = get_portal_base(self.url).split('/sharing')[0] + '/portaladmin/federation/servers'
            request_method = get_request_method(servers_url, {TOKEN: self.token.token, F: JSON}, client=self.client)
            # TODO: verify True
            serversResp = request_method(servers_url, params={TOKEN: self.token.token, F: JSON}).json()
            self.__servers = [ArcServerAdmin(s.get('adminUrl') + '/admin/services', token=self.token) for s in serversResp.get('servers', [])]
        return self.__servers

    def __repr__(self):
        return '<{}:Admin: "{}">'.format(self.__class__.__name__, self.name)

    def __iter__(self):
        for server in self._servers:
            yield server

    def __len__(self):
        return len(self._servers)


class AGOLAdmin(AGOLAdminInitializer):
    """Class to handle AGOL Hosted Services Admin capabilities."""

    @property
    def portalInfo(self):
        """Gets portal info."""
        return self.token.portalInfo

    @property
    def userContentUrl(self):
        """Returns URL for user content."""
        return '{}://www.arcgis.com/sharing/rest/content/users/{}'.format(PROTOCOL, self.portalInfo.username)

    def list_services(self):
        """Returns a list of services."""
        try:
            return [s.adminServiceInfo.name for s in self.json.services]
        except AttributeError:
            return []

    def content(self):
        """Returns content from user content url."""
        return self.request(self.userContentUrl)

class AGOLFeatureService(AGOLAdminInitializer):
    """Class that handles AGOL Feature Service."""

    @staticmethod
    def clearLastEditedDate(in_json):
        """Clears the lastEditDate within json, will throw an error if updating
        a service JSON definition if this value is not an empty string/null.

        Args:
            in_json: Input JSON.

        Returns:
            The edited input JSON.
        """
        if EDITING_INFO in in_json:
            in_json[EDITING_INFO][LAST_EDIT_DATE] = ''
        return in_json

    @passthrough
    def addToDefinition(self, addToDefinition, runAsync=FALSE):
        """Adds a definition property in a feature layer.

        Args:
            addToDefinition: The service update to the layer definition property
                for a feature service layer.
            runAsync: Optional boolean to run this process asynchronously.
                Default is FALSE.
        """

        self.clearLastEditedDate(addToDefinition)
        url = '/'.join([self.url, ADD_TO_DEFINITION])

        params = {
            F: JSON,
            ADD_TO_DEFINITION: addToDefinition,
            # ASYNC: runAsync
        }

        result = self.request(url, params, method='post')
        self.refresh()
        self.reload()
        return result

    @passthrough
    def deleteFromDefinition(self, deleteFromDefinition, runAsync=FALSE):
        """Deletes a definition property in a feature layer.

        Args:
            deleteFromDefinition: The service update to the layer definition property
                for a feature service layer.
            runAsync: Optional boolean to run this process asynchronously.
                Defaults to FALSE.
        """

        self.clearLastEditedDate(deleteFromDefinition)
        url = '/'.join([self.url, DELETE_FROM_DEFINITION])
        params = {
            F: JSON,
            DELETE_FROM_DEFINITION: deleteFromDefinition,
            # ASYNC: runAsync
        }

        result = self.request(url, params, method='post')
        self.refresh()
        self.reload()
        return result

    @passthrough
    def updateDefinition(self, updateDefinition, runAsync=FALSE):
        """Updates a definition property in a feature layer.

        Args:
            updateDefinition: The service update to the layer definition property
                for a feature service layer.
            runAsync: Optional boolean to run this process asynchronously.
                Default is FALSE.
        """

        self.clearLastEditedDate(updateDefinition)
        url = '/'.join([self.url, UPDATE_DEFINITION])
        params = {
            F: JSON,
            UPDATE_DEFINITION: updateDefinition,
            # ASYNC: runAsync
        }

        result = self.request(url, params, method='post')
        self.refresh()
        self.reload()
        return result

    @passthrough
    def enableEditorTracking(self):
        capabilities = self.get(CAPABILITIES, '')
        editorInfo = self.get(EDITOR_TRACKING_INFO, {
            "enableEditorTracking": True,
            "enableOwnershipAccessControl": False,
            "allowOthersToUpdate": True,
            "allowOthersToDelete": True,
            "allowOthersToQuery": True,
            "allowAnonymousToUpdate": True,
            "allowAnonymousToDelete": True
          })
        editorInfo["enableEditorTracking"] = True

        # enable editor tracking at Feature Service level
        result = {'layers': []}
        if CHANGE_TRACKING not in capabilities:
            capabilities = ','.join([capabilities, CHANGE_TRACKING])
            result['enabled_at_feature_service'] = self.updateDefinition({CAPABILITIES: capabilities, HAS_STATIC_DATA: False, EDITOR_TRACKING_INFO: editorInfo})
        else:
            result['enabled_at_feature_service'] = {'status': 'already enabled'}

        # loop through layers and enable editor tracking
        editFields = {"editFieldsInfo":{"creationDateField":"","creatorField":"","editDateField":"","editorField":""}}
        for lyrDef in self.layers:
            url = '/'.join([self.url, str(lyrDef.id)])
            lyr = AGOLFeatureLayer(url, token=self.token)
            status = lyr.addToDefinition(editFields)
            result['layers'].append({
                'id': lyr.id,
                'name': lyr.name,
                'result': status
            })
        return munch.munchify(result)

    @passthrough
    def disableEditorTracking(self):
        """Disables editor tracking."""
        capabilities = self.get(CAPABILITIES, '').split(',')
        editorInfo = self.get(EDITOR_TRACKING_INFO, {
            "enableEditorTracking": False,
            "enableOwnershipAccessControl": False,
            "allowOthersToUpdate": True,
            "allowOthersToDelete": True,
            "allowOthersToQuery": True,
            "allowAnonymousToUpdate": True,
            "allowAnonymousToDelete": True
          })
        editorInfo["enableEditorTracking"] = False

        # enable editor tracking at Feature Service level
        result = {}
        if CHANGE_TRACKING in capabilities:
            capabilities.remove(CHANGE_TRACKING)
            capabilities = ','.join(capabilities)
            result['disabled_at_feature_service'] = self.updateDefinition({CAPABILITIES: capabilities, HAS_STATIC_DATA: self.get(HAS_STATIC_DATA), EDITOR_TRACKING_INFO: editorInfo})
        else:
            result['disabled_at_feature_service'] = {'status': 'already disabled'}

        return munch.munchify(result)

    @passthrough
    def refresh(self):
        """Refreshes server cache for this layer."""
        return self.request(self.url + '/refresh')

    def reload(self):
        """Reloads the service to catch any changes."""
        self.__init__(self.url, token=self.token, client=self.client)

    def status(self):
        """Returns the status on service (whether it is stopped or started)."""
        url = self.url + '/status'
        return self.request(url)

    def __repr__(self):
        return '<{}: "{}">'.format(self.__class__.__name__, self.url.split('/')[-2])

class AGOLFeatureLayer(AGOLFeatureService):
    """Class that handles AGOL Feature Layer."""

    def status(self):
        """Returns the status on service (whether it is stopped or started)."""
        url = self.url.split('/FeatureServer/')[0] + '/FeatureServer/status'
        return self.request(url)

    @staticmethod
    def createNewGlobalIdFieldDefinition():
        """Adds a new global id field json defition."""
        return munch.munchify({
            NAME: 'GlobalID',
            TYPE: GLOBALID,
            ALIAS: 'GlobalID',
            SQL_TYPE: SQL_TYPE_OTHER,
            NULLABLE: FALSE,
            EDITABLE: FALSE,
            DOMAIN: NULL,
            DEFAULT_VALUE: SQL_GLOBAL_ID_EXP
        })

    @staticmethod
    def createNewDateFieldDefinition(name, alias='', autoUpdate=False):
        """Creates a json definition for a new date field.

        Args:
            name: Name of new date field.
            alias: Optional field name for alias.
            autoUpdate: Optional boolean to automatically populate the field
                with the current date/time when a new record is added or updated
                (like editor tracking). The default is False.
        """

        return munch.munchify({
            NAME: name,
            TYPE: DATE_FIELD,
            ALIAS: alias or name,
            SQL_TYPE: SQL_TYPE_OTHER,
            NULLABLE: FALSE,
            EDITABLE: TRUE,
            DOMAIN: NULL,
            DEFAULT_VALUE: SQL_AUTO_DATE_EXP if autoUpdate else NULL
        })

    @staticmethod
    def createNewFieldDefinition(name, field_type=TEXT_FIELD, alias='', **kwargs):
        """Creates a json definition for a new field.

        Args:
            name: Name of new field.
            field_type: Type of field.
            alias: Optional field name for alias.
            **kwargs: Optional additional field keys to set.
        """

        fd = munch.munchify({
            NAME: name,
            TYPE: field_type,
            ALIAS: alias or name,
            # SQL_TYPE: SQL_TYPE_OTHER,
            NULLABLE: True,
            EDITABLE: True,
            # DOMAIN: NULL,
            # DEFAULT_VALUE:  NULL,
            # LENGTH: NULL,
            # VISIBLE: True
        })
        for k,v in six.iteritems(kwargs):
            if k in fd:
                fd[k] = v
        if field_type == TEXT_FIELD and fd.get(LENGTH) in (NULL, None, ''):
            fd[LENGTH] = 50 # default
        return fd

    def addField(self, name, field_type=TEXT_FIELD, alias='', **kwargs):
        """Adds a new field to layer.

        Args:
            name: Name of new field.
            field_type: Type of field.
            alias: Optional field name for alias.
            **kwargs: Optional additional field keys to set.
        """

        self.addToDefinition({FIELDS: [self.createNewFieldDefinition(name, field_type, alias or name, **kwargs)]})

    @passthrough
    def truncate(self, attachmentOnly=TRUE, runAsync=FALSE):
        """Truncates the feature layer by removing all features.

        Args:
            attachmentOnly -- Optional boolean to delete all attachments only.
                Defaults to TRUE.
            runAsync: Optional boolean to run this process asynchronously.
                Defaults to FALSE.
        """

        if not self.json.get(SUPPORTS_TRUNCATE, False):
            raise NotImplementedError('This resource does not support the Truncate method')

        url = '/'.join([self.url, TRUNCATE])
        params = {
            ATTACHMENT_ONLY: attachmentOnly,
            ASYNC: runAsync
        }

        return self.request(url, params, method=POST)

    def __repr__(self):
        return '<{}: "{}">'.format(self.__class__.__name__, self.name)

class AGOLMapService(AdminRESTEndpoint):
    # TODO
    pass

