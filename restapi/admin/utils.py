from __future__ import print_function
from .. import admin, has_arcpy, munch
from ..rest_utils import JsonGetter, NameEncoder
import os
import json

__all__  = ['ServerAdministrator']

if has_arcpy:
    import arcpy

    class AdiminstratorBase(object):

        @staticmethod
        def find_ws(path, ws_type='', return_type=False):
            """finds a valid workspace path for an arcpy.da.Editor() Session

            Required:
                path -- path to features or workspace

            Optional:
                ws_type -- option to find specific workspace type (FileSystem|LocalDatabase|RemoteDatabase)
                return_type -- option to return workspace type as well.  If this option is selected, a tuple
                    of the full workspace path and type are returned

            """
            def find_existing(path):
                if arcpy.Exists(path):
                    return path
                else:
                    if not arcpy.Exists(path):
                        return find_existing(os.path.dirname(path))

            # try original path first
            if isinstance(path, (arcpy.mapping.Layer, arcpy.mapping.TableView)):
                path = path.dataSource
            if os.sep not in str(path):
                if hasattr(path, 'dataSource'):
                    path = path.dataSource
                else:
                    path = arcpy.Describe(path).catalogPath

            path = find_existing(path)
            desc = arcpy.Describe(path)
            if hasattr(desc, 'workspaceType'):
                if ws_type == desc.workspaceType:
                    if return_type:
                        return (path, desc.workspaceType)
                    else:
                        return path
                else:
                    if return_type:
                        return (path, desc.workspaceType)
                    else:
                        return path

            # search until finding a valid workspace
            path = str(path)
            split = filter(None, str(path).split(os.sep))
            if path.startswith('\\\\'):
                split[0] = r'\\{0}'.format(split[0])

            # find valid workspace
            for i in xrange(1, len(split)):
                sub_dir = os.sep.join(split[:-i])
                desc = arcpy.Describe(sub_dir)
                if hasattr(desc, 'workspaceType'):
                    if ws_type == desc.workspaceType:
                        if return_type:
                            return (sub_dir, desc.workspaceType)
                        else:
                            return sub_dir
                    else:
                        if return_type:
                            return (sub_dir, desc.workspaceType)
                        else:
                            return sub_dir


        @staticmethod
        def form_connection_string(ws):
            """esri's describe workspace connection string does not work at 10.4, bug???"""
            desc = arcpy.Describe(ws)
            if 'SdeWorkspaceFactory' in desc.workspaceFactoryProgID:
                cp = desc.connectionProperties
                props =  ['server', 'instance', 'database', 'version', 'authentication_mode']
                db_client = cp.instance.split(':')[1]
                con_properties = cp.server
                parts = []
                for prop in props:
                    parts.append('{}={}'.format(prop.upper(), getattr(cp, prop)))
                parts.insert(2, 'DBCLIENT={}'.format(db_client))
                parts.insert(3, 'DB_CONNECTION_PROPERTIES={}'.format(cp.server))
                return ';'.join(parts)
            else:
                return 'DATABASE=' + ws

        def stopServiceAndCompressDatabase(self, sde_loc, service_url_or_name):
            """will stop a service and compress all SDE databases within the map service

            Required:
                sde_loc -- location containing .sde connections
                service_url_or_name -- full path to REST endpoint or service name
            """
            service = self.ags.service(service_url_or_name)
            workspaces = []
            manifest = service.manifest()
            if hasattr(manifest, 'databases'):

                for db in manifest.databases:
                    # read layer xmls to find all workspaces
                    dbType = db.onServerWorkspaceFactoryProgID
                    if 'SdeWorkspaceFactory' in dbType:
                        cs = db.onServerConnectionString or db.onPremiseConnectionString
                        db_name = {k:v for k, v in iter(s.split('=') for s in cs.split(';'))}['DATABASE']
                        sde = os.path.join(sde_loc, db_name + '.sde')
                        workspaces.append(sde)

            if workspaces:

                # stop service
                service.stop()
                self.__stopped_services.append(service)
                print('Stopped Service...\n')

                # compress databases
                for ws in workspaces:
                    arcpy.management.Compress(ws)

                # start service
                service.start()
                self.__started_services.append(service)
                print('\nStarted Service')

            return workspaces

else:

    class AdiminstratorBase(object):

        @staticmethod
        def find_ws(path, ws_type='', return_type=False):
            """finds a valid workspace path for an arcpy.da.Editor() Session

            Required:
                path -- path to features or workspace

            Optional:
                ws_type -- option to find specific workspace type (FileSystem|LocalDatabase|RemoteDatabase)
                return_type -- option to return workspace type as well.  If this option is selected, a tuple
                    of the full workspace path and type are returned

            """
            if os.path.splitext(path)[1] in ('.gdb', '.mdb', '.sde') and ws_type != 'FileSystem':
                if return_type:
                    return (path, 'RemoteDatabase' if os.path.splitext(path)[1] == '.sde' else 'LocalDatabase')
                return path
            elif os.path.isdir(path):
                if return_type:
                    return (path, 'FileSystem')
                return path
            elif os.path.isfile(path):
                return find_ws(os.path.dirname(path))

        @staticmethod
        def form_connection_string(ws):
            """form connection string by parsing .sde connection files"""
            if ws.endswith('.sde'):
                with open(ws, 'rb') as f:
                    data = f.read()
                datastr = data.replace('\x00','')
                server = datastr.split('SERVER\x08\x0e')[1].split('\x12')[0]
                instance = datastr.split('INSTANCE\x08*')[1].split('\x12')[0]
                dbclient = ''.join(s for s in datastr.split('DBCLIENT\x08\x14')[1].split('DB_CONNECTION')[0] if s.isalpha())
                db_connection_properties = datastr.split('DB_CONNECTION_PROPERTIES\x08\x0e')[1].split('\x12')[0]
                database = datastr.split('DATABASE\x08\x16')[1].split('\x1e')[0]
                version = datastr.split('VERSION\x08\x18')[1].split('\x1a')[0]
                authentication_mode = datastr.split('AUTHENTICATION_MODE\x08\x08')[1].split('\x10')[0]
                parts =  [server, instance, dbclient, db_connection_properties, database, version, authentication_mode]
                props =  ['SERVER', 'INSTANCE', 'DBCLIENT', 'DB_CONNECTION_PROPERTIES', 'DATABASE', 'VERSION', 'AUTHENTICATION_MODE']
                return ';'.join(map(lambda p: '{}={}'.format(*p), zip(props, parts)))
            else:
                return 'DATABASE=' + ws

        def stopServiceAndCompressDatabase(self, sde_loc, service_url_or_name):
            """stops service and compresses all associated databases"""
            raise NotImplementedError('No access to the Arcpy Module!')

class MunchEncoder(munch.Munch):

    def __repr__(self):
        return json.dumps(self, indent=2, cls=NameEncoder)

    def __str__(self):
        return self.__repr__()

class ServerResources(JsonGetter):
    def __init__(self, json):
        self.json = MunchEncoder(json)

    def __repr__(self):
        return json.dumps(self.json, indent=2, cls=NameEncoder)

    def __str__(self):
        return self.__repr__()

class ServerAdministrator(AdiminstratorBase):
    def __init__(self, server_url, usr='', pw='', token=''):
        self.ags = admin.ArcServerAdmin(server_url, usr, pw, token)
        self.__stopped_services = []
        self.__started_services = []

    @staticmethod
    def test_connection_string(string1, string2):
        """tests if a database has the same instance and name"""
        db_props1 = {k:v for k, v in iter(s.split('=') for s in string1.split(';'))}
        db_props2 = {k:v for k, v in iter(s.split('=') for s in string2.split(';'))}
        return ';'.join([db_props1.get('DATABASE'), db_props1.get('INSTANCE','NULL')]) == ';'.join([db_props2.get('DATABASE'), db_props2.get('INSTANCE','NULL')])

    def find_services_containing(self, ws, fcs=[], stop=False):
        """finds services containing an entire workspace and any specific feature classes

        Required:
            ws -- SDE workspace path
            fcs -- list of specific feature classes to search for

        Optional:
            stop -- option to stop service once item is found
        """
        ws = self.find_ws(ws)
        con_str = self.form_connection_string(ws)
        service_map = {'workspace': [], 'feature_classes': {}}
        toStop = []

        for fc in fcs:
            service_map['feature_classes'][fc.split('.')[-1]] = []

        # iterate through services and find matching workspace/layers
        for service in self.ags.iter_services():
            if hasattr(service, 'type') and service.type == 'MapServer':
                # feature servers have map servers too
                manifest = service.manifest()
                if hasattr(manifest, 'databases'):
                    for db in manifest.databases:

                        # iterate through all layers to find workspaces/fc's
                        if self.test_connection_string(con_str, db.onServerConnectionString) or self.test_connection_string(con_str, db.onPremiseConnectionString):
                            service_map['workspace'].append(MunchEncoder({
                                'name': service.serviceName,
                                'service': service
                            }))
                            if service not in toStop:
                                toStop.append(service)

                            # check for specific feature classes
                            for ds in db.datasets:
                                lyr_name = ds.onServerName
                                if lyr_name in service_map['feature_classes']:
                                    service_map['feature_classes'][lyr_name].append(MunchEncoder({
                                        'name': service.serviceName,
                                        'service': service
                                    }))
                                    if service not in toStop:
                                        toStop.append(service)

        if stop:
            for service in toStop:
                service.stop()
                print('Stopped service: "{}"'.format(service.serviceName))
                self.__stopped_services.append(service)
        return ServerResources(service_map)


    def startStoppedServices(self):
        """start all stopped services that are in this instances cache, meaning those
        that have been stopped from this instance
        """
        for s in self.__stopped_services:
            s.start()
            print('Started service: "{}"'.format(s.serviceName))
            self.__stopped_services.remove(s)
            self.__started_services.append(s)