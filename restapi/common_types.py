# look for arcpy access, otherwise use open source version
from __future__ import print_function

import sqlite3
import base64
import shutil
import contextlib
from .rest_utils import *
from .decorator import decorator
import sys
import warnings

from . import six
from .six.moves import urllib, zip_longest

def force_open_source(force=True):
    """this function can be used to explicitly use open source mode, even if arcpy is available

    Optional:
        force -- when True, this will force restapi to use open source mode.
    """
    __opensource__ = force
    if force:
        from .open_restapi import Geometry, GeometryCollection, exportReplica, project, \
        partHandler, find_ws_type, SHP_FTYPES, __opensource__, GeocodeHandler, Geocoder

        for f in ['Geometry', 'GeometryCollection', 'exportReplica', 'partHandler', 'project', 'find_ws_type', 'SHP_FTYPES', '__opensource__', 'GeocodeHandler', 'Geocoder']:
            setattr(sys.modules[PACKAGE_NAME], f, locals().get(f))
            setattr(sys.modules[__name__], f, locals().get(f))

        setattr(sys.modules[PACKAGE_NAME], 'exportFeatureSet', exportFeatureSet_os)
        setattr(sys.modules[__name__], 'exportFeatureSet', exportFeatureSet_os)

    else:
        from .arc_restapi import  Geometry, GeometryCollection,  find_ws_type, \
        __opensource__, GeocodeHandler, Geocoder

        for f in ['Geometry', 'GeometryCollection', 'find_ws_type', '__opensource__', 'GeocodeHandler', 'Geocoder']:

            setattr(sys.modules[PACKAGE_NAME], f, locals().get(f))
            setattr(sys.modules[__name__], f, locals().get(f))

        setattr(sys.modules[PACKAGE_NAME], 'exportFeatureSet', exportFeatureSet_arcpy)
        setattr(sys.modules[__name__], 'exportFeatureSet', exportFeatureSet_arcpy)

try:
    # can explicitly choose to use open source
    print('FORCE OPEN SOURCE IS: ', FORCE_OPEN_SOURCE)
    if FORCE_OPEN_SOURCE:
        print('you have chosen to explicitly use the open source version.')
        raise ImportError

    import arcpy
    from .arc_restapi import *
    has_arcpy = True

except ImportError:
    warnings.warn('No Arcpy found, some limitations in functionality may apply.')
    from .open_restapi import *
    has_arcpy = False
    class Callable(object):
        def __call__(self, *args, **kwargs):
            raise NotImplementedError('No Access to arcpy!')

        def __getattr__(self, attr):
            """recursively raise not implemented error for any calls to arcpy:

                arcpy.management.AddField(...)

            or:

                arcpy.AddField_management(...)
            """
            return Callable()

        def __repr__(self):
            return ''

    class ArcpyPlaceholder(object):
        def __getattr__(self, attr):
            return Callable()

        def __setattr__(self, attr, val):
            pass

        def __repr__(self):
            return ''

    arcpy = ArcpyPlaceholder()

USE_GEOMETRY_PASSTHROUGH = True #can be set to false to not use @geometry_passthrough

@decorator
def geometry_passthrough(func, *args, **kwargs):
    """decorator to return a single geometry if a single geometry was returned
    in a GeometryCollection(), otherwise returns the full GeometryCollection()
    """
    f = func(*args, **kwargs)
    gc = GeometryCollection(f)
    if gc.count == 1 and USE_GEOMETRY_PASSTHROUGH:
        return gc[0]
    else:
        return gc
    return f

def getFeatureExtent(in_features):
    """gets the extent for a FeatureSet() or GeometryCollection(), must be convertible
    to a GeometryCollection().  Returns an envelope json structure (extent)

    Required:
        in_features -- input features (Feature|FeatureSet|GeometryCollection|json)
    """
    if not isinstance(in_features, GeometryCollection):
        in_features = GeometryCollection(in_features)

    extents = [g.envelopeAsJSON() for g in iter(in_features)]
    full_extent = {SPATIAL_REFERENCE: extents[0].get(SPATIAL_REFERENCE)}
    for attr, op in {XMIN: min, YMIN: min, XMAX: max, YMAX: max}.iteritems():
        full_extent[attr] = op([e.get(attr) for e in extents])
    return munch.munchify(full_extent)

def unqualify_fields(fs):
    """removes fully qualified field names from a feature set

    Required:
        fs -- restapi.FeatureSet() object or JSON
    """
    if not isinstance(fs, FeatureSet):
        fs = FeatureSet(fs)

    clean_fields = {}
    for f in fs.fields:
        clean = f.name.split('.')[-1]
        clean_fields[f.name] = clean
        f.name = clean

    for i,feature in enumerate(fs.features):
        feature_copy = {}
        for f, val in six.iteritems(feature.attributes):
            feature_copy[clean_fields.get(f, f)] = val
        fs.features[i].attributes = munch.munchify(feature_copy)

def exportFeatureSet_arcpy(feature_set, out_fc, include_domains=False, qualified_fieldnames=False, **kwargs):
        """export FeatureSet (JSON result)  to shapefile or feature class

        Required:
            feature_set -- JSON response obtained from a query or FeatureSet() object
            out_fc -- output feature class or shapefile

        Optional:
            include_domains -- if True, will manually create the feature class and add domains to GDB
                if output is in a geodatabase.
            qualified_fieldnames -- default is False, in situations where there are table joins, there
                are qualified table names such as ["table1.Field_from_tab1", "table2.Field_from_tab2"].
                By setting this to false, exported fields would be: ["Field_from_tab1", "Field_from_tab2"]

        at minimum, feature set must contain these keys:
            [u'features', u'fields', u'spatialReference', u'geometryType']
        """
        if __opensource__:
            return exportFeatureSet_os(feature_set, out_fc, **kwargs)

        out_fc = validate_name(out_fc)
        # validate features input (should be list or dict, preferably list)
        if not isinstance(feature_set, FeatureSet):
            feature_set = FeatureSet(feature_set)

        if not qualified_fieldnames:
            unqualify_fields(feature_set)

        # find workspace type and path
        ws, wsType = find_ws_type(out_fc)
        isShp = wsType == 'FileSystem'
        temp = time.strftime(r'in_memory\restapi_%Y%m%d%H%M%S') #if isShp else None
        original = out_fc
        if isShp:
            out_fc = temp
        try:
            hasGeom = GEOMETRY in feature_set.features[0]
        except:
            print('could not check geometry!')
            hasGeom = False

        # try converting JSON features from arcpy, seems very fragile...
        try:
            ##tmp = feature_set.dump(tmp_json_file(), indent=None)
            ##arcpy.conversion.JSONToFeatures(tmp, out_fc) #this tool is very buggy :(
            gp = arcpy.geoprocessing._base.Geoprocessor()
            arcpy_fs = gp.fromEsriJson(feature_set.dumps(indent=None)) #arcpy.FeatureSet from JSON string
            arcpy_fs.save(out_fc)

        except:
            # manually add records with insert cursor
            print('arcpy conversion failed, manually writing features...')
            outSR = arcpy.SpatialReference(feature_set.getSR())
            path, fc_name = os.path.split(out_fc)
            g_type = G_DICT.get(feature_set.geometryType, '').upper()
            arcpy.management.CreateFeatureclass(path, fc_name, g_type,
                                            spatial_reference=outSR)

            # add all fields
            cur_fields = []
            fMap = []
            if not isShp:
                gdb_domains = arcpy.Describe(ws).domains
            for field in feature_set.fields:
                if field.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
                    if '.' in field.name:
                        if 'shape.' not in field.name.lower():
                            field_name = field.name.split('.')[-1] #for weird SDE fields with periods
                        else:
                            field_name = '_'.join([f.title() for f in field.name.split('.')]) #keep geometry calcs if shapefile
                    else:
                        field_name = field.name


                    # need to filter even more as SDE sometimes yields weird field names...sigh
                    restricted = ('fid', 'shape', 'objectid')
                    if (not any(['shape_' in field.name.lower(),
                                'shape.' in field.name.lower(),
                                '(shape)' in field.name.lower()]) \
                                or isShp) and field.name.lower() not in restricted:
                        field_length = field.length if hasattr(field, 'length') else None
                        arcpy.management.AddField(out_fc, field_name, FTYPES[field.type],
                                                    field_length=field_length,
                                                    field_alias=field.alias)
                        cur_fields.append(field_name)
                        fMap.append(field.name)

            # insert cursor to write rows manually
            with arcpy.da.InsertCursor(out_fc, cur_fields + ['SHAPE@']) as irows:
                for row in Cursor(feature_set, fMap + ['SHAPE@']).get_rows():
                    irows.insertRow(row.values)

        if not isShp and include_domains in (True, 1, TRUE):
            gdb_domains = arcpy.Describe(ws).domains
            dom_map = {}
            for field in feature_set.fields:
                if field.get(DOMAIN):
                    field_name = field.name.split('.')[-1]
                    dom_map[field_name] = field.domain[NAME]
                    if field.domain[NAME] not in gdb_domains:
                        if CODED_VALUES in field.domain:
                            dType = CODED
                        else:
                            dType = RANGE_UPPER


                        arcpy.management.CreateDomain(ws, field.domain[NAME],
                                                          field.domain[NAME],
                                                          FTYPES[field.type],
                                                          dType)

                        try:

                            if dType == CODED:
                                for cv in field.domain[CODED_VALUES]:
                                    arcpy.management.AddCodedValueToDomain(ws, field.domain[NAME], cv[CODE], cv[NAME])

                            elif dType == RANGE_UPPER:
                                _min, _max = field.domain[RANGE]
                                arcpy.management.SetValueForRangeDomain(ws, field.domain[NAME], _min, _max)

                        except Exception as e:
                            warnings.warn(e)

                        gdb_domains.append(field.domain[NAME])
                        print('Added domain "{}" to database: "{}"'.format(field.domain[NAME], ws))

            # add domains
            if not isShp and include_domains:
                field_list = [f.name.split('.')[-1] for f in arcpy.ListFields(out_fc)]
                for fld, dom_name in six.iteritems(dom_map):
                    if fld in field_list:
                        arcpy.management.AssignDomainToField(out_fc, fld, dom_name)
                        print('Assigned domain "{}" to field "{}"'.format(dom_name, fld))


        # copy in_memory fc to shapefile
        if isShp:
            arcpy.management.CopyFeatures(out_fc, original)
            if arcpy.Exists(temp):
                arcpy.management.Delete(temp)

        print('Created: "{0}"'.format(original))
        return original

def exportFeatureSet_os(feature_set, out_fc, outSR=None, **kwargs):
        """export features (JSON result) to shapefile or feature class

        Required:
            out_fc -- output feature class or shapefile
            feature_set -- JSON response (feature set) obtained from a query

        Optional:
            outSR -- optional output spatial reference.  If none set, will default
                to SR of result_query feature set.
        """
        import shp_helper
        from .shapefile import shapefile
        out_fc = validate_name(out_fc)
        # validate features input (should be list or dict, preferably list)
        if not isinstance(feature_set, FeatureSet):
            feature_set = FeatureSet(feature_set)

        # make new shapefile
        fields = feature_set.fields
        this_sr = feature_set.getSR()
        if not outSR:
            outSR = this_sr
        else:
            if this_sr:
                if outSR != this_sr:
                    # do not change without reprojecting...
                    outSR = this_sr

        g_type = getattr(feature_set, GEOMETRY_TYPE)

        # add all fields
        w = shp_helper.ShpWriter(G_DICT[g_type].upper(), out_fc)
        field_map = []
        for fld in fields:
            if fld.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
                if not any(['shape_' in fld.name.lower(),
                            'shape.' in fld.name.lower(),
                            '(shape)' in fld.name.lower(),
                            'objectid' in fld.name.lower(),
                            fld.name.lower() == 'fid']):

                    field_name = fld.name.split('.')[-1][:10]
                    field_type = SHP_FTYPES[fld.type]
                    field_length = str(fld.length) if hasattr(fld, 'length') else "50"
                    w.add_field(field_name, field_type, field_length)
                    field_map.append((fld.name, field_name))

        # search cursor to write rows
        s_fields = [fl for fl in fields if fl.name in [f[0] for f in field_map]]
        for feat in feature_set:
            row = [feat.get(field) for field in [f[0] for f in field_map]]
            w.add_row(Geometry(feat.geometry).asShape(), row)

        w.save()
        print('Created: "{0}"'.format(out_fc))

        # write projection file
        project(out_fc, outSR)
        return out_fc

if has_arcpy:
    exportFeatureSet = exportFeatureSet_arcpy

else:
    exportFeatureSet = exportFeatureSet_os

def exportGeometryCollection(gc, output, **kwargs):
    """Exports a goemetry collection to shapefile or feature class

    Required:
        gc -- GeometryCollection() object
        output -- output data set (will be geometry only)
    """
    if isinstance(gc, Geometry):
        gc = GeometryCollection(gc)
    if not isinstance(gc, GeometryCollection):
        raise ValueError('Input is not a GeometryCollection!')

    fs_dict = {}
    fs_dict[SPATIAL_REFERENCE] = {WKID: gc.spatialReference}
    fs_dict[GEOMETRY_TYPE] = gc.geometryType
    fs_dict[DISPLAY_FIELD_NAME] = ''
    fs_dict[FIELD_ALIASES] = {OBJECTID: OBJECTID}
    fs_dict[FIELDS] = [{NAME: OBJECTID,
                        TYPE: OID,
                        ALIAS: OBJECTID}]
    fs_dict[FEATURES] = [{ATTRIBUTES: {OBJECTID:i+1}, GEOMETRY:ft.json} for i,ft in enumerate(gc)]

    fs = FeatureSet(fs_dict)
    return exportFeatureSet(fs, output, **kwargs)

def featureIterator(obj):
    if hasattr(obj, 'features'):
    	for ft in iter(obj.features):
    		yield Feature(ft)

FeatureSet.__iter__ = featureIterator

class Cursor(FeatureSet):
    """Class to handle Cursor object"""
    json = {}
    fieldOrder = []
    field_names = []

    class BaseRow(object):
        """Class to handle Row object"""
        def __init__(self, feature, spatialReference):
            """Row object for Cursor
            Required:
                feature -- features JSON object
            """
            self.feature = Feature(feature) if not isinstance(feature, Feature) else feature
            self.spatialReference = spatialReference

        def get(self, field):
            """gets an attribute by field name

            Required:
                field -- name of field for which to get the value
            """
            return self.feature.attributes.get(field)

    def __init__(self, feature_set, fieldOrder=[]):
        """Cursor object for a feature set
        Required:
            feature_set -- feature set as json or restapi.FeatureSet() object
        Optional:
            fieldOrder -- order of fields for cursor row returns.  To explicitly
                specify and OBJECTID field or Shape (geometry field), you must use
                the field tokens 'OID@' and 'SHAPE@' respectively.
        """
        if isinstance(feature_set, FeatureSet):
            feature_set = feature_set.json
        super(Cursor, self).__init__(feature_set)
        self.fieldOrder = self.__validateOrderBy(fieldOrder)

        cursor = self
        class Row(cursor.BaseRow):
            """Class to handle Row object"""

            @property
            def geometry(self):
                """returns a restapi Geometry() object"""
                if GEOMETRY in self.feature.json:
                    gd = {k: v for k,v in six.iteritems(self.feature.geometry)}
                    if SPATIAL_REFERENCE not in gd:
                        gd[SPATIAL_REFERENCE] = cursor.spatialReference
                    return Geometry(gd)
                return None

            @property
            def oid(self):
                """returns the OID for row"""
                if cursor.OIDFieldName:
                    return self.get(cursor.OIDFieldName)
                return None

            @property
            def values(self):
                """returns values as tuple"""
                # fix date format in milliseconds to datetime.datetime()
                vals = []
                for field in cursor.field_names:
                    if field in cursor.date_fields and self.get(field):
                        vals.append(mil_to_date(self.get(field)))
                    elif field in cursor.long_fields and self.get(field):
                        vals.append(long(self.get(field)))
                    else:
                        if field == OID_TOKEN:
                            vals.append(self.oid)
                        elif field == SHAPE_TOKEN:
                            if self.geometry:
                                vals.append(self.geometry.asShape())
                            else:
                                vals.append(None) #null Geometry
                        else:
                            vals.append(self.get(field))

                return tuple(vals)

            def __getitem__(self, i):
                """allows for getting a field value by index"""
                return self.values[i]

        # expose Row object
        self.__Row = Row

    @property
    def date_fields(self):
        """gets the names of any date fields within feature set"""
        return [f.name for f in self.fields if f.type == DATE_FIELD]

    @property
    def long_fields(self):
        """field names of type Long Integer, need to know this for use with
        arcpy.da.InsertCursor() as the values need to be cast to long
        """
        return [f.name for f in self.fields if f.type == LONG_FIELD]

    @property
    def field_names(self):
        """gets the field names for feature set"""
        names = []
        for f in self.fieldOrder:
            if f == OID_TOKEN and self.OIDFieldName:
                names.append(self.OIDFieldName)
            elif f == SHAPE_TOKEN and self.ShapeFieldName:
                names.append(self.ShapeFieldName)
            else:
                names.append(f)
        return names

    def get_rows(self):
        """returns row objects"""
        for feature in self.features:
            yield self._createRow(feature, self.spatialReference)

    def rows(self):
        """returns Cursor.rows() as generator"""
        for feature in self.features:
            yield self._createRow(feature, self.spatialReference).values

    def getRow(self, index):
        """returns row object at index"""
        return self._createRow(self.features[index], self.spatialReference)

    def _toJson(self, row):
        """casts row to json"""
        if isinstance(row, (list, tuple)):
            ft = {ATTRIBUTES: {}}
            for i,f in enumerate(self.field_names):
                if f != self.ShapeFieldName and f.upper() != SHAPE_TOKEN:
                    val = row[i]
                    if f in self.date_fields:
                        ft[ATTRIBUTES][f] = date_to_mil(val) if isinstance(val, datetime.datetime) else val
                    elif f in self.long_fields:
                        ft[ATTRIBUTES][f] = long(val) if val is not None else val
                    else:
                        ft[ATTRIBUTES][f] = val
                else:
                    geom = row[i]
                    if isinstance(geom, Geometry):
                        ft[GEOMETRY] = {k:v for k,v in six.iteritems(geom.json) if k != SPATIAL_REFERENCE}
                    else:
                        ft[GEOMETRY] = {k:v for k,v in six.iteritems(Geometry(geom).json) if k != SPATIAL_REFERENCE}
            return Feature(ft)
        elif isinstance(row, self.BaseRow):
            return row.feature
        elif isinstance(row, Feature):
            return row
        elif isinstance(row, dict):
            return Feature(row)

    def __iter__(self):
        """returns Cursor.rows()"""
        return self.rows()

    def _createRow(self, feature, spatialReference):
        return self.__Row(feature, spatialReference)

    def __validateOrderBy(self, fields):
        """fixes "fieldOrder" input fields, accepts esri field tokens too ("SHAPE@", "OID@")
        Required:
            fields -- list or comma delimited field list
        """
        if not fields or fields == '*':
            fields = [f.name for f in self.fields]
        if isinstance(fields, six.string_types):
            fields = fields.split(',')
        for i,f in enumerate(fields):
            if '@' in f:
                fields[i] = f.upper()
            if f == self.ShapeFieldName:
                fields[i] = SHAPE_TOKEN
            if f == self.OIDFieldName:
                fields[i] = OID_TOKEN

        return fields

    def __repr__(self):
        return object.__repr__(self)

class JsonReplica(JsonGetter):
    """represents a JSON replica"""
    def __init__(self, in_json):
        self.json = munch.munchify(in_json)
        super(self.__class__, self).__init__()

class SQLiteReplica(sqlite3.Connection):
    """represents a replica stored as a SQLite database"""
    def __init__(self, path):
        """represents a replica stored as a SQLite database, this should ALWAYS
        be used with a context manager.  For example:

            with SQLiteReplica(r'C:\TEMP\replica.geodatabase') as con:
                print(con.list_tables())
                # do other stuff

        Required:
            path -- full path to .geodatabase file (SQLite database)
        """
        self.db = path
        super(SQLiteReplica, self).__init__(self.db)
        self.isClosed = False

    @contextlib.contextmanager
    def execute(self, sql):
        """Executes an SQL query.  This method must be used via a "with" statement
        to ensure the cursor connection is closed.

        Required:
            sql -- sql statement to use

        >>> with restapi.SQLiteReplica(r'C:\Temp\test.geodatabase') as db:
        >>>     # now do a cursor using with statement
        >>>     with db.execute('SELECT * FROM Some_Table') as cur:
        >>>         for row in cur.fetchall():
        >>>             print(row)
        """
        cursor = self.cursor()
        try:
            yield cursor.execute(sql)
        finally:
            cursor.close()

    def list_tables(self, filter_esri=True):
        """returns a list of tables found within sqlite table

        Optional:
            filter_esri -- filters out all the esri specific tables (GDB_*, ST_*), default is True.  If
                False, all tables will be listed.
        """
        with self.execute("select name from sqlite_master where type = 'table'") as cursor:
            tables = cursor.fetchall()
        if filter_esri:
            return [t[0] for t in tables if not any([t[0].startswith('st_'),
                                                     t[0].startswith('GDB_'),
                                                     t[0].startswith('sqlite_')])]
        else:
            return [t[0] for t in tables]

    def list_fields(self, table_name):
        """lists fields within a table, returns a list of tuples with the following attributes:

        cid         name        type        notnull     dflt_value  pk
        ----------  ----------  ----------  ----------  ----------  ----------
        0           id          integer     99                      1
        1           name                    0                       0

        Required:
            table_name -- name of table to get field list from
        """
        with self.execute('PRAGMA table_info({})'.format(table_name)) as cur:
            return cur.fetchall()

    def exportToGDB(self, out_gdb_path):
        """exports the sqlite database (.geodatabase file) to a File Geodatabase, requires access to arcpy.
        Warning:  All cursor connections must be closed before running this operation!  If there are open
        cursors, this can lock down the database.

        Required:
            out_gdb_path -- full path to new file geodatabase (ex: r"C:\Temp\replica.gdb")
        """
        if not has_arcpy:
            raise NotImplementedError('no access to arcpy!')
        if not hasattr(arcpy.conversion, COPY_RUNTIME_GDB_TO_FILE_GDB):
            raise NotImplementedError('arcpy.conversion.{} tool not available!'.format(COPY_RUNTIME_GDB_TO_FILE_GDB))
        self.cur.close()
        if os.path.isdir(out_gdb_path):
            out_gdb_path = os.path.join(out_gdb_path, self._fileName)
        if not out_gdb_path.endswith('.gdb'):
            out_gdb_path = os.path.splitext(out_gdb_path)[0] + '.gdb'
        return arcpy.conversion.CopyRuntimeGdbToFileGdb(self.db, out_gdb_path).getOutput(0)

    def __safe_cleanup(self):
        """close connection and remove temporary .geodatabase file"""
        try:
            self.close()
            self.isClosed = True
            if TEMP_DIR in self.db:
                os.remove(self.db)
                self.db = None
                print('Cleaned up temporary sqlite database')
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__safe_cleanup()

    def __del__(self):
        self.__safe_cleanup()

class ArcServer(RESTEndpoint):
    """Class to handle ArcGIS Server Connection"""
    def __init__(self, url, usr='', pw='', token='', proxy=None, referer=None):
        super(ArcServer, self).__init__(url, usr, pw, token, proxy, referer)
        self.service_cache = []

    def getService(self, name_or_wildcard):
        """method to return Service Object (MapService, FeatureService, GPService, etc).
        This method supports wildcards

        Required:
            name_or_wildcard -- service name or wildcard used to grab service name
                (ex: "moun_webmap_rest/mapserver" or "*moun*mapserver")
        """
        full_path = self.get_service_url(name_or_wildcard)
        if full_path:
            extension = full_path.split('/')[-1]
            if extension == 'MapServer':
                return MapService(full_path, token=self.token)
            elif extension == 'FeatureServer':
                 return FeatureService(full_path, token=self.token)
            elif extension == 'GPServer':
                 return GPService(full_path, token=self.token)
            elif extension == 'ImageServer':
                 return ImageService(full_path, token=self.token)
            elif extension == 'GeocodeServer':
                return Geocoder(full_path, token=self.token)
            else:
                raise NotImplementedError('restapi does not support "{}" services!')

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

    def folder(self, name):
        """returns a restapi.Folder() object

        Required:
            name -- name of folder
        """
        return Folder('/'.join([self.ur, name]), token=self.token)

    def list_services(self, filterer=True):
        """returns a list of all services"""
        return list(self.iter_services(filterer))

    def iter_services(self, token='', filterer=True):
        """returns a generator for all services

        Required:
            service -- full path to a rest services directory

        Optional:
            token -- token to handle security (only required if security is enabled)
        """
        self.service_cache = []
        for s in self.services:
            full_service_url = '/'.join([self.url, s[NAME], s[TYPE]])
            self.service_cache.append(full_service_url)
            yield full_service_url

        for s in self.folders:
            new = '/'.join([self.url, s])
            resp = self.request(new)
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

    def walk(self):
        """method to walk through ArcGIS REST Services. ArcGIS Server only supports single
        folder heiarchy, meaning that there cannot be subdirectories within folders.

        will return tuple of the root folder and services from the topdown.
        (root, services) example:

        ags = restapi.ArcServer(url, username, password)
        for root, folders, services in ags.walk():
            print(root)
            print(services)
            print('\n\n')
        """
        self.service_cache = []
        services = []
        for s in self.services:
            qualified_service = '/'.join([s[NAME], s[TYPE]])
            full_service_url = '/'.join([self.url, qualified_service])
            services.append(qualified_service)
            self.service_cache.append(full_service_url)
        yield (self.url, services)

        for f in self.folders:
            new = '/'.join([self.url, f])
            endpt = self.request(new)
            services = []
            for serv in endpt[SERVICES]:
                qualified_service = '/'.join([serv[NAME], serv[TYPE]])
                full_service_url = '/'.join([self.url, qualified_service])
                services.append(qualified_service)
                self.service_cache.append(full_service_url)
            yield (f, services)

    def __iter__(self):
        """returns an generator for services"""
        return self.list_services()

    def __len__(self):
        """returns number of services"""
        return len(self.service_cache)

    def __repr__(self):
        parsed = urllib.parse.urlparse(self.url)
        try:
            instance = parsed.path.split('/')[1]
        except IndexError:
            instance = '?'
        return '<ArcServer: "{}" ("{}")>'.format(parsed.netloc, instance)


class MapServiceLayer(RESTEndpoint, SpatialReferenceMixin, FieldsMixin):
    """Class to handle advanced layer properties"""

    def _fix_fields(self, fields):
        """fixes input fields, accepts esri field tokens too ("SHAPE@", "OID@"), internal
        method used for cursors.

        Required:
            fields -- list or comma delimited field list
        """
        field_list = []
        if fields == '*':
            return fields
        elif isinstance(fields, six.string_types):
            fields = fields.split(',')
        if isinstance(fields, list):
            all_fields = self.list_fields()
            for f in fields:
                if '@' in f:
                    if f.upper() == SHAPE_TOKEN:
                        if self.ShapeFieldName:
                            field_list.append(self.ShapeFieldName)
                    elif f.upper() == OID_TOKEN:
                        if self.OIDFieldName:
                            field_list.append(self.OIDFieldName)
                else:
                    if f in all_fields:
                        field_list.append(f)
        return ','.join(field_list)

    def iter_queries(self, where='1=1', add_params={}, max_recs=None, chunk_size=None, **kwargs):
        """generator to form where clauses to query all records.  Will iterate through "chunks"
        of OID's until all records have been returned (grouped by maxRecordCount)

        *Thanks to Wayne Whitley for the brilliant idea to use izip_longest()



        Optional:
            where -- where clause for OID selection
            max_recs -- maximum amount of records returned for all queries for OID fetch
            chunk_size -- size of chunks for each iteration of query iterator
            add_params -- dictionary with any additional params you want to add (can also use **kwargs)
            token -- token to handle security (only required if security is enabled)
        """
        if isinstance(add_params, dict):
            add_params[RETURN_IDS_ONLY] = TRUE

        # get oids
        resp = self.query(where=where, add_params=add_params)
        oids = sorted(resp.get(OBJECT_IDS, []))[:max_recs]
        oid_name = resp.get(OID_FIELD_NAME, OBJECTID)
        print('total records: {0}'.format(len(oids)))

        # set returnIdsOnly to False
        add_params[RETURN_IDS_ONLY] = FALSE

        # iterate through groups to form queries
        # overwrite max_recs here with transfer limit from service
        if chunk_size and chunk_size < self.json.get(MAX_RECORD_COUNT, 1000):
            max_recs = chunk_size
        else:
            max_recs = self.json.get(MAX_RECORD_COUNT, 1000)
        for each in zip_longest(*(iter(oids),) * max_recs):
            theRange = filter(lambda x: x != None, each) # do not want to remove OID "0"
            if theRange:
                _min, _max = min(theRange), max(theRange)
                del each
                yield '{0} >= {1} and {0} <= {2}'.format(oid_name, _min, _max)

    def query(self, where='1=1', fields='*', add_params={}, records=None, exceed_limit=False, f=JSON, kmz='', **kwargs):
        """query layer and get response as JSON

        Optional:
            fields -- fields to return. Default is "*" to return all fields
            where -- where clause
            add_params -- extra parameters to add to query string passed as dict
            records -- number of records to return.  Default is None to return all
                records within bounds of max record count unless exceed_limit is True
            exceed_limit -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records.
            f -- return format, default is JSON.  (html|json|kmz)
            kmz -- full path to output kmz file.  Only used if output format is "kmz".
            kwargs -- extra parameters to add to query string passed as key word arguments,
                will override add_params***

        # default params for all queries
        params = {'returnGeometry' : 'true', 'outFields' : fields,
                  'where': where, 'f' : 'json'}
        """
        query_url = self.url + '/query'

        # default params
        params = {RETURN_GEOMETRY : TRUE, WHERE: where, F : f}

        for k,v in six.iteritems(add_params):
            params[k] = v

        for k,v in six.iteritems(kwargs):
            params[k] = v

        if RESULT_RECORD_COUNT in params and self.compatible_with_version('10.3'):
            params[RESULT_RECORD_COUNT] = min([int(params[RESULT_RECORD_COUNT]), self.get(MAX_RECORD_COUNT)])

        # check for tokens (only shape and oid)
        fields = self._fix_fields(fields)
        params[OUT_FIELDS] = fields

        # geometry validation
        if self.type == FEATURE_LAYER and GEOMETRY in params:
            geom = Geometry(params.get(GEOMETRY))
            if SPATIAL_REL not in params:
                params[SPATIAL_REL] = ESRI_INTERSECT
            if isinstance(geom, Geometry):
                if IN_SR not in params and geom.getSR():
                    params[IN_SR] = geom.getSR()
                if getattr(geom, GEOMETRY_TYPE) and GEOMETRY_TYPE not in params:
                    params[GEOMETRY_TYPE] = getattr(geom, GEOMETRY_TYPE)

        elif self.type == TABLE:
            del params[RETURN_GEOMETRY]

        # create kmz file if requested (does not support exceed_limit parameter)
        if f == 'kmz':
            r = self.request(query_url, params)
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
            if exceed_limit:

                for i, where2 in enumerate(self.iter_queries(where, params, max_recs=records)):
                    sql = ' and '.join(filter(None, [where.replace('1=1', ''), where2])) #remove default
                    params[WHERE] = sql
                    resp = self.request(query_url, params)
                    if i < 1:
                        server_response = resp
                    else:
                        server_response[FEATURES] += resp[FEATURES]

            else:
                server_response = self.request(query_url, params)

            # set fields to full field definition of the layer
            flds = self.fieldLookup
            if FIELDS in server_response:
                for i,fld in enumerate(server_response.fields):
                    server_response.fields[i] = flds.get(fld.name)

            if self.type == FEATURE_LAYER:
                for key in (FIELDS, GEOMETRY_TYPE, SPATIAL_REFERENCE):
                    if key not in server_response:
                        if key == SPATIAL_REFERENCE:
                            server_response[key] = getattr(self, '_' + SPATIAL_REFERENCE)
                        else:
                            server_response[key] = getattr(self, key)

            elif self.type == TABLE:
                if FIELDS not in server_response:
                    server_response[FIELDS] = getattr(self, FIELDS)

            if all(map(lambda k: k in server_response, [FIELDS, FEATURES])):
                if records:
                    server_response[FEATURES] = server_response[FEATURES][:records]
                return FeatureSet(server_response)
            else:
                if records:
                    if isinstance(server_response, list):
                        return server_response[:records]
                return server_response

    def query_related_records(self, objectIds, relationshipId, outFields='*', definitionExpression=None, returnGeometry=None, outSR=None, **kwargs):
        """Queries related records

        Required:
            objectIds -- list of object ids for related records
            relationshipId -- id of relationship

        Optional:
            outFields -- output fields for related features
            definitionExpression -- def query for output related records
            returnGeometry -- option to return Geometry
            outSR -- output spatial reference
            kwargs -- optional key word args
        """
        if not self.json.get(RELATIONSHIPS):
            raise NotImplementedError('This Resource does not have any relationships!')

        if isinstance(objectIds, (list, tuple)):
            objectIds = ','.join(map(str, objectIds))

        query_url = self.url + '/queryRelatedRecords'
        params = {
            OBJECT_IDS: objectIds,
            RELATIONSHIP_ID: relationshipId,
            OUT_FIELDS: outFields,
            DEFINITION_EXPRESSION: definitionExpression,
            RETURN_GEOMETRY: returnGeometry,
            OUT_SR: outSR
        }

        for k,v in six.iteritems(kwargs):
            params[k] = v
        return RelatedRecords(self.request(query_url, params))

    def select_by_location(self, geometry, geometryType='', inSR='', spatialRel=ESRI_INTERSECT, distance=0, units=ESRI_METER, add_params={}, **kwargs):
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
        geometry = Geometry(geometry)
        if not geometryType:
            geometryType = geometry.geometryType
        if not inSR:
            inSR = geometry.getSR()

        params = {GEOMETRY: geometry,
                  GEOMETRY_TYPE: geometryType,
                  SPATIAL_REL: spatialRel,
                  IN_SR: inSR,
            }

        if int(distance):
            params[DISTANCE] = distance
            params[UNITS] = units

        # add additional params
        for k,v in six.iteritems(add_params):
            if k not in params:
                params[k] = v

        # add kwargs
        for k,v in six.iteritems(kwargs):
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
        for k,v in six.iteritems(kwargs):
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
            r = self.request(query_url)

            add_tok = ''
            if self.token:
                add_tok = '?token={}'.format(self.token.token if isinstance(self.token, Token) else self.token)

            if ATTACHMENT_INFOS in r:
                for attInfo in r[ATTACHMENT_INFOS]:
                    att_url = '{}/{}'.format(query_url, attInfo[ID])
                    attInfo[URL] = att_url
                    if self._proxy:
                        attInfo[URL_WITH_TOKEN] = '?'.join([self._proxy, att_url])
                    else:
                        attInfo[URL_WITH_TOKEN] = att_url + ('?token={}'.format(self.token) if self.token else '')

                keys = []
                if r[ATTACHMENT_INFOS]:
                    keys = r[ATTACHMENT_INFOS][0].keys()

                props = list(set(['id', 'name', 'size', 'contentType', 'url', 'urlWithToken'] + keys))

                class Attachment(namedtuple('Attachment', ' '.join(props))):
                    """class to handle Attachment object"""
                    __slots__ = ()
                    def __new__(cls,  **kwargs):
                        return super(Attachment, cls).__new__(cls, **kwargs)

                    def __repr__(self):
                        if hasattr(self, ID) and hasattr(self, NAME):
                            return '<Attachment ID: {} ({})>'.format(self.id, self.name)
                        else:
                            return '<Attachment> ?'

                    def blob(self):
                        """download the attachment to specified path

                        out_path -- output path for attachment

                        optional:
                            name -- name for output file.  If left blank, will be same as attachment.
                            verbose -- if true will print sucessful download message
                        """
                        b = ''
                        resp = requests.get(getattr(self, URL_WITH_TOKEN), stream=True, verify=False)
                        for chunk in resp.iter_content(1024 * 16):
                            b += chunk
                        return b

                    def download(self, out_path, name='', verbose=True):
                        """download the attachment to specified path

                        out_path -- output path for attachment

                        optional:
                            name -- name for output file.  If left blank, will be same as attachment.
                            verbose -- if true will print sucessful download message
                        """
                        if not name:
                            out_file = assign_unique_name(os.path.join(out_path, self.name))
                        else:
                            ext = os.path.splitext(self.name)[-1]
                            out_file = os.path.join(out_path, name.split('.')[0] + ext)

                        resp = requests.get(getattr(self, URL_WITH_TOKEN), stream=True, verify=False)
                        with open(out_file, 'wb') as f:
                            for chunk in resp.iter_content(1024 * 16):
                                f.write(chunk)

                        if verbose:
                            print('downloaded attachment "{}" to "{}"'.format(self.name, out_path))
                        return out_file

                return [Attachment(**a) for a in r[ATTACHMENT_INFOS]]

            return []

        else:
            raise NotImplementedError('Layer "{}" does not support attachments!'.format(self.name))

    def cursor(self, fields='*', where='1=1', add_params={}, records=None, exceed_limit=False):
        """Run Cursor on layer, helper method that calls Cursor Object

        Optional:
            fields -- fields to return. Default is "*" to return all fields
            where -- where clause
            add_params -- extra parameters to add to query string passed as dict
            records -- number of records to return.  Default is None to return all
                records within bounds of max record count unless exceed_limit is True
            exceed_limit -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records.
        """
        cur_fields = self._fix_fields(fields)

        fs = self.query(where, cur_fields, add_params, records, exceed_limit)
        return Cursor(fs, fields)

    def export_layer(self, out_fc, fields='*', where='1=1', records=None, params={}, exceed_limit=False, sr=None,
                     include_domains=True, include_attachments=False, qualified_fieldnames=False, **kwargs):
        """Method to export a feature class or shapefile from a service layer

        Required:
            out_fc -- full path to output feature class

        Optional:
            where -- optional where clause
            params -- dictionary of parameters for query
            fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            records -- number of records to return. Default is none, will return maxRecordCount
            exceed_limit -- option to get all records.  If true, will recursively query REST endpoint
                until all records have been gathered. Default is False.
            sr -- output spatial refrence (WKID)
            include_domains -- if True, will manually create the feature class and add domains to GDB
                if output is in a geodatabase.
            include_attachments -- if True, will export features with attachments.  This argument is ignored
                when the "out_fc" param is not a feature class, or the ObjectID field is not included in "fields"
                param or if there is no access to arcpy.
            qualified_fieldnames -- option to keep qualified field names, default is False.
        """
        if self.type in (FEATURE_LAYER, TABLE):

            # make new feature class
            if not sr:
                sr = self.getSR()
            else:
                params[OUT_SR] = sr

            # do query to get feature set
            fs = self.query(where, fields, params, records, exceed_limit, **kwargs)

            # get any domain info
            f_dict = {f.name: f for f in self.fields}
            for field in fs.fields:
                field.domain = f_dict[field.name].get(DOMAIN)

            if has_arcpy:
                out_fc = exportFeatureSet(fs, out_fc, include_domains)
                fc_ws, fc_ws_type = find_ws_type(out_fc)

                if all([include_attachments, self.hasAttachments, fs.OIDFieldName, fc_ws_type != 'FileSystem']):

                    # get attachments (OID will start at 1)
                    att_folder = os.path.join(arcpy.env.scratchFolder, '{}_Attachments'.format(os.path.basename(out_fc)))
                    if not os.path.exists(att_folder):
                        os.makedirs(att_folder)

                    att_dict, att_ids = {}, []
                    for i,row in enumerate(fs):
                        att_id = 'P-{}'.format(i + 1)
                        print('\nattId: {}, oid: {}'.format(att_id, row.get(fs.OIDFieldName)))
                        att_ids.append(att_id)
                        att_dict[att_id] = []
                        for att in self.attachments(row.get(fs.OIDFieldName)):
                            print('\tatt: ', att)
                            out_att = att.download(att_folder, verbose=False)
                            att_dict[att_id].append(os.path.join(out_att))

                    # photo field (hopefully this is a unique field name...)
                    print('att_dict is: ', att_dict)

                    PHOTO_ID = 'PHOTO_ID_X_Y_Z__'
                    arcpy.management.AddField(out_fc, PHOTO_ID, 'TEXT', field_length=255)
                    with arcpy.da.UpdateCursor(out_fc, PHOTO_ID) as rows:
                        for i,row in enumerate(rows):
                            rows.updateRow((att_ids[i],))

                    # create temp table
                    arcpy.management.EnableAttachments(out_fc)
                    tmp_tab = r'in_memory\temp_photo_points'
                    arcpy.management.CreateTable(*os.path.split(tmp_tab))
                    arcpy.management.AddField(tmp_tab, PHOTO_ID, 'TEXT')
                    arcpy.management.AddField(tmp_tab, 'PATH', 'TEXT', field_length=255)
                    arcpy.management.AddField(tmp_tab, 'PHOTO_NAME', 'TEXT', field_length=255)

                    with arcpy.da.InsertCursor(tmp_tab, [PHOTO_ID, 'PATH', 'PHOTO_NAME']) as irows:
                        for k, att_list in six.iteritems(att_dict):
                            for v in att_list:
                                irows.insertRow((k,) + os.path.split(v))

                     # add attachments
                    arcpy.management.AddAttachments(out_fc, PHOTO_ID, tmp_tab, PHOTO_ID,
                                                    'PHOTO_NAME', in_working_folder=att_folder)
                    arcpy.management.Delete(tmp_tab)
                    arcpy.management.DeleteField(out_fc, PHOTO_ID)
                    try:
                        shutil.rmtree(att_folder)
                    except:
                        pass

                    print('added attachments to: "{}"'.format(out_fc))

            else:
                exportFeatureSet(fs, out_fc, outSR=sr)

        else:
            print('Layer: "{}" is not a Feature Layer!'.format(self.name))

        return out_fc

    def clip(self, poly, output, fields='*', out_sr='', where='', envelope=False, exceed_limit=True, **kwargs):
        """Method for spatial Query, exports geometry that intersect polygon or
        envelope features.

        Required:
            poly -- polygon (or other) features used for spatial query
            output -- full path to output feature class

        Optional:
             fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            out_sr -- output spatial refrence (WKID)
            where -- optional where clause
            envelope -- if true, the polygon features bounding box will be used.  This option
                can be used if the feature has many vertices or to check against the full extent
                of the feature class
        """

        in_geom = Geometry(poly)
        sr = in_geom.getSR()
        if envelope:
            geojson = in_geom.envelopeAsJSON()
            geometryType = ESRI_ENVELOPE
        else:
            geojson = in_geom.dumps()
            geometryType = in_geom.geometryType

        if not out_sr:
            out_sr = sr

        d = {GEOMETRY_TYPE: geometryType,
             RETURN_GEOMETRY: TRUE,
             GEOMETRY: geojson,
             IN_SR : sr,
             OUT_SR: out_sr,
             SPATIAL_REL: kwargs.get(SPATIAL_REL) or ESRI_INTERSECT
        }
        return self.export_layer(output, fields, where, params=d, exceed_limit=True, sr=out_sr)

    def __repr__(self):
        """string representation with service name"""
        return '<{}: "{}" (id: {})>'.format(self.__class__.__name__, self.name, self.id)

class MapServiceTable(MapServiceLayer):
    pass

    def export_table(self, *args, **kwargs):
        """Method to export a feature class or shapefile from a service layer

        Required:
            out_fc -- full path to output feature class

        Optional:
            where -- optional where clause
            params -- dictionary of parameters for query
            fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            records -- number of records to return. Default is none, will return maxRecordCount
            exceed_limit -- option to get all records.  If true, will recursively query REST endpoint
                until all records have been gathered. Default is False.
            sr -- output spatial refrence (WKID)
            include_domains -- if True, will manually create the feature class and add domains to GDB
                if output is in a geodatabase.
            include_attachments -- if True, will export features with attachments.  This argument is ignored
                when the "out_fc" param is not a feature class, or the ObjectID field is not included in "fields"
                param or if there is no access to arcpy.
        """
        return self.export_layer(*args, **kwargs)

    def clip(self):
        raise NotImplemented('Tabular Data cannot be clipped!')

    def select_by_location(self):
        raise NotImplemented('Select By Location not supported for tabular data!')

    def layer_to_kmz(self):
        raise NotImplemented('Tabular Data cannot be converted to KMZ!')

# LEGACY SUPPORT
MapServiceLayer.layer_to_fc = MapServiceLayer.export_layer

class MapService(BaseService):

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
            if fnmatch.fnmatch(fix_encoding(layer.get(NAME)), fix_encoding(name)):
                if SUB_LAYER_IDS in layer:
                    if grp_lyr and layer[SUB_LAYER_IDS] != None:
                        return layer[ID]
                    elif not grp_lyr and not layer[SUB_LAYER_IDS]:
                        return layer[ID]
                return layer[ID]
        for tab in self.tables:
            if fnmatch.fnmatch(fix_encoding(tab.get(NAME)), fix_encoding(name)):
                return tab.get(ID)
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
        return [fix_encoding(l.name) for l in self.layers]

    def list_tables(self):
        """Method to return a list of layer names in a MapService"""
        return [t.name for t in self.tables]

    def getNameFromId(self, lyrID):
        """method to get layer name from ID

        Required:
            lyrID -- id of layer for which to get name
        """
        return [fix_encoding(l.name) for l in self.layers if l.id == lyrID][0]

    def export(self, out_image, imageSR=None, bbox=None, bboxSR=None, size=None, dpi=96, format='png', transparent=True, **kwargs):
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
                size = ','.join(map(str, [abs(int(bbox[0]) - int(bbox[2])), abs(int(bbox[1]) - int(bbox[3]))]))

        if isinstance(bbox, dict) or (isinstance(bbox, six.string_types) and bbox.startswith('{')):
            print('it is a geometry object')
            bbox = Geometry(bbox)

        if isinstance(bbox, Geometry):
            geom = bbox
            bbox = geom.envelope()
            bboxSR = geom.spatialReference
            envJson = geom.envelopeAsJSON()
            size = ','.join(map(str, [abs(envJson.get(XMAX) - envJson.get(XMIN)), abs(envJson.get(YMAX) - envJson.get(YMIN))]))
            print('set size from geometry object: {}'.format(size))

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
          SIZE: size
        }

        # add additional params from **kwargs
        for k,v in six.iteritems(kwargs):
            if k not in params:
                params[k] = v

        # do post
        r = self.request(query_url, params, ret_json=False)

        # save image
        with open(out_image, 'wb') as f:
            f.write(r.content)

        return r

    def layer(self, name_or_id, **kwargs):
        """Method to return a layer object with advanced properties by name

        Required:
            name -- layer name (supports wildcard syntax*) or id (must be of type <int>)
        """
        if isinstance(name_or_id, int):
            # reference by id directly
            return MapServiceLayer('/'.join([self.url, str(name_or_id)]), token=self.token)

        layer_path = self.get_layer_url(name_or_id, self.token, **kwargs)
        if layer_path:
            return MapServiceLayer(layer_path, token=self.token, **kwargs)
        else:
            print('Layer "{0}" not found!'.format(name_or_id))

    def table(self, name_or_id):
        """Method to return a layer object with advanced properties by name

        Required:
            name -- table name (supports wildcard syntax*) or id (must be of type <int>)
        """
        if isinstance(name_or_id, int):
            # reference by id directly
            return MapServiceTable('/'.join([self.url, str(name_or_id)]), token=self.token)

        layer_path = self.get_layer_url(name_or_id, self.token)
        if layer_path:
            return MapServiceTable(layer_path, token=self.token)
        else:
            print('Table "{0}" not found!'.format(name_or_id))

    def cursor(self, layer_name, fields='*', where='1=1', records=None, add_params={}, exceed_limit=False):
        """Cusor object to handle queries to rest endpoints

        Required:
           layer_name -- name of layer in map service

        Optional:
            fields -- option to limit fields returned.  All are returned by default
            where -- where clause for cursor
            records -- number of records to return (within bounds of max record count)
            token --
            add_params -- option to add additional search parameters
            exceed_limit -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records
        """
        lyr = self.layer(layer_name)
        return lyr.cursor(fields, where, add_params, records, exceed_limit)

    def export_layer(self, layer_name,  out_fc, fields='*', where='1=1',
                    records=None, params={}, exceed_limit=False, sr=None):
        """Method to export a feature class from a service layer

        Required:
            layer_name -- name of map service layer to export to fc
            out_fc -- full path to output feature class

        Optional:
            where -- optional where clause
            params -- dictionary of parameters for query
            fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            records -- number of records to return. Default is none, will return maxRecordCount
            exceed_limit -- option to get all records.  If true, will recursively query REST endpoint
                until all records have been gathered. Default is False.
            sr -- output spatial refrence (WKID)
        """
        lyr = self.layer(layer_name)
        lyr.layer_to_fc(out_fc, fields, where,records, params, exceed_limit, sr)

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

    def clip(self, layer_name, poly, output, fields='*', out_sr='', where='', envelope=False):
        """Method for spatial Query, exports geometry that intersect polygon or
        envelope features.

        Required:
            layer_name -- name of map service layer to export to fc
            poly -- polygon (or other) features used for spatial query
            output -- full path to output feature class

        Optional:
             fields -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            sr -- output spatial refrence (WKID)
            where -- optional where clause
            envelope -- if true, the polygon features bounding box will be used.  This option
                can be used if the feature has many vertices or to check against the full extent
                of the feature class
        """
        lyr = self.layer(layer_name)
        return lyr.clip(poly, output, fields, out_sr, where, envelope)

    def __iter__(self):
        for lyr in self.layers:
            yield lyr

# Legacy support
MapService.layer_to_fc = MapService.export_layer

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
            reps = self.request(self.url + '/replicas')
            return [namedTuple('Replica', r) for r in reps]
        else:
            return []

    def layer(self, name_or_id):
        """Method to return a layer object with advanced properties by name

        Required:
            name -- layer name (supports wildcard syntax*) or layer id (int)
        """
        if isinstance(name_or_id, int):
            # reference by id directly
            return FeatureLayer('/'.join([self.url, str(name_or_id)]), token=self.token)

        layer_path = self.get_layer_url(name_or_id)
        if layer_path:
            return FeatureLayer(layer_path, token=self.token)
        else:
            print('Layer "{0}" not found!'.format(name_or_id))

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

    def createReplica(self, layers, replicaName, geometry='', geometryType='', inSR='', replicaSR='', dataFormat='json', returnReplicaObject=True, **kwargs):
        """query attachments, returns a JSON object

        Required:
            layers -- list of layers to create replicas for (valid inputs below)
            replicaName -- name of replica

        Optional:
            geometry -- optional geometry to query features, if none supplied, will grab all features
            geometryType -- type of geometry
            inSR -- input spatial reference for geometry
            replicaSR -- output spatial reference for replica data
            dataFormat -- output format for replica (sqlite|json)
            **kwargs -- optional keyword arguments for createReplica request

        Special Optional Args:
            returnReplicaObject -- option to return replica as an object (restapi.SQLiteReplica|restapi.JsonReplica)
                based on the dataFormat of the replica.  If the data format is sqlite and this parameter
                is False, the data will need to be fetched quickly because the server will automatically clean
                out the directory. The default cleanup for a sqlite file is 10 minutes. This option is set to True
                by default.  It is recommended to set this option to True if the output dataFormat is "sqlite".

        Documentation on Server Directory Cleaning:
            http://server.arcgis.com/en/server/latest/administer/linux/about-server-directories.htm
        """
        if hasattr(self, SYNC_ENABLED) and not self.syncEnabled:
            raise NotImplementedError('FeatureService "{}" does not support Sync!'.format(self.url))

        # validate layers
        if isinstance(layers, six.string_types):
            layers = [l.strip() for l in layers.split(',')]

        elif not isinstance(layers, (list, tuple)):
            layers = [layers]

        if all(map(lambda x: isinstance(x, int), layers)):
            layers = ','.join(map(str, layers))

        elif all(map(lambda x: isinstance(x, six.string_types), layers)):
            layers = ','.join(map(str, filter(lambda x: x is not None,
                                [s.id for s in self.layers if s.name.lower()
                                 in [l.lower() for l in layers]])))

        if not geometry and not geometryType:
            ext = self.initialExtent
            inSR = self.initialExtent.spatialReference
            geometry= ','.join(map(str, [ext.xmin,ext.ymin,ext.xmax,ext.ymax]))
            geometryType = ESRI_ENVELOPE
            inSR = self.spatialReference
            useGeometry = False
        else:
            useGeometry = True
            geometry = Geometry(geometry)
            inSR = geometry.getSR()
            geometryType = geometry.geometryType


        if not replicaSR:
            replicaSR = self.spatialReference

        validated = layers.split(',')
        options = {REPLICA_NAME: replicaName,
                   LAYERS: layers,
                   LAYER_QUERIES: '',
                   GEOMETRY: geometry,
                   GEOMETRY_TYPE: geometryType,
                   IN_SR: inSR,
                   REPLICA_SR:	replicaSR,
                   TRANSPORT_TYPE: TRANSPORT_TYPE_URL,
                   RETURN_ATTACHMENTS:	TRUE,
                   RETURN_ATTACHMENTS_DATA_BY_URL: TRUE,
                   ASYNC:	FALSE,
                   F: PJSON,
                   DATA_FORMAT: dataFormat,
                   REPLICA_OPTIONS: '',
                   }

        for k,v in six.iteritems(kwargs):
            if k != SYNC_MODEL:
                if k == LAYER_QUERIES:
                    if options[k]:
                        if isinstance(options[k], six.string_types):
                            options[k] = json.loads(options[k])
                        for key in options[k].keys():
                            options[k][key][USE_GEOMETRY] = useGeometry
                            options[k] = json.dumps(options[k], ensure_ascii=False)
                else:
                    options[k] = v

        if self.syncCapabilities.supportsPerReplicaSync:
            options[SYNC_MODEL] = PER_REPLICA
        else:
            options[SYNC_MODEL] = PER_LAYER

        if options[ASYNC] in (TRUE, True) and self.syncCapabilities.supportsAsync:
            st = self.request(self.url + '/createReplica', options, )
            while STATUS_URL not in st:
                time.sleep(1)
        else:
            options[ASYNC] = 'false'
            st = self.request(self.url + '/createReplica', options)

        if returnReplicaObject:
            return self.fetchReplica(st)
        else:
            return st

    @staticmethod
    def fetchReplica(rep_url):
        """fetches a replica from a server resource.  This can be a url or a
        dictionary/JSON object with a "URL" key.  Based on the file name of the
        replica, this will return either a restapi.SQLiteReplica() or
        restapi.JsonReplica() object.  The two valid file name extensions are ".json"
        (restapi.JsonReplica) or ".geodatabase" (restapi.SQLiteReplica).

        Required:
            rep_url -- url or JSON object that contains url to replica file on server

        If the file is sqlite, it is highly recommended to use a with statement to
        work with the restapi.SQLiteReplica object so the connection is automatically
        closed and the file is cleaned from disk.  Example:

            >>> url = 'http://someserver.com/arcgis/rest/directories/TEST/SomeService_MapServer/_ags_data{B7893BA273C164D96B7BEE588627B3EBC}.geodatabase'
            >>> with FeatureService.fetchReplica(url) as replica:
            >>>     # this is a restapi.SQLiteReplica() object
            >>>     # list tables in database
            >>>     print(replica.list_tables())
            >>>     # export to file geodatabase <- requires arcpy access
            >>>     replica.exportToGDB(r'C\Temp\replica.gdb')
        """
        if isinstance(rep_url, dict):
            rep_url = st.get(URL_UPPER)

        if rep_url.endswith('.geodatabase'):
            resp = requests.get(rep_url, stream=True, verify=False)
            fileName = rep_url.split('/')[-1]
            db = os.path.join(TEMP_DIR, fileName)
            with open(db, 'wb') as f:
                for chunk in resp.iter_content(1024 * 16):
                    if chunk:
                        f.write(chunk)
            return SQLiteReplica(db)

        elif rep_url.endswith('.json'):
            return JsonReplica(requests.get(self.url, verify=False).json())

        return None


    def replicaInfo(self, replicaID):
        """get replica information

        Required:
            replicaID -- ID of replica
        """
        query_url = self.url + '/replicas/{}'.format(replicaID)
        return namedTuple('ReplicaInfo', self.request(query_url))

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
        params = {REPLICA_ID: replicaID}

        for k,v in six.iteritems(kwargs):
            params[k] = v

        return self.request(query_url, params)


    def unRegisterReplica(self, replicaID):
        """unregisters a replica on the feature service

        Required:
            replicaID -- the ID of the replica registered with the service
        """
        query_url = self.url + '/unRegisterReplica'
        params = {REPLICA_ID: replicaID}
        return self.request(query_url, params)

class FeatureLayer(MapServiceLayer):

    def __init__(self, url='', usr='', pw='', token='', proxy=None, referer=None):
        """class to handle Feature Service Layer

        Required:
            url -- feature service layer url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
            referer -- option to add Referer Header if required by proxy, this parameter
                is ignored if no proxy is specified.
        """
        super(FeatureLayer, self).__init__(url, usr, pw, token, proxy, referer)

        # store list of EditResult() objects to track changes
        self.editResults = []

    def updateCursor(self, fields='*', where='1=1', add_params={}, records=None, exceed_limit=False, auto_save=True, useGlobalIds=False, **kwargs):
        """updates features in layer using a cursor, the applyEdits() method is automatically
        called when used in a "with" statement and auto_save is True.

        Optional:
            fields -- fields to return. Default is "*" to return all fields
            where -- where clause
            add_params -- extra parameters to add to query string passed as dict
            records -- number of records to return.  Default is None to return all
                records within bounds of max record count unless exceed_limit is True
            exceed_limit -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records.
            auto_save -- automatically apply edits when using with statement,
                if True, will apply edits on the __exit__ method.
            useGlobalIds -- (added at 10.4) Optional parameter which is false by default. Requires
                the layer's supportsApplyEditsWithGlobalIds property to be true.  When set to true, the
                features and attachments in the adds, updates, deletes, and attachments parameters are
                identified by their globalIds. When true, the service adds the new features and attachments
                while preserving the globalIds submitted in the payload. If the globalId of a feature
                (or an attachment) collides with a pre-existing feature (or an attachment), that feature
                and/or attachment add fails. Other adds, updates, or deletes are attempted if rollbackOnFailure
                is false. If rollbackOnFailure is true, the whole operation fails and rolls back on any failure
                including a globalId collision.

                When useGlobalIds is true, updates and deletes are identified by each feature or attachment
                globalId rather than their objectId or attachmentId.
            kwargs -- any additional keyword arguments supported by the applyEdits method of the REST API, see
                http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Apply_Edits_Feature_Service_Layer/02r3000000r6000000/
        """
        layer = self
        class UpdateCursor(Cursor):
            def __init__(self,  feature_set, fieldOrder=[], auto_save=auto_save, useGlobalIds=useGlobalIds, **kwargs):
                super(UpdateCursor, self).__init__(feature_set, fieldOrder)
                self.useGlobalIds = useGlobalIds
                self._deletes = []
                self._updates = []
                self._attachments = {
                    ADDS: [],
                    UPDATES: [],
                    DELETES: []
                }
                self._kwargs = {}
                for k,v in six.iteritems(kwargs):
                    if k not in('feature_set', 'fieldOrder', 'auto_save'):
                        self._kwargs[k] = v
                for i, f in enumerate(self.features):
                    ft = Feature(f)
                    oid = self._get_oid(ft)
                    self.features[i] = ft

            @property
            def has_oid(self):
                try:
                    return hasattr(self, OID_FIELD_NAME) and getattr(self, OID_FIELD_NAME)
                except:
                    return False

            @property
            def has_globalid(self):
                try:
                    return hasattr(self, GLOBALID_FIELD_NAME) and getattr(self, GLOBALID_FIELD_NAME)
                except:
                    return False

            @property
            def canEditByGlobalId(self):
                return all([
                    self.useGlobalIds,
                    layer.canUseGlobalIdsForEditing,
                    self.has_globalid,
                    getattr(self, GLOBALID_FIELD_NAME) in self.field_names
                ])

            def _find_by_oid(self, oid):
                """gets a feature by its OID"""
                for ft in iter(self.features):
                    if self._get_oid(ft) == oid:
                        return ft

            def _find_index_by_oid(self, oid):
                """gets the index of a Feature by it's OID"""
                for i, ft in enumerate(self.features):
                    if self._get_oid(ft) == oid:
                        return i

            def _replace_feature_with_oid(self, oid, feature):
                """replaces a feature with OID with another Feature"""
                feature = self._toJson(feature)
                if self._get_oid(feature) != oid:
                    feature.json[ATTRIBUTES][layer.OIDFieldName] = oid
                for i, ft in enumerate(self.features):
                    if self._get_oid(ft) == oid:
                        self.features[i] = feature

            def _find_by_globalid(self, globalid):
                """gets a feature by its GlobalId"""
                for ft in iter(self.features):
                    if self._get_globalid(ft) == globalid:
                        return ft

            def _find_index_by_globalid(self, globalid):
                """gets the index of a Feature by it's GlobalId"""
                for i, ft in enumerate(self.features):
                    if self._get_globalid(ft) == globalid:
                        return i

            def _replace_feature_with_globalid(self, globalid, feature):
                """replaces a feature with GlobalId with another Feature"""
                feature = self._toJson(feature)
                if self._get_globalid(feature) != globalid:
                    feature.json[ATTRIBUTES][layer.OIDFieldName] = globalid
                for i, ft in enumerate(self.features):
                    if self._get_globalid(ft) == globalid:
                        self.features[i] = feature

            def __enter__(self):
                return self

            def __exit__(self, type, value, traceback):
                if isinstance(type, Exception):
                    raise type(value)
                elif type is None and bool(auto_save):
                    self.applyEdits()

            def rows(self):
                """returns Cursor.rows() as generator"""
                for feature in self.features:
                    yield list(self._createRow(feature, self.spatialReference).values)

            def _get_oid(self, row):
                if isinstance(row, six.integer_types):
                    return row
                try:
                    return self._toJson(row).get(layer.OIDFieldName)
                except:
                    return None

            def _get_globalid(self, row):
                if isinstance(row, six.integer_types):
                    return row
                try:
                    return self._toJson(row).get(layer.GlobalIdFieldName or getattr(self, GLOBALID_FIELD_NAME))
                except:
                    return None

            def _get_row_identifier(self, row):
                """gets the appropriate row identifier (OBJECTID or GlobalID)"""
                if self.canEditByGlobalId:
                    return self._get_globalid(row)
                return self._get_oid(row)

            def addAttachment(self, row_or_oid, attachment, **kwargs):
                """adds an attachment

                Required:
                    row_or_oid -- row returned from cursor or an OID/GlobalId
                    attachment -- full path to attachment
                """
                if not hasattr(layer, HAS_ATTACHMENTS) or not getattr(layer, HAS_ATTACHMENTS):
                    raise NotImplemented('{} does not support attachments!'.format(layer))
                if not self.has_oid:
                    raise ValueError('No OID field found! In order to add attachments, make sure the OID field is returned in the query.')
##                # cannot get this to work :(
##                if layer.canApplyEditsWithAttachments:
##                    att_key = DATA
##                    if isinstance(attachment, six.string_types) and attachment.startswith('{'):
##                        # this is an upload id?
##                        att_key = UPLOAD_ID
##                    att = {
##                        PARENT_GLOBALID: self._get_globalid(row_or_oid),
##                        att_key: attachment
##                    }
##                    self._attachments[ADDS].append(att)
##                    return

                oid = self._get_oid(row_or_oid)
                if oid:
                    return layer.addAttachment(oid, attachment, **kwargs)
                raise ValueError('No valid OID or GlobalId found to add attachment!')

            def updateAttachment(self, row_or_oid, attachmentId, attachment, **kwargs):
                """adds an attachment

                Required:
                    row_or_oid -- row returned from cursor or an OID/GlobalId
                    attachment -- full path to attachment
                """
                if not hasattr(layer, HAS_ATTACHMENTS) or not getattr(layer, HAS_ATTACHMENTS):
                    raise NotImplemented('{} does not support attachments!'.format(layer))
                if not self.has_oid:
                    raise ValueError('No OID field found! In order to add attachments, make sure the OID field is returned in the query.')
##                # cannot get this to work :(
##                if layer.canApplyEditsWithAttachments:
##                    att_key = DATA
##                    if isinstance(attachment, six.string_types) and attachment.startswith('{'):
##                        # this is an upload id?
##                        att_key = UPLOAD_ID
##                    att = {
##                        PARENT_GLOBALID: self._get_globalid(row_or_oid),
##                        GLOBALID_CAMEL: attachmentId,
##                        att_key: attachment
##                    }
##                    self._attachments[UPDATES].append(att)
##                    return

                oid = self._get_oid(row_or_oid)
                if oid:
                    return layer.updateAttachment(oid, attachmentId, attachment, **kwargs)
                raise ValueError('No valid OID or GlobalId found to add attachment!')

            def deleteAttachments(self, row_or_oid, attachmentIds, **kwargs):
                """adds an attachment

                Required:
                    row_or_oid -- row returned from cursor or an OID/GlobalId
                    attachment -- full path to attachment
                """
                if not hasattr(layer, HAS_ATTACHMENTS) or not getattr(layer, HAS_ATTACHMENTS):
                    raise NotImplemented('{} does not support attachments!'.format(layer))
                if not self.has_oid:
                    raise ValueError('No OID field found! In order to add attachments, make sure the OID field is returned in the query.')

                oid = self._get_oid(row_or_oid)
                if oid:
                    return layer.deleteAttachments(oid, attachmentIds, **kwargs)
                raise ValueError('No valid OID or GlobalId found to add attachment!')

            def updateRow(self, row):
                """updates the feature with values from updated row.  If not used in context of
                a "with" statement, updates will have to be applied manually after all edits are
                made using the UpdateCursor.applyEdits() method.  When used in the context of a
                "with" statement, edits are automatically applied on __exit__.

                Required:
                    row -- list/tuple/Feature/Row that has been updated
                """
                row = self._toJson(row)
                if self.canEditByGlobalId:
                    globalid = self._get_globalid(row)
                    self._replace_feature_with_globalid(globalid, row)
                else:
                    oid = self._get_oid(row)
                    self._replace_feature_with_oid(oid, row)
                self._updates.append(row)

            def deleteRow(self, row):
                """deletes the row

                Required:
                    row -- list/tuple/Feature/Row that has been updated
                """
                oid = self._get_oid(row)
                self.features.remove(self._find_by_oid(oid))
                if oid:
                    self._deletes.append(oid)

            def applyEdits(self):
                attCount = filter(None, [len(atts) for op, atts in six.iteritems(self._attachments)])
                if (self.has_oid or (self.has_globalid and layer.canUseGlobalIdsForEditing and self.useGlobalIds)) \
                and any([self._updates, self._deletes, attCount]):
                    kwargs = {UPDATES: self._updates, DELETES: self._deletes}
                    if layer.canApplyEditsWithAttachments and self._attachments:
                        kwargs[ATTACHMENTS] = self._attachments
                    for k,v in six.iteritems(self._kwargs):
                        kwargs[k] = v
                    if layer.canUseGlobalIdsForEditing:
                        kwargs[USE_GLOBALIDS] = self.useGlobalIds
                    return layer.applyEdits(**kwargs)
                elif not (self.has_oid or not (self.has_globalid and layer.canUseGlobalIdsForEditing and self.useGlobalIds)):
                    raise RuntimeError('Missing OID or GlobalId Field in Data!')

        cur_fields = self._fix_fields(fields)
        fs = self.query(where, cur_fields, add_params, records, exceed_limit)
        return UpdateCursor(fs, fields)

    def insertCursor(self, fields=[], template_name=None, auto_save=True):
        """inserts new features into layer using a cursor, , the applyEdits() method is automatically
        called when used in a "with" statement and auto_save is True.

        Required:
            fields -- list of fields for cursor

        Optional:
            template_name -- name of template from type
            auto_save -- automatically apply edits when using with statement,
                if True, will apply edits on the __exit__ method.
        """
        layer = self
        field_names = [f.name for f in layer.fields if f.type not in (GLOBALID, OID)]
        class InsertCursor(object):
            def __init__(self, fields, template_name=None, auto_save=True):
                self._adds = []
                self.fields = fields
                self.has_geometry = getattr(layer, TYPE) == FEATURE_LAYER
                skip = (SHAPE_TOKEN, OID_TOKEN, layer.OIDFieldName)
                if template_name:
                    try:
                        self.template = self.get_template(template_name).templates[0].prototype
                    except:
                        self.template = {ATTRIBUTES: {k: NULL for k in self.fields if k not in skip}}
                else:
                    self.template = {ATTRIBUTES: {k: NULL for k in self.fields if k not in skip}}
                if self.has_geometry:
                    self.template[GEOMETRY] = NULL
                try:
                    self.geometry_index = self.fields.index(SHAPE_TOKEN)
                except ValueError:
                    try:
                        self.geometry_index = self.fields.index(layer.shapeFieldName)
                    except ValueError:
                        self.geometry_index = None

            def insertRow(self, row):
                """inserts a row into the InsertCursor._adds cache

                Required:
                    row -- list/tuple/dict/Feature/Row that has been updated
                """
                feature = {k:v for k,v in six.iteritems(self.template)}
                if isinstance(row, (list, tuple)):
                    for i, value in enumerate(row):
                        try:
                            field = self.fields[i]
                            if field == SHAPE_TOKEN:
                                feature[GEOMETRY] = Geometry(value).json
                            else:
                                if isinstance(value, datetime.datetime):
                                    value = date_to_mil(value)
                                feature[ATTRIBUTES][field] = value
                        except IndexError:
                            pass
                    self._adds.append(feature)
                    return
                elif isinstance(row, Feature):
                    row = row.json
                if isinstance(row, dict):
                    if GEOMETRY in row and self.has_geometry:
                        feature[GEOMETRY] = Geometry(row[GEOMETRY]).json
                    if ATTRIBUTES in row:
                        for f, value in six.iteritems(row[ATTRIBUTES]):
                            if f in feature[ATTRIBUTES]:
                                if isinstance(value, datetime.datetime):
                                    value = date_to_mil(value)
                                feature[ATTRIBUTES][f] = value
                    else:
                        for f, value in six.iteritems(row):
                            if f in feature[ATTRIBUTES]:
                                if isinstance(value, datetime.datetime):
                                    value = date_to_mil(value)
                                feature[ATTRIBUTES][f] = value
                            elif f == SHAPE_TOKEN and self.has_geometry:
                                feature[GEOMETRY] = Geometry(value).json
                    self._adds.append(feature)
                    return

            def applyEdits(self):
                """applies the edits to the layer"""
                return layer.applyEdits(adds=self._adds)

            def __enter__(self):
                return self

            def __exit__(self, type, value, traceback):
                if isinstance(type, Exception):
                    raise type(value)
                elif type is None and bool(auto_save):
                    self.applyEdits()

        return InsertCursor(fields, template_name, auto_save)

    @property
    def canUseGlobalIdsForEditing(self):
        """will be true if the layer supports applying edits where globalid values
        provided by the client are used. In order for supportsApplyEditsWithGlobalIds
        to be true, layers must have a globalid column and have isDataVersioned as false.
        Layers with hasAttachments as true additionally require attachments with globalids
        and attachments related to features via the features globalid.
        """
        return all([
            self.compatible_with_version(10.4),
            hasattr(self, SUPPORTS_APPLY_EDITS_WITH_GLOBALIDS),
            getattr(self, SUPPORTS_APPLY_EDITS_WITH_GLOBALIDS)
        ])


    @property
    def canApplyEditsWithAttachments(self):
        """convenience property to check if attachments can be edited in
        applyEdits() method"""
        try:
            return all([
                self.compatible_with_version(10.4),
                hasattr(self, HAS_ATTACHMENTS),
                getattr(self, HAS_ATTACHMENTS)
            ])
        except AttributeError:
            return False

    @staticmethod
    def guess_content_type(attachment, content_type=''):
        # use mimetypes to guess "content_type"
        if not content_type:
            content_type = mimetypes.guess_type(os.path.basename(attachment))[0]
            if not content_type:
                known = mimetypes.types_map
                common = mimetypes.common_types
                ext = os.path.splitext(attachment)[-1].lower()
                if ext in known:
                    content_type = known[ext]
                elif ext in common:
                    content_type = common[ext]
        return content_type

    def get_template(self, name=None):
        """returns a template by name

        Optional:
            name -- name of template
        """
        type_names = [t.get(NAME) for t in self.json.get(TYPES, [])]
        if name in type_names:
            for t in self.json.get(TYPES, []):
                if name == t.get(NAME):
                    return t
        try:
            return self.json.get(TYPES)[0]
        except IndexError:
            return {}


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
        if isinstance(features, (list, tuple)):
            features = json.dumps(features, ensure_ascii=False)
        params = {FEATURES: features,
                  GDB_VERSION: gdbVersion,
                  ROLLBACK_ON_FAILURE: rollbackOnFailure,
                  F: PJSON}

        # add features
        return self.__edit_handler(self.request(add_url, params))

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
        if isinstance(features, (list, tuple)):
            features = json.dumps(features, ensure_ascii=False)
        update_url = self.url + '/updateFeatures'
        params = {FEATURES: features,
                  GDB_VERSION: gdbVersion,
                  ROLLBACK_ON_FAILURE: rollbackOnFailure,
                  F: JSON}

        # update features
        return self.__edit_handler(self.reques(update_url, params))

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
            geometryType = ESRI_ENVELOPE
        if not spatialRel:
            spatialRel = ESRI_INTERSECT

        del_url = self.url + '/deleteFeatures'
        if isinstance(oids, (list, tuple)):
            oids = ', '.join(map(str, oids))
        params = {OBJECT_IDS: oids,
                  WHERE: where,
                  GEOMETRY: geometry,
                  GEOMETRY_TYPE: geometryType,
                  SPATIAL_REL: spatialRel,
                  GDB_VERSION: gdbVersion,
                  ROLLBACK_ON_FAILURE: rollbackOnFailure,
                  F: JSON}

        # delete features
        return self.__edit_handler(self.request(del_url, params))

    def applyEdits(self, adds=None, updates=None, deletes=None, attachments=None, gdbVersion=None, rollbackOnFailure=TRUE, useGlobalIds=False, **kwargs):
        """apply edits on a feature service layer

        Optional:
            adds -- features to add (JSON)
            updates -- features to be updated (JSON)
            deletes -- oids to be deleted (list, tuple, or comma separated string)
            attachments -- attachments to be added, updated or deleted (added at version 10.4).  Attachments
                in this instance must use global IDs and the layer's "supportsApplyEditsWithGlobalIds" must
                be true.
            gdbVersion -- geodatabase version to apply edits
            rollbackOnFailure -- specify if the edits should be applied only if all submitted edits succeed
            useGlobalIds -- (added at 10.4) Optional parameter which is false by default. Requires
                the layer's supportsApplyEditsWithGlobalIds property to be true.  When set to true, the
                features and attachments in the adds, updates, deletes, and attachments parameters are
                identified by their globalIds. When true, the service adds the new features and attachments
                while preserving the globalIds submitted in the payload. If the globalId of a feature
                (or an attachment) collides with a pre-existing feature (or an attachment), that feature
                and/or attachment add fails. Other adds, updates, or deletes are attempted if rollbackOnFailure
                is false. If rollbackOnFailure is true, the whole operation fails and rolls back on any failure
                including a globalId collision.

                When useGlobalIds is true, updates and deletes are identified by each feature or attachment
                globalId rather than their objectId or attachmentId.
            kwargs -- any additional keyword arguments supported by the applyEdits method of the REST API, see
                http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Apply_Edits_Feature_Service_Layer/02r3000000r6000000/

        attachments example (supported only in 10.4 and above):
            {
              "adds": [{
                  "globalId": "{55E85F98-FBDD-4129-9F0B-848DD40BD911}",
                  "parentGlobalId": "{02041AEF-4174-4d81-8A98-D7AC5B9F4C2F}",
                  "contentType": "image/pjpeg",
                  "name": "Pothole.jpg",
                  "uploadId": "{DD1D0A30-CD6E-4ad7-A516-C2468FD95E5E}"
                },
                {
                  "globalId": "{3373EE9A-4619-41B7-918B-DB54575465BB}",
                  "parentGlobalId": "{6FA4AA68-76D8-4856-971D-B91468FCF7B7}",
                  "contentType": "image/pjpeg",
                  "name": "Debree.jpg",
                  "data": "<base 64 encoded data>"
                }
              ],
              "updates": [{
                "globalId": "{8FDD9AEF-E05E-440A-9426-1D7F301E1EBA}",
                "contentType": "image/pjpeg",
                "name": "IllegalParking.jpg",
                "uploadId": "{57860BE4-3B85-44DD-A0E7-BE252AC79061}"
              }],
              "deletes": [
                "{95059311-741C-4596-88EF-C437C50F7C00}",
                " {18F43B1C-2754-4D05-BCB0-C4643C331C29}"
              ]
            }
        """
        edits_url = self.url + '/applyEdits'
        if isinstance(adds, FeatureSet):
            adds = json.dumps(adds.features, ensure_ascii=False, cls=RestapiEncoder)
        elif isinstance(adds, (list, tuple)):
            adds = json.dumps(adds, ensure_ascii=False, cls=RestapiEncoder)
        if isinstance(updates, FeatureSet):
            updates = json.dumps(updates.features, ensure_ascii=False, cls=RestapiEncoder)
        elif isinstance(updates, (list, tuple)):
            updates = json.dumps(updates, ensure_ascii=False, cls=RestapiEncoder)
        if isinstance(deletes, (list, tuple)):
            deletes = ', '.join(map(str, deletes))

        params = {ADDS: adds,
                  UPDATES: updates,
                  DELETES: deletes,
                  GDB_VERSION: gdbVersion,
                  ROLLBACK_ON_FAILURE: rollbackOnFailure,
                  USE_GLOBALIDS: useGlobalIds
        }

        # handle attachment edits (added at version 10.4) cannot get this to work :(
##        if self.canApplyEditsWithAttachments and isinstance(attachments, dict):
##            for edit_type in (ADDS, UPDATES):
##                if edit_type in attachments:
##                    for att in attachments[edit_type]:
##                        if att.get(DATA) and os.path.isfile(att.get(DATA)):
##                            # multipart encoded files
##                            ct = self.guess_content_type(att.get(DATA))
##                            if CONTENT_TYPE not in att:
##                                att[CONTENT_TYPE] = ct
##                            if NAME not in att:
##                                att[NAME] = os.path.basename(att.get(DATA))
##                            with open(att.get(DATA), 'rb') as f:
##                                att[DATA] = 'data:{};base64,'.format(ct) + base64.b64encode(f.read())
##                                print(att[DATA][:50])
##                            if GLOBALID_CAMEL not in att:
##                                att[GLOBALID_CAMEL] = 'f5e0f368-17a1-4062-b848-48eee2dee1d5'
##                        temp = {k:v for k,v in six.iteritems(att) if k != 'data'}
##                        temp[DATA] = att['data'][:50]
##                        print(json.dumps(temp, indent=2))
##            params[ATTACHMENTS] = attachments
##            if any([params[ATTACHMENTS].get(k) for k in (ADDS, UPDATES, DELETES)]):
##                params[USE_GLOBALIDS] = TRUE
        # add other keyword arguments
        for k,v in six.iteritems(kwargs):
            kwargs[k] = v
        return self.__edit_handler(self.request(edits_url, params))

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

            content_type = self.guess_content_type(attachment, content_type)

            # make post request
            att_url = '{}/{}/addAttachment'.format(self.url, oid)
            files = {ATTACHMENT: (os.path.basename(attachment), open(attachment, 'rb'), content_type)}
            params = {F: JSON}
            if isinstance(self.token, Token) and self.token.isAGOL:
                params[TOKEN] = str(self.token)
            if gdbVersion:
                params[GDB_VERSION] = gdbVersion
            return self.__edit_handler(requests.post(att_url, params, files=files, cookies=self._cookie, verify=False).json(), oid)

        else:
            raise NotImplementedError('FeatureLayer "{}" does not support attachments!'.format(self.name))

    def deleteAttachments(self, oid, attachmentIds, gdbVersion='', **kwargs):
        """deletes attachments in a feature layer

        Required:
            oid -- OBJECT ID of feature in which to add attachment
            attachmentIds -- IDs of attachments to be deleted.  If attachmentIds param is set to "All", all
                attachments for this feature will be deleted.

        Optional:
            kwargs -- additional keyword arguments supported by deleteAttachments method
        """
        if self.hasAttachments:
            att_url = '{}/{}/deleteAttachments'.format(self.url, oid)
            if isinstance(attachmentIds, (list, tuple)):
                attachmentIds = ','.join(map(str, attachmentIds))
            elif isinstance(attachmentIds, six.string_types) and attachmentIds.title() == 'All':
                attachmentIds = ','.join(map(str, [getattr(att, ID) for att in self.attachments(oid)]))
            if not attachmentIds:
                return
            params = {F: JSON, ATTACHMENT_IDS: attachmentIds}
            if isinstance(self.token, Token) and self.token.isAGOL:
                params[TOKEN] = str(self.token)
            if gdbVersion:
                params[GDB_VERSION] = gdbVersion
            for k,v in six.iteritems(kwargs):
                params[k] = v
            return self.__edit_handler(requests.post(att_url, params, cookies=self._cookie, verify=False).json(), oid)
        else:
            raise NotImplementedError('FeatureLayer "{}" does not support attachments!'.format(self.name))

    def updateAttachment(self, oid, attachmentId, attachment, content_type='', gdbVersion='', validate=False):
        """add an attachment to a feature service layer

        Required:
            oid -- OBJECT ID of feature in which to add attachment
            attachmentId -- ID of feature attachment
            attachment -- path to attachment

        Optional:
            content_type -- html media type for "content_type" header.  If nothing provided,
                will use a best guess based on file extension (using mimetypes)
            gdbVersion -- geodatabase version for attachment
            validate -- option to check if attachment ID exists within feature first before
                attempting an update, this adds a small amount of overhead to method because
                a request to fetch attachments is made prior to updating. Default is False.

            valid content types can be found here @:
                http://en.wikipedia.org/wiki/Internet_media_type
        """
        if self.hasAttachments:
            content_type = self.guess_content_type(attachment, content_type)

            # make post request
            att_url = '{}/{}/updateAttachment'.format(self.url, oid)
            if validate:
                if attachmentId not in [getattr(att, ID) for att in self.attachments(oid)]:
                    raise ValueError('Attachment with ID "{}" not found in Feature with OID "{}"'.format(oid, attachmentId))
            files = {ATTACHMENT: (os.path.basename(attachment), open(attachment, 'rb'), content_type)}
            params = {F: JSON, ATTACHMENT_ID: attachmentId}
            if isinstance(self.token, Token) and self.token.isAGOL:
                params[TOKEN] = str(self.token)
            if gdbVersion:
                params[GDB_VERSION] = gdbVersion
            return self.__edit_handler(requests.post(att_url, params, files=files, cookies=self._cookie, verify=False).json(), oid)

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
        if self.json.get(SUPPORTS_CALCULATE, False):
            calc_url = self.url + '/calculate'
            p = {WHERE: where,
                CALC_EXPRESSION: json.dumps(exp, ensure_ascii=False),
                SQL_FORMAT: sqlFormat}

            return self.request(calc_url, params=p)

        else:
            raise NotImplementedError('FeatureLayer "{}" does not support field calculations!'.format(self.name))

    def __edit_handler(self, response, feature_id=None):
        """hanlder for edit results

        response -- response from edit operation
        """
        e = EditResult(response, feature_id)
        self.editResults.append(e)
        e.summary()
        return e

class FeatureTable(FeatureLayer, MapServiceTable):
    pass

class GeometryService(RESTEndpoint):
    linear_units = sorted(LINEAR_UNITS.keys())
    _default_url = 'https://utility.arcgisonline.com/ArcGIS/rest/services/Geometry/GeometryServer'

    def __init__(self, url=None, usr=None, pw=None, token=None, proxy=None, referer=None):
        if not url:
            # use default arcgis online Geometry Service
            url = self._default_url
        super(GeometryService, self).__init__(url, usr, pw, token, proxy, referer)

    @staticmethod
    def getLinearUnits():
        """returns a Munch() dictionary of linear units"""
        return munch.munchify(LINEAR_UNITS)

    @staticmethod
    def getLinearUnitWKID(unit_name):
        """gets a well known ID from a unit name

        Required:
            unit_name -- name of unit to fetch WKID for.  It is safe to use this as
                a filter to ensure a valid WKID is extracted.  if a WKID is passed in,
                that same value is returned.  This argument is expecting a string from
                linear_units list.  Valid options can be viewed with GeometryService.linear_units
        """
        if isinstance(unit_name, int) or six.text_type(unit_name).isdigit():
            return int(unit_name)

        for k,v in six.iteritems(LINEAR_UNITS):
            if k.lower() == unit_name.lower():
                return int(v[WKID])

    @staticmethod
    def validateGeometries(geometries, use_envelopes=False):
        """validates geometries to be passed into operations that use an
        array of geometries.

        Required:
            geometries -- list of geometries.  Valid inputs are GeometryCollection()'s, json,
                FeatureSet()'s, or Geometry()'s.
            use_envelopes -- option to use envelopes of all the input geometires
        """
        return GeometryCollection(geometries, use_envelopes)

    @geometry_passthrough
    def buffer(self, geometries, distances, unit='', inSR=None, outSR='', use_envelopes=False, **kwargs):
        """buffer a single geoemetry or multiple

        Required:
            geometries -- array of geometries to be buffered. The spatial reference of the geometries
                is specified by inSR. The structure of each geometry in the array is the same as the
                structure of the JSON geometry objects returned by the ArcGIS REST API.  This should be
                a restapi.GeometryCollection().

            distances -- the distances that each of the input geometries is buffered. The distance units
                are specified by unit.

        Optional:
            units -- input units (esriSRUnit_Meter|esriSRUnit_StatuteMile|esriSRUnit_Foot|esriSRUnit_Kilometer|
                esriSRUnit_NauticalMile|esriSRUnit_USNauticalMile)
            inSR -- wkid for input geometry
            outSR -- wkid for output geometry
            use_envelopes -- not a valid option in ArcGIS REST API, this is an extra argument that will
                convert the geometries to bounding box envelopes ONLY IF they are restapi.Geometry objects,
                otherwise this parameter is ignored.

        restapi constants for units:
            restapi.ESRI_METER
            restapi.ESRI_MILE
            restapi.ESRI_FOOT
            restapi.ESRI_KILOMETER
            restapi.ESRI_NAUTICAL_MILE
            restapi.ESRI_US_NAUTICAL_MILE
        """
        buff_url = self.url + '/buffer'
        geometries = self.validateGeometries(geometries)
        params = {F: PJSON,
                  GEOMETRIES: geometries,
                  IN_SR: inSR or geometries.getSR(),
                  DISTANCES: distances,
                  UNIT: self.getLinearUnitWKID(unit) or unit,
                  OUT_SR: outSR,
                  UNION_RESULTS: FALSE,
                  GEODESIC: TRUE,
                  OUT_SR: None,
                  BUFFER_SR: None
        }

        # add kwargs
        for k,v in six.iteritems(kwargs):
            if k not in (GEOMETRIES, DISTANCES, UNIT):
                params[k] = v

        # perform operation
        print('params: {}'.format({k:v for k,v in six.iteritems(params) if k != GEOMETRIES}))
        return GeometryCollection(self.request(buff_url, params),
                                  spatialReference=outSR if outSR else inSR)

    @geometry_passthrough
    def intersect(self, geometries, geometry, sr):
        """performs intersection of input geometries and other geometry

        Required:
            geometries -- input geometries (GeometryCollection|FeatureSet|json|arcpy.mapping.Layer|FeatureClass|Shapefile)

        Optional:
            sr -- spatial reference for input geometries, if not specified will be derived from input geometries
        """
        query_url = self.url + '/intersect'
        geometries = self.validateGeometries(geometries)
        sr = sr or geometries.getWKID() or NULL
        params = {
            GEOMETRY: geometry,
            GEOMETRIES: geometries,
            SR: sr
        }
        return GeometryCollection(self.request(query_url, params), spatialReference=sr)

    def union(self, geometries, sr=None):
        """performs union on input geometries

        Required:
            geometries -- input geometries (GeometryCollection|FeatureSet|json|arcpy.mapping.Layer|FeatureClass|Shapefile)

        Optional:
            sr -- spatial reference for input geometries, if not specified will be derived from input geometries
        """
        url = self.url + '/union'
        geometries = self.validateGeometries(geometries)
        sr = sr or geometries.getWKID() or NULL
        params = {
            GEOMETRY: geometries,
            GEOMETRIES: geometries,
            SR: sr
        }
        return Geometry(self.request(url, params), spatialReference=sr)

    def findTransformations(self, inSR, outSR, extentOfInterest='', numOfResults=1):
        """finds the most applicable transformation based on inSR and outSR

        Required:
            inSR -- input Spatial Reference (wkid)
            outSR -- output Spatial Reference (wkid)

        Optional:
            extentOfInterest --e bounding box of the area of interest specified as a
                JSON envelope. If provided, the extent of interest is used to return
                the most applicable geographic transformations for the area. If a spatial
                reference is not included in the JSON envelope, the inSR is used for the
                envelope.

            numOfResults -- The number of geographic transformations to return. The
                default value is 1. If numOfResults has a value of -1, all applicable
                transformations are returned.

        return looks like this:
            [
              {
                "wkid": 15851,
                "latestWkid": 15851,
                "name": "NAD_1927_To_WGS_1984_79_CONUS"
              },
              {
                "wkid": 8072,
                "latestWkid": 1172,
                "name": "NAD_1927_To_WGS_1984_3"
              },
              {
                "geoTransforms": [
                  {
                    "wkid": 108001,
                    "latestWkid": 1241,
                    "transformForward": true,
                    "name": "NAD_1927_To_NAD_1983_NADCON"
                  },
                  {
                    "wkid": 108190,
                    "latestWkid": 108190,
                    "transformForward": false,
                    "name": "WGS_1984_(ITRF00)_To_NAD_1983"
                  }
                ]
              }
            ]
        """
        params = {IN_SR: inSR,
                  OUT_SR: outSR,
                  EXTENT_OF_INTEREST: extentOfInterest,
                  NUM_OF_RESULTS: numOfResults
                }

        res = self.request(self.url + '/findTransformations', params)
        if int(numOfResults) == 1:
            return res[0]
        else:
            return res

    @geometry_passthrough
    def project(self, geometries, inSR, outSR, transformation='', transformForward='false'):
        """project a single or group of geometries

        Required:
            geometries --
            inSR --
            outSR --

        Optional:
            transformation --
            trasnformForward --
        """
        params = {GEOMETRIES: self.validateGeometries(geometries),
                  IN_SR: inSR,
                  OUT_SR: outSR,
                  TRANSFORMATION: transformation,
                  TRANSFORM_FORWARD: transformForward
                }

        return GeometryCollection(self.request(self.url + '/project', params),
                                spatialReference=outSR if outSR else inSR)

    def __repr__(self):
        try:
            return "<restapi.GeometryService: '{}'>".format(self.url.split('://')[1].split('/')[0])
        except:
            return '<restapi.GeometryService>'

class ImageService(BaseService):
    geometry_service = None

    def adjustbbox(self, boundingBox):
        """method to adjust bounding box for image clipping to maintain
        cell size.

        Required:
            boundingBox -- bounding box string (comma separated)
        """
        cell_size = int(self.pixelSizeX)
        if isinstance(boundingBox, six.string_types):
            boundingBox = boundingBox.split(',')
        return ','.join(map(str, map(lambda x: Round(x, cell_size), boundingBox)))

    def pointIdentify(self, geometry=None, **kwargs):
        """method to get pixel value from x,y coordinates or JSON point object

        geometry -- input restapi.Geometry() object or point as json

        Recognized key word arguments:
            x -- x coordinate
            y -- y coordinate
            inSR -- input spatial reference.  Should be supplied if spatial
                reference is different from the Image Service's projection

        geometry example:
            geometry = {"x":3.0,"y":5.0,"spatialReference":{"wkid":102100}}
        """
        IDurl = self.url + '/identify'

##        if geometry is not None:
##            if not isinstance(geometry, Geometry):
##                geometry = Geometry(geometry)
##
##        elif GEOMETRY in kwargs:
##            g = Geometry(kwargs[GEOMETRY])
##
##        elif X in kwargs and Y in kwargs:
##            g = {X: kwargs[X], Y: kwargs[Y]}
##            if SR in kwargs:
##                g[SPATIAL_REFERENCE] = {WKID: kwargs[SR]}
##            elif SPATIAL_REFERENCE in kwargs:
##                g[SPATIAL_REFERENCE] = {WKID: kwargs[SPATIAL_REFERENCE]}
##            else:
##                g[SPATIAL_REFERENCE] = {WKID: self.getSR()}
##            geometry = Geometry(g)
##
##        else:
##            raise ValueError('Not a valid input geometry!')

        params = {
            GEOMETRY: geometry.dumps(),
            GEOMETRY_TYPE: geometry.geometryType,
            F: JSON,
            RETURN_GEOMETRY: FALSE,
            RETURN_CATALOG_ITEMS: FALSE,
        }

        for k,v in six.iteritems(kwargs):
            if k not in params:
                params[k] = v

        j = self.request(IDurl, params)
        return j.get(VALUE)

    def exportImage(self, poly, out_raster, envelope=False, rendering_rule=None, interp=BILINEAR_INTERPOLATION, nodata=None, **kwargs):
        """method to export an AOI from an Image Service

        Required:
            poly -- polygon features
            out_raster -- output raster image

        Optional:
            envelope -- option to use envelope of polygon
            rendering_rule -- rendering rule to perform raster functions as JSON
            kwargs -- optional key word arguments for other parameters
        """
        if not out_raster.endswith('.tif'):
            out_raster = os.path.splitext(out_raster)[0] + '.tif'
        query_url = '/'.join([self.url, EXPORT_IMAGE])

        if isinstance(poly, Geometry):
            in_geom = poly
        else:
            in_geom = Geometry(poly)

        sr = in_geom.getSR()

        if envelope:
            geojson = in_geom.envelope()
            geometryType = ESRI_ENVELOPE
        else:
            geojson = in_geom.dumps()
            geometryType = in_geom.geometryType

##        if sr != self.spatialReference:
##            self.geometry_service = GeometryService()
##            gc = self.geometry_service.project(in_geom, in_geom.spatialReference, self.getSR())
##            in_geom = gc

        # The adjust aspect ratio doesn't seem to fix the clean pixel issue
        bbox = self.adjustbbox(in_geom.envelope())
        #if not self.compatible_with_version(10.3):
        #    bbox = self.adjustbbox(in_geom.envelope())
        #else:
        #    bbox = in_geom.envelope()

        # imageSR
        if IMAGE_SR not in kwargs:
            imageSR = sr
        else:
            imageSR = kwargs[IMAGE_SR]

        if NO_DATA in kwargs:
            nodata = kwargs[NO_DATA]

        # check for raster function availability
        if not self.allowRasterFunction:
            rendering_rule = None

        # find width and height for image size (round to whole number)
##        bbox_int = map(int, map(float, bbox.split(',')))
##        width = abs(bbox_int[0] - bbox_int[2])
##        height = abs(bbox_int[1] - bbox_int[3])
        bbox_int = map(int, map(float, bbox.split(',')))
        width = abs(bbox_int[0] - bbox_int[2]) / int(self.get(PIXEL_SIZE_X))
        height = abs(bbox_int[1] - bbox_int[3]) / int(self.get(PIXEL_SIZE_Y))

        matchType = NO_DATA_MATCH_ANY
        if ',' in str(nodata):
            matchType = NO_DATA_MATCH_ALL

        # set params
        p = {F: PJSON,
             RENDERING_RULE: rendering_rule,
             ADJUST_ASPECT_RATIO: TRUE,
             BBOX: bbox,
             FORMAT: TIFF,
             IMAGE_SR: imageSR,
             BBOX_SR: sr,
             SIZE: '{0},{1}'.format(width,height),
             PIXEL_TYPE: self.pixelType,
             NO_DATA_INTERPRETATION: '&'.join(map(str, filter(lambda x: x not in (None, ''),
                ['noData=' + str(nodata) if nodata != None else '', 'noDataInterpretation=%s' %matchType if nodata != None else matchType]))),
             INTERPOLATION: interp
            }

        # overwrite with kwargs
        for k,v in six.iteritems(kwargs):
            if k not in [SIZE, BBOX_SR]:
                p[k] = v

        # post request
        r = self.request(query_url, p)

        if r.get('href', None) is not None:
            tiff = self.request(r.get('href').strip(), ret_json=False).content
            with open(out_raster, 'wb') as f:
                f.write(tiff)
            print('Created: "{0}"'.format(out_raster))

    def clip(self, poly, out_raster, envelope=True, imageSR='', noData=None):
        """method to clip a raster"""
        # check for raster function availability
        if not self.allowRasterFunction:
            raise NotImplemented('This Service does not support Raster Functions!')
        if envelope:
            if not isinstance(poly, Geometry):
                poly = Geometry(poly)
            geojson = poly.envelopeAsJSON()
            geojson[SPATIAL_REFERENCE] = poly.json[SPATIAL_REFERENCE]
        else:
            geojson = Geometry(poly).dumps() if not isinstance(poly, Geometry) else poly.dumps()
        ren = {
          "rasterFunction" : "Clip",
          "rasterFunctionArguments" : {
            "ClippingGeometry" : geojson,
            "ClippingType": CLIP_INSIDE,
            },
          "variableName" : "Raster"
        }
        self.exportImage(poly, out_raster, rendering_rule=ren, noData=noData, imageSR=imageSR)

    def arithmetic(self, poly, out_raster, raster_or_constant, operation=RASTER_MULTIPLY, envelope=False, imageSR='', **kwargs):
        """perform arithmetic operations against a raster

        Required:
            poly -- input polygon or JSON polygon object
            out_raster -- full path to output raster
            raster_or_constant -- raster to perform opertion against or constant value

        Optional:
            operation -- arithmetic operation to use, default is multiply (3) all options: (1|2|3)
            envelope -- if true, will use bounding box of input features
            imageSR -- output image spatial reference

        Operations:
            1 -- esriRasterPlus
            2 -- esriRasterMinus
            3 -- esriRasterMultiply
        """
        ren = {
              "rasterFunction" : "Arithmetic",
              "rasterFunctionArguments" : {
                   "Raster" : "$$",
                   "Raster2": raster_or_constant,
                   "Operation" : operation
                 }
              }
        self.exportImage(poly, out_raster, rendering_rule=json.dumps(ren, ensure_ascii=False), imageSR=imageSR, **kwargs)

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
            for k,v in six.iteritems(kwargs):
                params_json[k] = v
        params_json['env:outSR'] = outSR
        params_json['env:processSR'] = processSR
        params_json[RETURN_Z] = returnZ
        params_json[RETURN_M] = returnZ
        params_json[F] = JSON
        r = self.request(gp_exe_url, params_json, ret_json=False)
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
