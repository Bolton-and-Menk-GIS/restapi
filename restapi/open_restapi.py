#-------------------------------------------------------------------------------
# Open source version
# special thanks to geospatial python for shapefile module
#-------------------------------------------------------------------------------
from __future__ import print_function
import urllib
import shapefile
import shp_helper
import os
import json
from collections import OrderedDict
from rest_utils import *
from shapefile import shapefile

# field types for shapefile module
SHP_FTYPES = {
          DATE_FIELD:'D',
          TEXT_FIELD:'C',
          FLOAT_FIELD:'F',
          DOUBLE_FIELD:'F',
          SHORT_FIELD:'N',
          LONG_FIELD:'N',
          GUID_FIELD:'C',
          RASTER_FIELD:'B',
          BLOB_FIELD: 'B',
          GLOBALID: 'C'
          }

def project(SHAPEFILE, wkid):
    """creates .prj for shapefile

    Required:
        SHAPEFILE -- full path to shapefile
        wkid -- well known ID for spatial reference
    """
    # write .prj file
    prj_file = os.path.splitext(SHAPEFILE)[0] + '.prj'
    with open(prj_file, 'w') as f:
        f.write(PROJECTIONS[str(wkid)].replace("'", '"'))
    return prj_file

def exportFeatureSet(out_fc, feature_set, outSR=None):
    """export features (JSON result) to shapefile or feature class

    Required:
        out_fc -- output feature class or shapefile
        feature_set -- JSON response (feature set) obtained from a query

    Optional:
        outSR -- optional output spatial reference.  If none set, will default
            to SR of result_query feature set.
    """
    # validate features input (should be list or dict, preferably list)
    if isinstance(feature_set, basestring):
        try:
            feature_set = json.loads(feature_set)
        except:
            raise IOError('Not a valid input for "features" parameter!')

    if not isinstance(feature_set, dict) or not 'features' in feature_set:
        raise IOError('Not a valid input for "features" parameter!')

    # make new shapefile
    fields = [Field(f) for f in feature_set['fields']]

    if not outSR:
        sr_dict = feature_set[SPATIAL_REFERNCE]
        if LATEST_WKID in sr_dict:
            outSR = int(sr_dict[LATEST_WKID])
        else:
            outSR = int(sr_dict[WKID])

    g_type = feature_set[GEOMETRY_TYPE]

    # add all fields
    w = shp_helper.shp(G_DICT[g_type].upper(), out_fc)
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
                field_length = str(fld.length) if fld.length else "50"
                w.add_field(field_name, field_type, field_length)
                field_map.append((fld.name, field_name))

    # search cursor to write rows
    s_fields = [fl for fl in fields if fl.name in [f[0] for f in field_map]]
    for feat in feature_set['features']:
        row = Row(feat, s_fields, outSR, g_type).values
        w.add_row(row[-1], [v if v else ' ' for v in row[:-1]])

    w.save()
    print('Created: "{0}"'.format(out_fc))

    # write projection file
    project(out_fc, outSR)
    return out_fc

def exportFeaturesWithAttachments(out_ws, lyr_url, fields='*', where='1=1', token='', max_recs=None, get_all=False, **kwargs):
    """exports a map service layer with attachments.  Output is a shapefile.  New records will be created if there
    are multiple attachments per feature, thus creating a ONE TO ONE relationship as shapefiles cannot handle a
    ONE TO MANY relationship.

    Required:
        out_ws -- output location to put new file gdb
        lyr_url -- url to map service layer

    Optional:
        fields -- list of fields or comma separated list of desired fields. Default is all fields ('*')
        where -- where clause for query
        token -- token to handle security, only required if service is secured
        max_recs -- maximum number of records to return. Ignored if get_all is set to True.
        get_all -- option to exceed transfer limit
        **kwargs -- key word arguments to further filter query (i.e. geometry or outSR)
    """
    lyr = MapServiceLayer(url, token=token)

    # make sure there is an OID field
    oid_name = lyr.OID.name
    if isinstance(fields, basestring):
        if fields != '*':
            if 'OID@' not in fields or oid_name not in fields:
                fields += ',{}'.format(oid_name)

    elif isinstance(fields, list):
        if 'OID@' not in fields or oid_name not in fields:
            fields.append(oid_name)

    # get feature set
    cursor = lyr.cursor(fields, where, records=max_recs, add_params=kwargs, get_all=get_all)
    oid_index = [i for i,f in enumerate(cursor.field_objects) if f.type == OID][0]

    # form feature set and call export feature set
    fs = {'features': cursor.features,
          'fields': lyr.response['fields'],
          SPATIAL_REFERNCE: lyr.response['extent'][SPATIAL_REFERNCE],
          GEOMETRY_TYPE: lyr.geometryType}

    # create new shapefile
    out_fc = validate_name(os.path.join(out_ws, lyr.name + '.shp'))
    exportFeatureSet(out_fc, fs)

    # get attachments (OID will start at 1)
    att_folder = os.path.join(out_ws, '{}_Attachments'.format(os.path.basename(out_fc).split('.')[0]))
    if not os.path.exists(att_folder):
        os.makedirs(att_folder)

    att_dict = {}
    for i,row in enumerate(cursor.get_rows()):
        att_dict[i] = []
        for att in lyr.attachments(row.oid):
            out_att = att.download(att_folder, verbose=False)
            att_dict[i].append(out_att)

    # write attachment field for hyperlinks (duplicate features with multiple attachments)
    e = shp_helper.shpEditor(out_fc)
    e.add_field('PHO_LINK', 'C', '254')
    for i, attachments in att_dict.iteritems():
        for ac, att in enumerate(attachments):
            if ac >= 1:
                recs = list(e.records[i])
                recs[-1] = att
                e.add_row(e.shapes[i], recs)
            else:
                e.update_row(i, PHO_LINK=att)
    e.save()
    return out_fc

def exportReplica(replica, out_folder):
    """converts a restapi.Replica() to a Shapefiles

    replica -- input restapi.Replica() object, must be generated from restapi.FeatureService.createReplica()
    out_folder -- full path to folder location where new files will be stored.
    """
    if not hasattr(replica, 'replicaName'):
        print('Not a valid input!  Must be generated from restapi.FeatureService.createReplica() method!')
        return

    # attachment directory and gdb set up
    att_loc = os.path.join(out_folder, 'Attachments')
    if not os.path.exists(att_loc):
        os.makedirs(att_loc)

    # set schema and create feature classes
    for layer in replica.layers:

        # download attachments
        att_dict = {}
        for attInfo in layer.attachments:
            out_file = assignUniqueName(os.path.join(att_loc, attInfo['name']))
            with open(out_file, 'wb') as f:
                f.write(urllib.urlopen(attInfo['url']).read())
            att_dict[attInfo['parentGlobalId']] = out_file.strip()

        if layer.features:

            # make new feature class
            sr = layer.spatialReference

            out_fc = validate_name(os.path.join(out_folder, layer.name + '.shp'))
            g_type = G_DICT[layer.geometryType]

            # add all fields
            layer_fields = [f for f in layer.fields if f.type not in (SHAPE, OID)]
            w = shp_helper.shp(g_type, out_fc)
            guid = None
            field_map = []
            for fld in layer_fields:
                field_name = fld.name.split('.')[-1][:10]
                field_type = SHP_FTYPES[fld.type]
                if fld.type == 'esriFieldTypeGlobalID':
                    guid = fld.name
                field_length = str(fld.length) if fld.length else "50"
                w.add_field(field_name, field_type, field_length)
                field_map.append((fld.name, field_name))

            w.add_field('ATTCH_PATH', 'C', '254')

            # search cursor to write rows
            s_fields = [f[0] for f in field_map]
            date_indices = [i for i,f in enumerate(layer_fields) if f.type == 'esriFieldTypeDate']

            for feature in layer.features:
                row = [feature['attributes'][f] for f in s_fields]
                if guid:
                    row += [att_dict[feature['attributes'][guid]]]
                for i in date_indices:
                    row[i] = mil_to_date(row[i])

                g_type = G_DICT[layer.geometryType]
                if g_type == 'Polygon':
                    geom = feature['geometry']['rings']

                elif g_type == 'Polyline':
                     geom = feature['geometry']['paths']

                elif g_type == 'Point':
                     geom = [feature['geometry']['x'], feature['geometry']['y']]

                else:
                    # multipoint - to do
                    pass

                w.add_row(geom, [v if v else ' ' for v in row])

            w.save()
            print('Created: "{0}"'.format(out_fc))

            # write projection file
            project(out_fc, sr)

    return out_folder

def partHandler(shape):
    """builds multipart features if necessary, returns parts
    as a list.

    Required:
        shape -- shapefile._Shape() object
    """
    parts = []
    if isinstance(shape, shapefile._Shape):
        if hasattr(shape, 'parts'):
            # add parts
            part_indices = shape.parts
            if len(part_indices) >= 2:
                parts = []
                st = 0
                for pi in part_indices[1:]:
                    parts.append(shape.points[st:pi])
                    st += pi
                    if pi == part_indices[-1]:
                        parts.append(shape.points[pi:])
                        break
            else:
                parts = [shape.points]
    elif isinstance(shape, list):
        # check if multipart
        if any(isinstance(i, list) for i in shape):
            part_indices = [0] + [len(i) for i in iter(shape)][:-1]
            if len(part_indices) >= 2:
                parts = []
                st = 0
                for pi in part_indices[1:]:
                    parts.extend(shape[st:pi])
                    st += pi
                    if pi == part_indices[1:]:
                        parts.extend(shape[pi:])
                        break
            else:
                parts = [shape]
        else:
            parts = [shape]
    else:
        raise IOError('Not a valid shapefile._Shape() input!')
    return parts

class Geometry(object):
    """class to handle restapi.Geometry"""
    def __init__(self, geometry, spatialReference=None):
        """converts geometry input to restapi.Geometry object

        Required:
            geometry -- input geometry.  Can be shapefile._Shape(),
                a path to shapefile, or JSON object.

        Optional:
            spatailReference -- optional WKID for input coordinates.  Useful
                for a replacement of a WKT.
        """
        self.spatialReference = spatialReference
        self.geometryType = None
        self.JSON = OrderedDict2()
        if isinstance(geometry, shapefile._Shape):
            if geometry.shapeType in (1, 11, 21):
                self.geometryType = ESRI_POINT
            elif geometry.shapeType in (3, 13, 23):
                self.geometryType = ESRI_POLYLINE
            elif geometry.shapeType in (5,15, 25):
                self.geometryType = ESRI_POLYGON
            elif self.geometryType in (8, 18, 28):
                self.geometryType = ESRI_MULTIPOINT
            if self.geometryType != ESRI_POINT:
                self.JSON[JSON_CODE[self.geometryType]] = partHandler(geometry.points)
            else:
                self.JSON = OrderedDict2(zip(['x', 'y'], geometry.points[0]))

        elif isinstance(geometry, basestring):
            try:
                geometry = json.loads(geometry)
            except:
                # maybe it's a shapefile?
                if os.path.exists(geometry) and geometry.endswith('.shp'):
                    prj_file = os.path.splitext(geometry)[0] + '.prj'
                    if os.path.exists(prj_file):
                        with open(prj_file, 'r') as f:
                            prj_string = f.readlines()[0].strip()
                        if 'PROJCS' in prj_string:
                            name = prj_string.split('PROJCS["')[1].split('"')[0]
                        elif 'GEOGCS' in prj_string:
                            name = prj_string.split('GEOGCS["')[1].split('"')[0]
                        if name in PRJ_NAMES:
                            self.spatialReference = PRJ_NAMES[name]
                    r = shapefile.Reader(geometry)
                    if r.shapeType in (1, 11, 21):
                        self.geometryType = ESRI_POINT
                    elif r.shapeType in (3, 13, 23):
                        self.geometryType = ESRI_POLYLINE
                    elif r.shapeType in (5,15, 25):
                        self.geometryType = ESRI_POLYGON
                    elif self.geometryType in (8, 18, 28):
                        self.geometryType = ESRI_MULTIPOINT
                    if self.geometryType != ESRI_POINT:
                        self.JSON[JSON_CODE[self.geometryType]] = partHandler(r.shape())
                    else:
                        self.JSON = OrderedDict2(zip(['x', 'y'], r.shape().points[0]))
                else:
                    raise IOError('Not a valid geometry input!')

        if isinstance(geometry, dict):
            if SPATIAL_REFERNCE in geometry:
                sr_json = geometry[SPATIAL_REFERNCE]
                if LATEST_WKID in sr_json:
                    self.spatialReference = sr_json[LATEST_WKID]
                else:
                    try:
                        self.spatialReference = sr_json[WKID]
                    except:
                        raise IOError('No spatial reference found in JSON object!')
                if 'features' in geometry:
                    d = geometry['features'][0]
                    if 'geometry' in d:
                        d = geometry['features'][0]['geometry']
                    self.JSON = d
                elif 'geometry' in geometry:
                    for k,v in geometry['geometry']:
                        self.JSON[k] = v
                if not self.JSON:
                    if 'rings' in geometry:
                        self.JSON['rings'] = [geometry['rings']]
                        self.geometryType = GEOM_DICT['rings']
                    elif 'paths' in geometry:
                        self.JSON['paths'] = [geometry['paths']]
                        self.geometryType = GEOM_DICT['paths']
                    elif 'points' in geometry:
                        self.JSON['points'] = [geometry['points']]
                        self.geometryType = GEOM_DICT['points']
                    elif 'x' in geometry and 'y' in geometry:
                        self.JSON['x'] = geometry['x']
                        self.JSON['y'] = geometry['y']
                        self.geometryType = ESRI_POINT
                    else:
                        raise IOError('Not a valid JSON object!')
                if not self.geometryType and GEOMETRY_TYPE in geometry:
                    self.geometryType = geometry[GEOMETRY_TYPE]
        if not SPATIAL_REFERNCE in self.JSON and self.spatialReference:
            self.JSON[SPATIAL_REFERNCE] = {WKID: self.spatialReference}

    def envelope(self):
        """return an envelope from shape"""
        if self.geometryType != ESRI_POINT:
            coords = []
            for i in self.JSON[JSON_CODE[self.geometryType]]:
                coords.extend(i)
            XMin = min(g[0] for g in coords)
            YMin = min(g[1] for g in coords)
            XMax = max(g[0] for g in coords)
            YMax = max(g[1] for g in coords)
            return ','.join(map(str, [XMin, YMin, XMax, YMax]))
        else:
            return '{0},{1},{0},{1}'.format(self.JSON['x'], self.JSON['y'])

    def dumps(self):
        """retuns json as a string"""
        # cannot use json.dumps, fails to serialize some nested lists
        if self.geometryType == ESRI_POINT:
            if self.spatialReference:
                return '{"x":%s, "y":%s, "spatialReference":{"wkid": %s}}' %(self.JSON['x'],
                                                                             self.JSON['y'],
                                                                             self.spatialReference)
            else:
                return '{"x":%s, "y":%s}' %(self.JSON['x'], self.JSON['y'])
        if self.spatialReference:
            return '{"%s":%s, "spatialReference":{"wkid": %s}}' %(JSON_CODE[self.geometryType],
                                                                  self.JSON[JSON_CODE[self.geometryType]],
                                                                  self.spatialReference)
        else:
            return '{"%s":%s}' %(JSON_CODE[self.geometryType], self.JSON[JSON_CODE[self.geometryType]])

    def asShape(self):
        """returns geometry as shapefile._Shape() object"""
        shp = shapefile._Shape(shp_helper.shp_dict[self.geometryType.split('Geometry')[1].upper()])
        if self.geometryType != ESRI_POINT:
            shp.points = self.JSON[JSON_CODE[self.geometryType]]
        else:
            shp.points = [[self.JSON['x'], self.JSON['y']]]

        # check if multipart, will need to fix if it is
        if any(isinstance(i, list) for i in shp.points):
            coords = []
            part_indices = [0] + [len(i) for i in iter(shp.points)][:-1]
            for i in shp.points:
                coords.extend(i)
            shp.points = coords
            shp.parts = shapefile._Array('i', part_indices)
        else:
            shp.parts = shapefile._Array('i', [0])

        if shp.shapeType not in (0,1,8,18,28,31):
            XMin = min(coords[0] for coords in shp.points)
            YMin = min(coords[1] for coords in shp.points)
            XMax = max(coords[0] for coords in shp.points)
            YMax = max(coords[1] for coords in shp.points)
            shp.bbox = shapefile._Array('d', [XMin, YMin, XMax, YMax])

        return shp

    def __str__(self):
        """dumps JSON to string"""
        return self.dumps()


class GeocodeHandler(object):
    """class to handle geocode results"""
    __slots__ = [SPATIAL_REFERNCE, 'results', 'fields', 'formattedResults']

    def __init__(self, geocodeResult):
        """geocode response object handler

        Required:
            geocodeResult -- GeocodeResult object
        """
        self.results = geocodeResult.results
        self.spatialReference = geocodeResult.spatialReference[WKID]

    @property
    def fields(self):
        """returns collections.namedtuple with (name, type)"""
        res_sample = self.results[0]
        __fields = []
        for f, val in res_sample.attributes.iteritems():
            if isinstance(val, float):
                if val >= -3.4E38 and val <= 1.2E38:
                    __fields.append(FIELD_SCHEMA(name=f, type='F'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='D'))
            elif isinstance(val, (int, long)):
                __fields.append(FIELD_SCHEMA(name=f, type='I'))
            else:
                __fields.append(FIELD_SCHEMA(name=f, type='C'))
        return __fields

    @property
    def formattedResults(self):
        """returns a generator with formated results as Row objects"""
        for res in self.results:
            pt = (res.location['x'], res.location['y'])
            yield (pt,) + tuple(res.attributes[f.name] for f in self.fields)



class MapServiceLayer(BaseMapServiceLayer):
    """Class to handle advanced layer properties"""
    def __init__(self, url='', usr='', pw='', token='', proxy=None):
        """MapService Layer object

        Required:
            url -- MapService layer url (should include index to layer)

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
        """
        super(MapServiceLayer, self).__init__(url, usr, pw, token, proxy)

    def cursor(self, fields='*', where='1=1', records=None, add_params={}, get_all=False):
        """Run Cursor on layer, helper method that calls Cursor Object"""
        return Cursor(self.url, fields, where, records, self.token, add_params, get_all)

    def layer_to_fc(self, out_fc, fields='*', where='1=1', records=None, params={}, get_all=False, sr=None):
        """Method to export a feature class from a service layer

        Required:
            out_fc -- full path to output feature class

        Optional:
            sr -- output spatial refrence (WKID)
            where -- optional where clause
            params -- dictionary of parameters for query
            flds -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            records -- number of records to return. Default is none, will return maxRecordCount
            get_all -- option to get all records.  If true, will recursively query REST endpoint
                until all records have been gathered. Default is False.
        """
        if self.type == 'Feature Layer':
            if not fields:
                fields = '*'
            if fields == '*':
                _fields = self.fields
            else:
                if isinstance(fields, basestring):
                    fields = fields.split(',')
                _fields = [f for f in self.fields if f.name in fields]

            # make new feature class
            if not sr:
                sr = self.spatialReference
            g_type = G_DICT[self.geometryType]

            # add all fields
            w = shp_helper.shp(g_type, out_fc)
            field_map = []
            for fld in _fields:
                if fld.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
                    if not any(['shape_' in fld.name.lower(),
                                'shape.' in fld.name.lower(),
                                '(shape)' in fld.name.lower(),
                                'objectid' in fld.name.lower(),
                                fld.name.lower() == 'fid']):

                        field_name = fld.name.split('.')[-1][:10]
                        field_type = SHP_FTYPES[fld.type]
                        field_length = str(fld.length) if fld.length else "50"
                        w.add_field(field_name, field_type, field_length)
                        field_map.append((fld.name, field_name))

            # search cursor to write rows
            s_fields = [f[0] for f in field_map]
            if not self.SHAPE.name in s_fields and 'SHAPE@' not in s_fields:
                s_fields.append('SHAPE@')

            query_resp = self.cursor(s_fields, where, records, params, get_all).response
            return exportFeatureSet(out_fc, query_resp, outSR=sr)
        else:
            print('Cannot convert layer: "{0}" to Feature Layer, Not a vector layer!'.format(self.name))

    def clip(self, poly, output, fields='*', out_sr='', where='', envelope=False):
        """Method for spatial Query, exports geometry that intersect polygon or
        envelope features.

        Required:
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
        if isinstance(poly, Geometry):
            in_geom = poly
        else:
            in_geom = Geometry(poly)
        sr = in_geom.spatialReference
        if envelope:
            geojson = in_geom.envelope()
            geometryType = 'esriGeometryEnvelope'
        else:
            geojson = in_geom.dumps()
            geometryType = in_geom.geometryType

        if not out_sr:
            out_sr = sr

        d = {GEOMETRY_TYPE : geometryType,
             'geometry': geojson, 'inSR' : out_sr, 'outSR': out_sr}
        return self.layer_to_fc(output, fields, where, params=d, get_all=True, sr=out_sr)

class ImageService(BaseImageService):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token='', proxy=None):
        """Base REST Endpoint Object to handle credentials and get JSON response

        Required:
            url -- image service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
        """
        super(ImageService, self).__init__(url, usr, pw, token, proxy)

    def pointIdentify(self, **kwargs):
        """method to get pixel value from x,y coordinates or JSON point object

        Recognized key word arguments:
            geometry -- JSON point object as dictionary
            x -- x coordinate
            y -- y coordinate
            sr -- input spatial reference.  Should be supplied if spatial
                reference is different from the Image Service's projection

        geometry example:
            geometry = {"x":3.0,"y":5.0,"spatialReference":{"wkid":102100}}
        """
        IDurl = self.url + '/identify'

        geometry = {}
        if 'inSR' in kwargs:
            inSR = kwargs['inSR']
        else:
            inSR = self.spatialReference

        if 'geometry' in kwargs:
            g = kwargs['geometry']
            if isinstance(g, Geometry):
                g = geometry.dumps()
                inSR = g.spatialReference
            elif isinstance(g, dict):
                geometry = json.dumps(g)
            elif isinstance(g, basestring):
                geometry = g

        elif 'x' in kwargs and 'y' in kwargs:
            g = {'x': kwargs['x'], 'y': kwargs['y']}
            if 'sr' in kwargs:
                g["spatialReference"] = {"wkid": kwargs['sr']}
            else:
                g["spatialReference"] = {"wkid": self.spatialReference}
            geometry = json.dumps(g)

        params = {'geometry': geometry,
                  'inSR': inSR,
                  GEOMETRY_TYPE:ESRI_POINT,
                  'f':'json',
                  'returnGeometry': 'false',
                  'returnCatalogItems': 'false'
                  }

        for k,v in kwargs.iteritems():
            if k not in params:
                params[k] = v

        j = POST(IDurl, params, cookies=self._cookie)
        if 'value' in j:
            return j['value']

    def exportImage(self, poly, out_raster, sr='', envelope=False, rendering_rule={}, interp='RSP_BilinearInterpolation', **kwargs):
        """method to export an AOI from an Image Service

        Required:
            poly -- polygon features
            out_raster -- output raster image

        Optional:
            sr -- spatial reference. Use WKID
            envelope -- option to use envelope of polygon
            rendering_rule -- rendering rule to perform raster functions
            kwargs -- optional key word arguments for other parameters
        """
        if not out_raster.endswith('.tif'):
            out_raster = os.path.splitext(out_raster)[0] + '.tif'
        query_url = '/'.join([self.url, 'exportImage'])

        if isinstance(poly, Geometry):
            in_geom = poly
        else:
            in_geom = Geometry(poly)
        bbox = self.adjustbbox(in_geom.envelope())
        if not sr:
            sr = self.spatialReference

        # find width and height for image size (round to pixel size)
        bbox_int = map(int, bbox.split(','))
        width = abs(bbox_int[0] - bbox_int[2])
        height = abs(bbox_int[1] - bbox_int[3])

        # check for raster function availability
        if not self.allowRasterFunction:
            rendering_rule = ''

        # set params
        p = {'f':'pjson',
             'renderingRule': rendering_rule,
             'bbox': bbox,
             'format': 'tiff',
             'imageSR': sr,
             'bboxSR': sr,
             'size': '{0},{1}'.format(width, height),
             'pixelType': self.pixelType,
             'noDataInterpretation': 'esriNoMatchAny',
             'interpolation': interp
            }

        # overwrite with kwargs
        for k,v in kwargs.iteritems():
            if k not in ['size', 'bboxSR']:
                p[k] = v

        # post request
        r = POST(query_url, p, cookies=self._cookie)

               # check for errors
        if 'error' in r:
            if 'details' in r['error']:
                raise RuntimeError('\n'.join(r['error']['details']))

        elif 'href' in r:
            tiff = urllib.urlopen(r['href'].strip()).read()
            with open(out_raster, 'wb') as f:
                f.write(tiff)
            print('Created: "{0}"'.format(out_raster))

    def clip(self, poly, out_raster, envelope=False):
        """method to clip a raster"""
        if envelope:
            geojson = Geometry(poly).envelope() if not isinstance(poly, Geometry) else poly.envelope()
        else:
            geojson = Geometry(poly).dumps() if not isinstance(poly, Geometry) else poly.dumps()
        ren = {
          "rasterFunction" : "Clip",
          "rasterFunctionArguments" : {
            "ClippingGeometry" : geojson,
            "ClippingType": 1
            },
          "variableName" : "Raster"
        }
        self.exportImage(poly, out_raster, rendering_rule=ren)

    def arithmetic(self, poly, out_raster, raster_or_constant, operation=3, envelope=False, imageSR=''):
        """perform arithmetic operations against a raster

        Required:
            poly -- input polygon or JSON polygon object
            out_raster -- full path to output raster
            raster_or_constant -- raster to perform opertion against or constant value

        Optional:
            operation -- arithmetic operation to use (1|2|3)
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
        self.exportImage(poly, out_raster, rendering_rule=json.dumps(ren), imageSR=imageSR)

class Geocoder(GeocodeService):
    """class to handle Geocoding operations"""
    def __init__(self, url, usr='', pw='', token='', proxy=None):
        """Geocoder object, created from GeocodeService

        Required:
            url -- Geocode service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
            proxy -- option to use proxy page to handle security, need to provide
                full path to proxy url.
        """
        super(Geocoder, self).__init__(url, usr, pw, token, proxy)

    @staticmethod
    def exportResults(geocodeResultObject, out_fc):
        """exports the geocode results to feature class

        Required:
            geocodeResultObject -- results from geocode operation, must be of type
                GeocodeResult.
            out_fc -- full path to output shapefile
        """
        if isinstance(geocodeResultObject, GeocodeResult):
            handler = GeocodeHandler(geocodeResultObject)
            if not handler.results:
                print('Geocoder returned 0 results! Did not create output')
                return None

            # create shapefile
            w = shp_helper.shp('POINT', out_fc)
            for field in handler.fields:
                w.add_field(field.name, field.type, 254)

            # add values
            for values in handler.formattedResults:
                w.add_row(values[0], values[1:])
            w.save()

            # project shapefile
            project(out_fc, handler.spatialReference)
            print('Created: "{}"'.format(out_fc))
            return out_fc

        else:
            raise TypeError('{} is not a {} object!'.format(geocodeResultObject, GeocodeResult))
