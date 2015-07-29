#-------------------------------------------------------------------------------
# Open source version
# special thanks to geospatial python for shapefile module
#-------------------------------------------------------------------------------
import urllib
import shapefile
import shp_helper
import os
import json
from rest_utils import *

# field types for shapefile module
SHP_FTYPES = {
          'esriFieldTypeDate':'D',
          'esriFieldTypeString':'C',
          'esriFieldTypeSingle':'F',
          'esriFieldTypeDouble':'F',
          'esriFieldTypeSmallInteger':'N',
          'esriFieldTypeInteger':'N',
          'esriFieldTypeGUID':'C',
          'esriFieldTypeRaster':'B',
          'esriFieldTypeGlobalID': 'C'
          }

def project(SHAPEFILE, wkid):
    """creates .prj for shapefile

    Required:
        SHAPEFILE -- full path to shapefile
        wkid -- well known ID for spatial reference
    """
    try:
        path = os.path.dirname(__file__)
    except:
        import sys
        path = os.path.abspath(os.path.dirname(sys.argv[0]))
    prj_json = os.path.join(path, 'shapefile', 'projections.json')
    prj_dict = json.loads(open(prj_json).read())

    # write .prj file
    prj_file = os.path.splitext(SHAPEFILE)[0] + '.prj'
    with open(prj_file, 'w') as f:
        f.write(prj_dict[str(wkid)].replace("'", '"'))
    del prj_dict
    return prj_file

def get_bbox(poly):
    """gets a bounding box"""
    sf = shapefile.Reader(poly)
    shape = sf.shape()
    if shape.shapeType not in (5,15,25):
        raise ValueError('"{0}" does not contain polygon features!'.format(poly))
    return ','.join(map(str, shape.bbox))

def poly_to_json(poly, wkid=3857, envelope=False):
    """Converts a features to JSON

    Required:
        poly -- input features (does not have to be polygons)
        wkid -- well known ID for spatial Reference

    Optional:
        envelope -- if True, will use bounding box of input features
    """
    if isinstance(poly, dict): #already a JSON object
        return json.dumps(poly)
    sf = shapefile.Reader(poly)
    shape = sf.shape()
    if shape.shapeType not in (5,15,25):
        raise ValueError('"{0}" does not contain polygon features!'.format(poly))
    if envelope:
        return ','.join(map(str, shape.bbox))
    else:
        # add parts
        part_indices = shape.parts
        if len(part_indices) >= 2:
            rings = []
            st = 0
            for pi in part_indices[1:]:
                rings.append(shape.points[st:pi])
                st += pi
                if pi == part_indices[-1]:
                    rings.append(shape.points[pi:])
                    break
        else:
            rings = [shape.points]
        ring_dict = {"rings": rings, "spatialReference":{"wkid":wkid}}
        return ring_dict

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
        sr_dict = feature_set['spatialReference']
        if 'latestWkid' in sr_dict:
            outSR = int(sr_dict['latestWkid'])
        else:
            outSR = int(sr_dict['wkid'])

    g_type = feature_set['geometryType']

    # add all fields
    w = shp_helper.shp(G_DICT[g_type].upper(), out_fc)
    field_map = []
    for fld in fields:
        if fld.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
            if not any(['shape_area' in fld.name.lower(),
                        'shape_length' in fld.name.lower(),
                        'shape.' in fld.name.lower(),
                        '(shape)' in fld.name.lower(),
                        'ojbectid' in fld.name.lower(),
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
        w.add_row(row[-1], row[:-1])

    w.save()
    print 'Created: "{0}"'.format(out_fc)

    # write projection file
    project(out_fc, outSR)
    return out_fc

def exportReplica(replica, out_folder):
    """converts a restapi.Replica() to a Shapefiles

    replica -- input restapi.Replica() object, must be generated from restapi.FeatureService.createReplica()
    out_folder -- full path to folder location where new files will be stored.
    """
    if not hasattr(replica, 'replicaName'):
        print 'Not a valid input!  Must be generated from restapi.FeatureService.createReplica() method!'
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
            out_file = os.path.join(att_loc, attInfo['name'])
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

                w.add_row(geom, row)

            w.save()
            print 'Created: "{0}"'.format(out_fc)

            # write projection file
            project(out_fc, sr)

    return out_folder

class Cursor(BaseCursor):
    """Class to handle Cursor object"""
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
        super(Cursor, self).__init__(url, fields, where, records, token, add_params, get_all)

    def get_rows(self):
        """returns row objects"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects, self.spatialReference, self.geometryType)

    def rows(self):
        """returns row values as tuple"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects, self.spatialReference, self.geometryType).values

    def __iter__(self):
        """returns Cursor.rows() generator"""
        return self.rows()

class Row(BaseRow):
    """Class to handle Row object"""
    def __init__(self, features={}, fields=[], spatialReference=None, g_type=''):
        """Row object for Cursor

        Required:
            features -- features JSON object
            fields -- fields participating in cursor
            spatialReference -- spatial reference for geometry (ignored in this source version)
            g_type -- geometry type
        """
        super(Row, self).__init__(features, fields, spatialReference)
        self.geometryType = g_type

    @property
    def geometry(self):
        """returns REST API geometry as esri JSON"""
        if self.esri_json:
            g_type = G_DICT[self.geometryType]
            if g_type == 'Polygon':
                return self.esri_json['rings']

            elif g_type == 'Polyline':
                return self.esri_json['paths']

            elif g_type == 'Point':
                return [self.esri_json['x'], self.esri_json['y']]

            else:
                # multipoint - to do
                pass
        return None

    @property
    def oid(self):
        """returns the OID for row"""
        if self.oid_field_ob:
            return self.atts[self.oid_field_ob.name]
        return None

    @property
    def values(self):
        """returns values as tuple"""
        _values = [self.atts[f.name] for f in self.fields
                   if f.type != SHAPE]

        if self.geometry and self.shape_field_ob:
            _values.insert(self.fields.index(self.shape_field_ob), self.geometry)

        elif self.geometry:
            _values.append(self.geometry)

        return tuple(_values)

class GeocodeHandler(object):
    """class to handle geocode results"""
    __slots__ = ['spatialReference', 'results', 'fields', 'formattedResults']

    def __init__(self, geocodeResult):
        """geocode response object handler

        Required:
            geocodeResult -- GeocodeResult object
        """
        self.results = geocodeResult.results
        self.spatialReference = geocodeResult.spatialReference['wkid']

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

class ArcServer(BaseArcServer):
    """class to handle ArcServer connection"""
    def __init__(self, url, usr='', pw='', token=''):
        """Base REST Endpoint Object to handle credentials and get JSON response

        Required:
            url -- ArcGIS services directory

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(ArcServer, self).__init__(url, usr, pw, token)

    def get_MapService(self, name_or_wildcard):
        """method to return MapService Object, supports wildcards

        Required:
            name_or_wildcard -- service name or wildcard used to grab service name
                (ex: "moun_webmap_rest/mapserver" or "*moun*mapserver")
        """
        full_path = self.get_service_url(name_or_wildcard)
        if full_path:
            return MapService(full_path, token=self.token)

class MapService(BaseMapService):
    def __init__(self, url, usr='', pw='', token=''):
        """MapService object

        Required:
            url -- MapService url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(MapService, self).__init__(url, usr, pw, token)

    def layer(self, name):
        """Method to return a layer object with advanced properties by name

        Required:
            name -- layer name (supports wildcard syntax*)
        """
        layer_path = get_layer_url(self.url, name, self.token)
        if layer_path:
            return MapServiceLayer(layer_path, token=self.token)
        else:
            print 'Layer "{0}" not found!'.format(name)

    def cursor(self, layer_name, fields='*', where='1=1', records=None, add_params={}, get_all=False):
        """Cusor object to handle queries to rest endpoints

        Required:
           layer_name -- name of layer in map service

        Optional:
            fields -- option to limit fields returned.  All are returned by default
            where -- where clause for cursor
            records -- number of records to return (within bounds of max record count)
            token --
            add_params -- option to add additional search parameters
            get_all -- option to get all records in layer.  This option may be time consuming
                because the ArcGIS REST API uses default maxRecordCount of 1000, so queries
                must be performed in chunks to get all records
        """
        lyr = get_layer_url(self.url, layer_name, self.token)
        return Cursor(lyr, fields, where, records, self.token, add_params, get_all)

    def layer_to_fc(self, layer_name, out_fc, sr=None,
                    where='1=1', params={}, flds='*',
                    records=None, get_all=False):
        """Method to export a feature class from a service layer

        Required:
            layer_name -- name of map service layer to export to fc
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
        lyr = self.layer(layer_name)
        lyr.layer_to_fc(out_fc, sr, where, params, flds, records, get_all)

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

class MapServiceLayer(BaseMapServiceLayer):
    """Class to handle advanced layer properties"""
    def __init__(self, url='', usr='', pw='', token=''):
        """MapService Layer object

        Required:
            url -- MapService layer url (should include index to layer)

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(MapServiceLayer, self).__init__(url, usr, pw, token)

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
                                'ojbectid' in fld.name.lower(),
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
            return exportFeatureSet(out_fc, query_resp, sr)
        else:
            print 'Cannot convert layer: "{0}" to Feature Layer, Not a vector layer!'.format(self.name)

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
        if not out_sr:
            out_sr = self.spatialReference
        geojson = poly_to_json(poly, out_sr, envelope=envelope)
        d = {'geometryType' : 'esriGeometryPolygon',
             'geometry': str(geojson), 'inSR' : out_sr, 'outSR': out_sr}
        return self.layer_to_fc(output, fields, where, params=d, get_all=True, sr=out_sr)

class ImageService(BaseImageService):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
        """Base REST Endpoint Object to handle credentials and get JSON response

        Required:
            url -- image service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(ImageService, self).__init__(url, usr, pw, token)

    def exportImage(self, poly, out_raster, sr='', envelope=False, rendering_rule={}, interp='RSP_BilinearInterpolation'):
        """method to export an AOI from an Image Service

        Required:
            poly -- polygon features
            out_raster -- output raster image

        Optional:
            sr -- spatial reference. Use WKID
            envelope -- option to use envelope of polygon
            rendering_rule -- rendering rule to perform raster functions

        """
        if not out_raster.endswith('.tif'):
            out_raster = os.path.splitext(out_raster)[0] + '.tif'
        query_url = '/'.join([self.url, 'exportImage'])
        geojson = poly_to_json(poly, envelope)
        bbox = self.adjustbbox(get_bbox(poly))
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

        # post request
        r = POST(query_url, p, token=self.token)

               # check for errors
        if 'error' in r:
            if 'details' in r['error']:
                raise RuntimeError('\n'.join(r['error']['details']))

        elif 'href' in r:
            tiff = urllib.urlopen(r['href'].strip()).read()
            with open(out_raster, 'wb') as f:
                f.write(tiff)
            print 'Created: "{0}"'.format(out_raster)

    def clip(self, poly, out_raster, envelope=False):
        """method to clip a raster"""
        geojson = poly_to_json(poly, envelope)
        ren = {
          "rasterFunction" : "Clip",
          "rasterFunctionArguments" : {
            "ClippingGeometry" : geojson,
            "ClippingType": 1
            },
          "variableName" : "Raster"
        }
        self.exportImage(poly, out_raster, rendering_rule=ren)

class Geocoder(GeocodeService):
    """class to handle Geocoding operations"""
    def __init__(self, url, usr='', pw='', token=''):
        """Geocoder object, created from GeocodeService

        Required:
            url -- Geocode service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
        super(Geocoder, self).__init__(url, usr, pw, token)

    def exportResults(self, geocodeResultObject, out_fc):
        """exports the geocode results to feature class

        Required:
            geocodeResultObject -- results from geocode operation, must be of type
                GeocodeResult.
            out_fc -- full path to output shapefile
        """
        handler = GeocodeHandler(geocodeResultObject)
        if not handler.results:
            print 'Geocoder returned 0 results! Did not create output'
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
        print 'Created: "{}"'.format(out_fc)
        return out_fc
