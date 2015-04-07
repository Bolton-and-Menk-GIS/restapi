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
          'esriFieldTypeGUID':'L',
          'esriFieldTypeRaster':'B',
          'esriFieldTypeGlobalID': 'L'
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

class Cursor(BaseCursor):
    """Class to handle Cursor object"""
    def __init__(self, url, fields='*', where='1=1', records=None, token='', add_params={}, get_all=False):
        super(Cursor, self).__init__(url, fields, where, records, token, add_params, get_all)

    def get_rows(self):
        """returns row objects"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects, self.geometryType)

    def rows(self):
        """returns row values as tuple"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects, self.geometryType).values

class Row(BaseRow):
    """Class to handle Row object"""
    def __init__(self, features={}, fields=[], g_type=''):
        super(Row, self).__init__(features, fields)
        self.geometryType = g_type

    @property
    def geometry(self):
        """returns REST API geometry as esri JSON

        Warning: output is unprojected
        """
        if self.shape_field_ob:
            g_type = G_DICT[self.geometryType]
            if g_type == 'Polygon':
                return self.features['geometry']['rings']

            elif g_type == 'Polyline':
                return self.features['geometry']['paths']

            elif g_type == 'Point':
                return [self.features['geometry']['x'], self.features['geometry']['y']]

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
        if self.geometry:
            _values.insert(self.fields.index(self.shape_field_ob), self.geometry)
        return tuple(_values)

class ArcServer(BaseArcServer):
    """class to handle ArcServer connection"""
    def __init__(self, url, usr='', pw='', token=''):
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
        super(MapServiceLayer, self).__init__(url, usr, pw, token)

    def cursor(self, fields='*', where='1=1', records=None, add_params={}, get_all=False):
        """Run Cursor on layer, helper method that calls Cursor Object"""
        return Cursor(self.url, fields, where, records, self.token, add_params, get_all)

    def layer_to_fc(self, out_fc, sr=None, where='1=1', params={}, flds='*', records=None, get_all=False):
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
            if not flds:
                flds = '*'
            if flds:
                if flds == '*':
                    fields = self.fields
                else:
                    if isinstance(flds, list):
                        pass
                    elif isinstance(flds, basestring):
                        flds = flds.split(',')
                    fields = [f for f in self.fields if f.name in flds]

            # make new feature class
            if not sr:
                sr = self.spatialReference
            g_type = G_DICT[self.geometryType]

            # add all fields
            w = shp_helper.shp(g_type, out_fc)
            field_map = []
            for fld in fields:
                if fld.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
                    if not 'shape' in fld.name.lower():
                        field_name = fld.name.split('.')[-1][:10]
                        field_type = SHP_FTYPES[fld.type]
                        field_length= str(fld.length)
                        w.add_field(field_name, field_type, field_length)
                        field_map.append((fld.name, field_name))

            # search cursor to write rows
            s_fields = ['SHAPE@'] + [f[0] for f in field_map]
            cursor = self.cursor(s_fields, where, records, params, get_all)
            for row in cursor.rows():
                w.add_row(row[0], row[1:])

            w.save()
            print 'Created: "{0}"'.format(out_fc)

            # write projection file
            project(out_fc, sr)
            return out_fc
        else:
            print 'Cannot convert layer: "{0}" to Feature Layer, Not a vector layer!'.format(self.name)

    def clip(self, poly, output, flds='*', out_sr='', where='', envelope=False):
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
        return self.layer_to_fc(output, out_sr, where, d, flds, get_all=True)

class ImageService(BaseImageService):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
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
