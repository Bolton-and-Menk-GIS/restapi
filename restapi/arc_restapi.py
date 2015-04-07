# proprietary version (uses arcpy)
import urllib
import arcpy
import os
import json
from rest_utils import *

def poly_to_json(poly, envelope=False):
    """Converts a features to JSON

    Required:
        poly -- input features (does not have to be polygons)

    Optional:
        envelope -- if True, will use bounding box of input features
    """
    if isinstance(poly, dict): #already a JSON object
        return json.dumps(poly)
    if envelope:
        e = arcpy.Describe(poly).extent
        return ','.join(map(str, [e.XMin, e.YMin, e.XMax, e.YMax]))
    elif isinstance(poly, arcpy.Polygon):
        return poly.JSON
    with arcpy.da.SearchCursor(poly, ['SHAPE@JSON']) as rows:
        for row in rows:
            return row[0].encode('utf-8')

class Cursor(BaseCursor):
    """Class to handle Cursor object"""
    def __init__(self, url, fields='*', where='1=1', records=None, token='', add_params={}, get_all=False):
        super(Cursor, self).__init__(url, fields, where, records, token, add_params, get_all)

    def get_rows(self):
        """returns row objects"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects)

    def rows(self):
        """returns row values as tuple"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects).values

class Row(BaseRow):
    """Class to handle Row object"""
    def __init__(self, features, fields):
        super(Row, self).__init__(features, fields)

    @property
    def geometry(self):
        """returns arcpy geometry object
        Warning: output is unprojected
            use the projectAs(wkid, {transformation_name})
            methedto project geometry
        """
        if self.shape_field_ob:
            return arcpy.AsShape(self.esri_json, True)
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
            isShp = False
            # dump to in_memory if output is shape to handle field truncation
            if out_fc.endswith('.shp'):
                isShp = True
                shp_name = out_fc
                out_fc = r'in_memory\temp_xxx'

            arcpy.env.overwriteOutput = True
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
            else:
                params['outSR'] = sr
            g_type = G_DICT[self.geometryType]
            path, fc_name = os.path.split(out_fc)
            arcpy.CreateFeatureclass_management(path, fc_name, g_type,
                                                spatial_reference=sr)

            # add all fields
            cur_fields = ['SHAPE@']
            for fld in fields:
                if fld.type not in [OID, SHAPE] + SKIP_FIELDS.keys():
                    if not any(['shape_' in fld.name.lower(),
                                'shape.' in fld.name.lower(),
                                '(shape)' in fld.name.lower()]):
                        arcpy.AddField_management(out_fc, fld.name.split('.')[-1],
                                                  FTYPES[fld.type],
                                                  field_length=fld.length,
                                                  field_alias=fld.alias)
                        cur_fields.append(fld.name)

            # insert cursor to write rows
            with arcpy.da.InsertCursor(out_fc, [f.split('.')[-1] for f in cur_fields]) as irows:
                for row in self.cursor(cur_fields, where, records, params, get_all).rows():
                    irows.insertRow(row)

            del irows

            # if output is a shapefile
            if isShp:
                out_fc = arcpy.management.CopyFeatures(out_fc, shp_name)
            print 'Created: "{0}"'.format(out_fc)
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
             flds -- list of fields for fc. If none specified, all fields are returned.
                Supports fields in list [] or comma separated string "field1,field2,.."
            out_sr -- output spatial refrence (WKID)
            where -- optional where clause
            envelope -- if true, the polygon features bounding box will be used.  This option
                can be used if the feature has many vertices or to check against the full extent
                of the feature class
        """
        if isinstance(poly, dict):
            sr = poly['spatialReference']['wkid']
            if 'rings' in poly:
                shape = 'Polygon'
            elif 'paths' in poly:
                shape = 'Polyline'
            else:
                shape = 'Point'
        else:
            desc = arcpy.Describe(poly)
            sr = desc.spatialReference.factoryCode
            shape = desc.shapeType
        if envelope:
            shape = 'Envelope'
        geojson = poly_to_json(poly, envelope=envelope)
        if not out_sr:
            out_sr = sr
        d = {'geometryType' : 'esriGeometry{0}'.format(shape),
             'geometry': geojson, 'inSR' : sr, 'outSR': out_sr}
        return self.layer_to_fc(output, out_sr, where, d, flds, get_all=True)

class ImageService(BaseImageService):
    """Class to handle map service and requests"""
    def __init__(self, url, usr='', pw='', token=''):
        super(ImageService, self).__init__(url, usr, pw, token)

    def exportImage(self, poly, out_raster, envelope=False, rendering_rule={}, interp='RSP_BilinearInterpolation'):
        """method to export an AOI from an Image Service

        Required:
            poly -- polygon features
            out_raster -- output raster image

        Optional:
            envelope -- option to use envelope of polygon
            rendering_rule -- rendering rule to perform raster functions

        """
        if not out_raster.endswith('.tif'):
            out_raster = os.path.splitext(out_raster)[0] + '.tif'
        query_url = '/'.join([self.url, 'exportImage'])
        geojson = poly_to_json(poly, envelope)
        desc = arcpy.Describe(poly)
        e = desc.extent
        bbox = self.adjustbbox([e.XMin, e.YMin, e.XMax, e.YMax])
        sr = desc.spatialReference.factoryCode

        # check for raster function availability
        if not self.allowRasterFunction:
            rendering_rule = ''

        # find width and height for image size (round to whole number)
        bbox_int = map(int, bbox.split(','))
        width = abs(bbox_int[0] - bbox_int[2])
        height = abs(bbox_int[1] - bbox_int[3])

        # set params
        p = {'f':'pjson',
             'renderingRule': rendering_rule,
             'bbox': bbox,
             'format': 'tiff',
             'imageSR': sr,
             'bboxSR': sr,
             'size': '{0},{1}'.format(width,height),
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
            try:
                arcpy.management.CalculateStatistics(out_raster)
            except:
                pass
            print 'Created: "{0}"'.format(out_raster)

    def clip(self, poly, out_raster, envelope=False):
        """method to clip a raster"""
        geojson = poly_to_json(poly, envelope)
        ren = {
          "rasterFunction" : "Clip",
          "rasterFunctionArguments" : {
            "ClippingGeometry" : json.loads(geojson),
            "ClippingType": 1
            },
          "variableName" : "Raster"
        }
        self.exportImage(poly, out_raster, rendering_rule=ren)
