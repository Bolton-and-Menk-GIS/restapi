# proprietary version (uses arcpy)
import urllib
import arcpy
import os
import time
import json
from rest_utils import *
arcpy.env.overwriteOutput = True

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

def exportReplica(replica, out_folder):
    """converts a restapi.Replica() to a File Geodatabase

    replica -- input restapi.Replica() object, must be generated from restapi.FeatureService.createReplica()
    out_folder -- full path to folder location where new geodatabase will be stored.
                The geodatabase will be named the same as the replica
    """
    if not hasattr(replica, 'replicaName'):
        print 'Not a valid input!  Must be generated from restapi.FeatureService.createReplica() method!'
        return

    # attachment directory and gdb set up
    att_loc = os.path.join(out_folder, 'Attachments')
    if not os.path.exists(att_loc):
        os.makedirs(att_loc)

    out_gdb_name = arcpy.ValidateTableName(replica.replicaName, out_folder).split('.')[0] + '.gdb'
    gdb = arcpy.management.CreateFileGDB(out_folder, out_gdb_name, 'CURRENT').getOutput(0)

    # set schema and create feature classes
    for layer in replica.layers:

        # download attachments
        att_dict = {}
        for attInfo in layer.attachments:
            out_file = os.path.join(att_loc, attInfo['name'])
            with open(out_file, 'wb') as f:
                f.write(urllib.urlopen(attInfo['url']).read())
            att_dict[attInfo['parentGlobalId']] = out_file

        fc = os.path.join(gdb, arcpy.ValidateTableName(layer.name, gdb))

        arcpy.management.CreateFeatureclass(gdb, os.path.basename(fc), G_DICT[layer.geometryType],
                                            spatial_reference=layer.spatialReference)

        # set up schema
        guid, guidFieldName = None, None
        layer_fields = [f for f in layer.fields if f.type not in (SHAPE, OID)]
        for i, field in enumerate(layer_fields):

            if field.type == 'esriFieldTypeGlobalID':
                field_name = 'ORIG_GlobalID'
                guid = i
                guidFieldName = field.name
            else:
                field_name = field.name

            # set up domain if necessary
            gdb_domains = []
            if field.domain:
                if field.domain['name'] not in gdb_domains:
                    if 'codedValues' in field.domain:
                        dType = 'CODED'
                    else:
                        dType = 'RANGE'

                    arcpy.management.CreateDomain(gdb, field.domain['name'],
                                                  field.domain['name'],
                                                  FTYPES[field.type],
                                                  dType)
                    if dType == 'CODED':
                        for cv in field.domain['codedValues']:
                            arcpy.management.AddCodedValueToDomain(gdb, field.domain['name'], cv['code'], cv['name'])
                    else:
                        _min, _max = field.domain['range']
                        arcpy.management.SetValueForRangeDomain(gdb, field.domain['name'], _min, _max)

                    gdb_domains.append(field.domain['name'])

                field_domain = field.domain['name']
            else:
                field_domain = ''

            arcpy.management.AddField(fc, field_name, FTYPES[field.type],
                                        field_length=field.length,
                                        field_alias=field.alias,
                                        field_domain=field_domain)

        # set up field values
        fld_names = ['SHAPE@'] + [f.name for f in layer_fields]
        if guid != None:
            fld_names[guid + 1] = 'ORIG_GlobalID'
        date_indices = [i for i,f in enumerate(layer_fields) if f.type == 'esriFieldTypeDate']

        with arcpy.da.InsertCursor(fc, fld_names) as irows:
            for rowD in layer.features:
                row = [rowD['attributes'][f] if f in rowD['attributes']
                       else rowD['attributes'][guidFieldName] for f in fld_names[1:]]

                for i in date_indices:
                    row[i] = mil_to_date(row[i])

                shape = arcpy.AsShape(rowD['geometry'], True)
                irows.insertRow([shape] + row)

        # Enable Attachments
        if layer.attachments:
            arcpy.management.AddGlobalIDs(fc)
            arcpy.management.EnableAttachments(fc)

            # create temp table
            tmp_tab = r'in_memory\temp_photo_points'
            arcpy.management.CreateTable('in_memory', 'temp_photo_points')
            arcpy.management.AddField(tmp_tab, 'ORIG_GlobalID', 'TEXT')
            arcpy.management.AddField(tmp_tab, 'PATH', 'TEXT', field_length=255)
            arcpy.management.AddField(tmp_tab, 'PHOTO_NAME', 'TEXT', field_length=254)
            with arcpy.da.InsertCursor(tmp_tab, ['ORIG_GlobalID', 'PATH', 'PHOTO_NAME']) as irows:
                for k,v in att_dict.iteritems():
                    irows.insertRow((k,) + os.path.split(v))

            # add attachments
            arcpy.management.AddAttachments(fc, 'ORIG_GlobalID', tmp_tab, 'ORIG_GlobalID', 'PHOTO_NAME', in_working_folder=att_loc)
            arcpy.management.Delete(tmp_tab)

    print 'Created: "{}"'.format(gdb)
    return gdb

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
            yield Row(feature, self.field_objects)

    def rows(self):
        """returns Cursor.rows() as generator"""
        for feature in self.features[:self.records]:
            yield Row(feature, self.field_objects).values

    def __iter__(self):
        """returns Cursor.rows()"""
        return self.rows()

class Row(BaseRow):
    """Class to handle Row object"""
    def __init__(self, features, fields):
        """Row object for Cursor

        Required:
            features -- features JSON object
            fields -- fields participating in cursor
        """
        super(Row, self).__init__(features, fields)

    @property
    def geometry(self):
        """returns arcpy geometry object
        Warning: output is unprojected
            use the projectAs(wkid, {transformation_name})
            method to project geometry
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
                    __fields.append(FIELD_SCHEMA(name=f, type='FLOAT'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='DOUBLE'))
            elif isinstance(val, (int, long)):
                if abs(val) < 32768:
                    __fields.append(FIELD_SCHEMA(name=f, type='SHORT'))
                else:
                    __fields.append(FIELD_SCHEMA(name=f, type='LONG'))
            else:
                __fields.append(FIELD_SCHEMA(name=f, type='TEXT'))
        return __fields


    @property
    def formattedResults(self):
        """returns a generator with formated results as tuple"""
        for res in self.results:
            pt = arcpy.PointGeometry(arcpy.Point(res.location['x'],
                                                 res.location['y']),
                                                 self.spatialReference)

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
        try:
            # having issues if ran from Python Window in ArcMap
            arcpy.env.addOutputsToMap = False
        except:
            pass
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
                                '(shape)' in fld.name.lower(),
                                'ojbectid' in fld.name.lower(),
                                fld.name.lower() == 'fid']):
                        arcpy.AddField_management(out_fc, fld.name.split('.')[-1],
                                                  FTYPES[fld.type],
                                                  field_length=fld.length,
                                                  field_alias=fld.alias)
                        cur_fields.append(fld.name)

            # insert cursor to write rows (using arcpy.FeatureSet() is too buggy)
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
        """Image Service object

        Required:
            url -- image service url

        Optional (below params only required if security is enabled):
            usr -- username credentials for ArcGIS Server
            pw -- password credentials for ArcGIS Server
            token -- token to handle security (alternative to usr and pw)
        """
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
            out_fc -- full path to output feature class
        """
        handler = GeocodeHandler(geocodeResultObject)
        if not handler.results:
            print 'Geocoder returned 0 results! Did not create output'
            return None

        # make feature class
        path, name = os.path.split(out_fc)
        arcpy.management.CreateFeatureclass(path, name, 'POINT', spatial_reference=handler.spatialReference)
        for field in handler.fields:
            arcpy.management.AddField(out_fc, field.name, field.type, field_length=254)

        # add records
        fields = ['SHAPE@'] + [f.name for f in handler.fields]
        with arcpy.da.InsertCursor(out_fc, fields) as irows:
            for values in handler.formattedResults:
                irows.insertRow(values)
        print 'Created: "{}"'.format(out_fc)
        return out_fc
