import os
import sys
import json

# CONSTANTS
# esri fields
OID = 'esriFieldTypeOID'
SHAPE = 'esriFieldTypeGeometry'
GLOBALID = 'esriFieldTypeGlobalID'
DATE_FIELD = 'esriFieldTypeDate'
TEXT_FIELD = 'esriFieldTypeString'
FLOAT_FIELD = 'esriFieldTypeSingle'
DOUBLE_FIELD = 'esriFieldTypeDouble'
SHORT_FIELD = 'esriFieldTypeSmallInteger'
LONG_FIELD = 'esriFieldTypeInteger'
GUID_FIELD = 'esriFieldTypeGUID'
RASTER_FIELD = 'esriFieldTypeRaster'
BLOB_FIELD = 'esriFieldTypeBlob'

# geometries
ESRI_POLYGON = 'esriGeometryPolygon'
ESRI_POLYLINE = 'esriGeometryPolyline'
ESRI_POINT = 'esriGeometryPoint'
ESRI_MULTIPOINT = 'esriGeometryMultipoint'
ESRI_ENVELOPE = 'esriGeometryEnvelope'

# common feature set keys, does not encompass all
SPATIAL_REFERENCE = 'spatialReference'
WKID = 'wkid'
LATEST_WKID = 'latestWkid'
WKT = 'wkt'
GEOMETRY_TYPE = 'geometryType'
GEOMETRY = 'geometry'

# misc
SHAPE_TOKEN = 'SHAPE@'
OID_TOKEN = 'OID@'

# dictionaries
FTYPES = {DATE_FIELD:'DATE',
          TEXT_FIELD:'TEXT',
          FLOAT_FIELD:'FLOAT',
          DOUBLE_FIELD :'DOUBLE',
          SHORT_FIELD:'SHORT',
          LONG_FIELD:'LONG',
          GUID_FIELD:'GUID',
          GLOBALID: 'GUID'}

SKIP_FIELDS = {
          RASTER_FIELD:'RASTER',
          BLOB_FIELD: 'BLOB'}

EXTRA ={OID: 'OID@',
        SHAPE: 'SHAPE@'}

G_DICT = {ESRI_POLYGON: 'Polygon',
          ESRI_POINT: 'Point',
          ESRI_POLYLINE: 'Polyline',
          ESRI_MULTIPOINT: 'Multipoint',
          ESRI_ENVELOPE:'Envelope'}

GEOM_DICT = {'rings': ESRI_POLYGON,
             'paths': ESRI_POLYLINE,
             'points': ESRI_MULTIPOINT,
             'x': ESRI_POINT,
             'y': ESRI_POINT}

GEOM_CODE = {v:k for k,v in GEOM_DICT.iteritems()}
BASE_PATTERN = 'http*://*/rest/services*'
USER_AGENT = 'restapi (Python)'
PROTOCOL = ''

# WKID json files
try:
    JSON_PATH = os.path.dirname(__file__)
except:
    JSON_PATH = os.path.abspath(os.path.dirname(sys.argv[0]))

PROJECTIONS = json.loads(open(os.path.join(JSON_PATH, 'shapefile', 'projections.json')).read())
PRJ_NAMES = json.loads(open(os.path.join(JSON_PATH, 'shapefile', 'projection_names.json')).read())
PRJ_STRINGS = json.loads(open(os.path.join(JSON_PATH, 'shapefile', 'projection_strings.json')).read())
GTFS = json.loads(open(os.path.join(JSON_PATH, 'shapefile', 'gtf.json')).read())
LINEAR_UNITS = json.loads(open(os.path.join(JSON_PATH, 'shapefile', 'linearUnits.json')).read())

### Constants list for import *
##CONSTANTS = ['OID', 'SHAPE', 'GLOBALID', 'FTYPES', 'G_DICT', 'GEOM_DICT', 'GEOM_CODE', 'PROJECTIONS',
##             'PRJ_NAMES', 'PRJ_STRINGS', 'GTFS', 'LINEAR_UNITS']
