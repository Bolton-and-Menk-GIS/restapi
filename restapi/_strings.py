import os
import sys
import json

# esri fields
OID = 'esriFieldTypeOID'
SHAPE = 'esriFieldTypeGeometry'
GLOBALID = 'esriFieldTypeGlobalID'

# dictionaries
FTYPES = {'esriFieldTypeDate':'DATE',
          'esriFieldTypeString':'TEXT',
          'esriFieldTypeSingle':'FLOAT',
          'esriFieldTypeDouble':'DOUBLE',
          'esriFieldTypeSmallInteger':'SHORT',
          'esriFieldTypeInteger':'LONG',
          'esriFieldTypeGUID':'GUID',
          'esriFieldTypeGlobalID': 'GUID'}

SKIP_FIELDS = {
          'esriFieldTypeRaster':'RASTER',
          'esriFieldTypeBlob': 'BLOB'}

EXTRA ={'esriFieldTypeOID': 'OID@',
        'esriFieldTypeGeometry': 'SHAPE@'}

G_DICT = {'esriGeometryPolygon': 'Polygon',
          'esriGeometryPoint': 'Point',
          'esriGeometryPolyline': 'Polyline',
          'esriGeometryMultipoint': 'Multipoint',
          'esriGeometryEnvelope':'Envelope'}

GEOM_DICT = {'rings': 'esriGeometryPolygon',
             'paths': 'esriGeometryPolyline',
             'points': 'esriGeometryMultipoint',
             'x': 'esriGeometryPoint',
             'y': 'esriGeometryPoint'}

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

# Constants list for import *
CONSTANTS = ['OID', 'SHAPE', 'GLOBALID', 'FTYPES', 'G_DICT', 'GEOM_DICT', 'GEOM_CODE', 'PROJECTIONS',
             'PRJ_NAMES', 'PRJ_STRINGS', 'GTFS', 'LINEAR_UNITS']
