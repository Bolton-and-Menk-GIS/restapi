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
ATTRIBUTES = 'attributes'
FEATURES = 'features'

# common request parms
IN_SR = 'inSR'
OUT_SR = 'outSR'
SPATIAL_REL = 'spatialRel'
WHERE = 'where'
OBJECT_IDS = 'objectIds'
TIME = 'time'
DISTANCE = 'distance'
UNITS = 'units'
OUT_FIELDS = 'outFields'
RETURN_GEOMETRY = 'returnGeometry'
GEOMETRY_PRECISION = 'geometryPrecision'
RELATION_PARAM = 'relationParam'
RETURN_Z = 'returnZ'
RETURN_M = 'returnM'
RETURN_TRUE_CURVES = 'returnTrueCurves'
RETURN_IDS_ONLY = 'returnIdsOnly'
RESULT_RECORD_COUNT = 'resultRecordCount' # added at 10.3
RETURN_ATTACHMENTS = 'returnAttachments'
RETURN_ATTACHMENTS_DATA_BY_URL = 'returnAttachmentsDataByUrl'
RETURN_CATALOG_ITEMS = 'returnCatalogItems'
ROLLBACK_ON_FAILURE = 'rollbackOnFailure'
USE_GLOBALIDS = 'useGlobalIds'
GDB_VERSION = 'gdbVersion'
DATA_FORMAT = 'dataFormat'
ATTACHMENT = 'attachment'
ATTACHMENTS = 'attachments'
ADD_ATTACHMENT_RESULT = 'addAttachmentResult'
UPLOAD_ID = 'uploadId'
EDITS = 'edits'
FIELDS = 'fields'
ADDS = 'adds'
UPDATES = 'updates'
DELETES = 'deletes'
REPLICA_NAME = 'replicaName'
REPLICA_SR = 'replicaSR'
REPLICA_ID = 'replicaID'
REPLICA_OPTIONS = 'replicaOptions'
ASYNC = 'async'
SYNC_MODEL = 'syncModel'
LAYERS = 'layers'
LAYER_QUERIES = 'layerQueries'
BBOX = 'bbox'
BBOX_SR = 'bboxSR'
SIZE = 'size'
ADJUST_ASPECT_RATIO = 'adjustAspectRatio' # added at 10.3
NO_DATA = 'noData'
NO_DATA_INTERPRETATION = 'noDataInterpretation'
MOSAIC_RULE = 'mosaicRule'
RENDERING_RULE = 'renderingRule'
INTERPOLATION = 'interpolation'
COMPRESSION = 'compression'
COMPRESSION_QUALITY = 'compressionQuality'
BAND_IDS = 'bandIds'
IMAGE_SR = 'imageSR'
PIXEL_TYPE = 'pixelType'
GEOMETRIES = 'geometries'
DISTANCES = 'distances'
GEODESIC = 'geodesic'
DISTANCE_UNIT = 'distanceUnit'
TRANSFORMATION = 'transformation'
TRANSFORM_FORWARD = 'transformForward'
TRANSPORT_TYPE = 'transportType'
LOCATION = 'location'
LOCATIONS = 'locations'
CATEGORY = 'category'
SEARCH_EXTENT = 'searchExtent'
SINGLE_LINE = 'singleLine'
ADDRESSES = 'addresses'
SOURCE_COUNTRY = 'sourceCountry'
LANG_CODE = 'langCode' # added at 10.3
RETURN_INTERSECTION = 'returnIntersection'
SQL_FORMAT = 'sqlFormat'
CALC_EXPRESSION = 'calcExpression' # added at 10.3
SUPPORTS_CALCULATE = 'supportsCalculate'
USE_GEOMETRY = 'useGeometry'
PER_REPLICA = 'perReplica'
PER_LAYER = 'perLayer'
EXECUTE = 'execute'
TRANSPARENT = 'transparent'
SUBMIT_JOB = 'submitJob'
EXTENT = 'extent'
XMIN = 'xmin'
XMAX = 'xmax'
YMIN = 'ymin'
YMAX = 'ymax'
TRUE = 'true'
FALSE = 'false'
NULL = 'null'
NAME = 'name'
TYPE = 'type'
SR = 'sr'
F = 'f' # format

# misc
CURRENT_VERSION = 'currentVersion'
DPI = 'dpi'
TIFF = 'tiff'
FORMAT = 'format'
PJSON = 'pjson'
JSON = 'json'
LAYER_URL = 'layerURL'
SERVICES = 'services'
FOLDERS = 'folders'
LENGTH = 'length'
DOMAIN = 'domain'
SHAPE_TOKEN = 'SHAPE@'
OID_TOKEN = 'OID@'
CODED_VALUES = 'codedValues'
CODED = 'CODED'
RANGE = 'range'
RANGE_UPPER = 'RANGE'
RINGS = 'rings'
PATHS = 'paths'
POINTS = 'points'
X = 'x'
Y = 'y'


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

GEOM_DICT = {RINGS: ESRI_POLYGON,
             PATHS: ESRI_POLYLINE,
             POINTS: ESRI_MULTIPOINT,
             X: ESRI_POINT,
             Y: ESRI_POINT}

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
