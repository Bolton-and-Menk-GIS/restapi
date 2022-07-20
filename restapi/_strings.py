import os
import sys
import json

import six

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
SQL_TYPE = 'sqlType'
SQL_TYPE_OTHER = 'sqlTypeOther'

# geometries
ESRI_POLYGON = 'esriGeometryPolygon'
ESRI_POLYLINE = 'esriGeometryPolyline'
ESRI_POINT = 'esriGeometryPoint'
ESRI_MULTIPOINT = 'esriGeometryMultipoint'
ESRI_ENVELOPE = 'esriGeometryEnvelope'
NULL_GEOMETRY = 'null'

# common feature set keys, does not encompass all
SPATIAL_REFERENCE = 'spatialReference'
WKID = 'wkid'
LATEST_WKID = 'latestWkid'
WKT = 'wkt'
FID = 'FID'
GEOMETRY_TYPE = 'geometryType'
GEOMETRY = 'geometry'
ATTRIBUTES = 'attributes'
FEATURES = 'features'
PROPERTIES = 'properties'
GEOJSON = 'geojson'

# common request parms
USER_NAME = 'username'
PASSWORD = 'password'
CLIENT = 'client'
EXPIRATION = 'expiration'
EXPIRES = 'expires'
TOKEN = 'token'
AUTH_INFO = 'authInfo'
REFERER = 'referer'
TOKEN_SERVICES_URL = 'tokenServicesUrl'
SHORT_LIVED_TOKEN_VALIDITY = 'shortLivedTokenValidity'
REQUEST_IP = 'requestip'
DISPLAY_FIELD_NAME = 'displayFieldName'
FIELD_ALIASES = 'fieldAliases'
IN_SR = 'inSR'
OUT_SR = 'outSR'
SPATIAL_REL = 'spatialRel'
WHERE = 'where'
OBJECT_IDS = 'objectIds'
TIME = 'time'
DISTANCE = 'distance'
UNITS = 'units'
IMAGE = 'image'
LOCATORS = 'locators'
OUT_FIELDS = 'outFields'
RETURN_GEOMETRY = 'returnGeometry'
GEOMETRY_PRECISION = 'geometryPrecision'
RELATION_PARAM = 'relationParam'
MAX_RECORD_COUNT = 'maxRecordCount'
RETURN_Z = 'returnZ'
RETURN_M = 'returnM'
HAS_Z = 'hasZ'
HAS_M = 'hasM'
RETURN_TRUE_CURVES = 'returnTrueCurves'
RETURN_IDS_ONLY = 'returnIdsOnly'
RESULT_RECORD_COUNT = 'resultRecordCount' # added at 10.3
RETURN_ATTACHMENTS = 'returnAttachments'
HAS_ATTACHMENTS = 'hasAttachments'
ATTACHMENT_IDS = 'attachmentIds'
ATTACHMENT_ID = 'attachmentId'
SUPPORTS_APPLY_EDITS_WITH_GLOBALIDS = 'supportsApplyEditsWithGlobalIds'
RETURN_ATTACHMENTS_DATA_BY_URL = 'returnAttachmentsDataByUrl'
RETURN_CATALOG_ITEMS = 'returnCatalogItems'
ROLLBACK_ON_FAILURE = 'rollbackOnFailure'
USE_GLOBALIDS = 'useGlobalIds'
PARENT_GLOBALID = 'parentGlobalId'
PARENT_OBJECTID = 'parentObjectId'
GDB_VERSION = 'gdbVersion'
DATA_FORMAT = 'dataFormat'
ATTACHMENT = 'attachment'
ATTACHMENTS = 'attachments'
ATTACHMENT_GROUPS = 'attachmentGroups'
ATTACHMENT_INFOS = 'attachmentInfos'
CAN_APPLY_EDITS_WITH_ATTACHMENTS = 'canApplyEditsWithAttachments'
SUPPORTS_APPLY_EDITS_WITH_GLOBALIDS = 'supportsApplyEditsWithGlobalIds'
STATUS_URL = 'statusUrl'
URL_WITH_TOKEN = 'urlWithToken' # not in ArcGIS REST API, custom key
ADD_ATTACHMENT_RESULT = 'addAttachmentResult'
ADD_ATTACHMENT_RESULTS = 'addAttachmentResults'
UPDATE_ATTACHMENT_RESULT = 'updateAttachmentResult'
DELETE_ATTACHMENT_RESULTS = 'deleteAttachmentResults'
RELATED_RECORD_GROUPS = 'relatedRecordGroups'
RELATED_RECORDS = 'relatedRecords'
RELATIONSHIP_ID = 'relationshipId'
RELATIONSHIPS = 'relationships'
DEFINITION_EXPRESSION = 'definitionExpression'
UPLOAD_ID = 'uploadId'
CONTENT_TYPE = 'contentType'
DATA = 'data'
EDITS = 'edits'
FIELDS = 'fields'
ADDS = 'adds'
UPDATES = 'updates'
DELETES = 'deletes'
ADD_RESULTS = 'addResults'
UPDATE_RESULTS = 'updateResults'
DELETE_RESULTS = 'deleteResults'
SYNC_ENABLED = 'syncEnabled'
REPLICA_NAME = 'replicaName'
REPLICA_SR = 'replicaSR'
REPLICA_ID = 'replicaID'
REPLICA_OPTIONS = 'replicaOptions'
ASYNC = 'async'
SYNC_MODEL = 'syncModel'
TABLE = 'Table'
TABLES = 'tables'
LAYERS = 'layers'
LAYER_DEFS = 'layerDefs'
LAYER_QUERIES = 'layerQueries'
SUB_LAYER_IDS = 'subLayerIds'
BBOX = 'bbox'
BBOX_SR = 'bboxSR'
BUFFER_SR = 'bufferSR'
SIZE = 'size'
UNION_RESULTS = 'unionResults'
ADJUST_ASPECT_RATIO = 'adjustAspectRatio' # added at 10.3
NO_DATA = 'noData'
NO_DATA_INTERPRETATION = 'noDataInterpretation'
NO_DATA_MATCH_ANY = 'esriNoDataMatchAny'
NO_DATA_MATCH_ALL = 'esriNoDataMatchAll'
MOSAIC_RULE = 'mosaicRule'
RENDERING_RULE = 'renderingRule'
INTERPOLATION = 'interpolation'
COMPRESSION = 'compression'
COMPRESSION_QUALITY = 'compressionQuality'
BAND_IDS = 'bandIds'
IMAGE_SR = 'imageSR'
PIXEL_TYPE = 'pixelType'
PIXEL_SIZE_X = 'pixelSizeX'
PIXEL_SIZE_Y = 'pixelSizeY'
GEOMETRIES = 'geometries'
DISTANCES = 'distances'
GEODESIC = 'geodesic'
UNIT = 'unit'
DISTANCE_UNIT = 'distanceUnit'
TRANSFORMATION = 'transformation'
TRANSFORM_FORWARD = 'transformForward'
TRANSPORT_TYPE = 'transportType'
TRANSPORT_TYPE_URL = 'esriTransportTypeUrl'
LOCATION = 'location'
LOCATIONS = 'locations'
CATEGORY = 'category'
SEARCH_EXTENT = 'searchExtent'
EXTENT_OF_INTEREST = 'extentOfInterest'
NUM_OF_RESULTS = 'numOfResults'
SINGLE_LINE = 'singleLine'
ADDRESSES = 'addresses'
SOURCE_COUNTRY = 'sourceCountry'
LANG_CODE = 'langCode' # added at 10.3
RETURN_INTERSECTION = 'returnIntersection'
CANDIDATES = 'candidates'
SCORE = 'score'
ADDRESS = 'address'
FEATURE = 'feature'
FEATURE_COLLECTION = 'FeatureCollection'
FEATURE_LAYER = 'Feature Layer'
TABLE = 'Table'
SQL_FORMAT = 'sqlFormat'
CALC_EXPRESSION = 'calcExpression' # added at 10.3
SUPPORTS_CALCULATE = 'supportsCalculate'
USE_GEOMETRY = 'useGeometry'
PER_REPLICA = 'perReplica'
PER_LAYER = 'perLayer'
EXECUTE = 'execute'
SUBMIT_JOB = 'submitJob'
JOB_STATUS = 'jobStatus'
JOB_ID = 'jobId'
JOB_SUBMITTED = 'esriJobSubmitted'
JOB_EXECUTING = 'esriJobExecuting'
JOB_SUCCEEDED = 'esriJobSucceeded'
JOB_FAILED = 'esriJobFailed'
JOBS = 'jobs'
JOB_URL = 'jobUrl'
PARAM_URL = 'paramUrl'
PARAM_NAME = 'paramName'
DATA_TYPE = 'dataType'
GP_RECORDSET_LAYER = 'GPFeatureRecordSetLayer'
SYNCHRONOUS = 'esriExecutionTypeSynchronous'
ASYNCHRONOUS = 'esriExecutionTypeAsynchronous'
OUTPUT_PARAMETER = 'esriGPParameterDirectionOutput'
BILINEAR_INTERPOLATION = 'RSP_BilinearInterpolation'
TRANSPARENT = 'transparent'
RESULTS = 'results'
EDITING_INFO = 'editingInfo'
LAST_EDIT_DATE = 'lastEditDate'
VALUE = 'value'
EXTENT = 'extent'
INITIAL_EXTENT = 'initialExtent'
FULL_EXTENT = 'fullExtent'
XMIN = 'xmin'
XMAX = 'xmax'
YMIN = 'ymin'
YMAX = 'ymax'
TRUE = 'true'
FALSE = 'false'
NULL = 'null'
NAME = 'name'
TYPE = 'type'
TYPES = 'types'
ALIAS = 'alias'
SR = 'sr'
ID = 'id'
F = 'f' # format
NULLABLE = 'nullable'
EDITABLE = 'editable'
ADVANCED_QUERY_CAPABILITIES = 'advancedQueryCapabilities'
SUPPORTS_PAGINATION = 'supportsPagination'
RESULTOFFSET = 'resultOffset'
ORDER_BY_FIELDS = 'orderByFields'
EXCEED_TRANSFER_LIMIT = 'exceededTransferLimit'

# misc
DETAILS = 'details'
ERROR = 'error'
MESSAGE = 'message'
REFERER_HEADER = 'Referer'
VISIBLE = 'visible'
GLOBALID_CAMEL = 'globalId'
OBJECTID_FIELD = 'objectIdField'
GLOBALID_FIELD  = 'globalIdField'
PROTOTYPE = 'prototype'
TEMPLATES = 'templates'
DESCRIPTION = 'description'
SQLITE = 'sqlite'
RECORDS = 'records'
RESPONSE = 'response'
PROXY = 'proxy'
EXPORT_IMAGE = 'exportImage'
OBJECTID = 'OBJECTID'
RESULT_OBJECT_ID = 'objectId'
RESULT_GLOBAL_ID = 'globalId'
SUCCESS_STATUS = 'success'
AGS_TOKEN = 'agstoken'
CURRENT_VERSION = 'currentVersion'
FAILED_OIDS = 'failedOIDs'
AFFECTED_OIDS = 'affectedOIDs'
SUMMARY = 'summary'
URL = 'url'
URL_UPPER = 'URL'
DPI = 'dpi'
TIFF = 'tiff'
FORMAT = 'format'
PJSON = 'pjson'
JSON = 'json'
GEOJSON_FORMAT = 'geoJSON'
ESRI_JSON_FORMAT = 'esriJSON'
COORDINATES = 'coordinates'
CRS = 'crs'
LAYER_URL = 'layerURL'
SERVICES = 'services'
FOLDERS = 'folders'
LENGTH = 'length'
DOMAIN = 'domain'
SHAPE_TOKEN = 'SHAPE@'
OID_TOKEN = 'OID@'
OID_FIELD_NAME = 'objectIdFieldName'
GLOBALID_FIELD_NAME = 'globalIdFieldName'
CODED_VALUES = 'codedValues'
CODED = 'CODED'
CODE = 'code'
RANGE = 'range'
RANGE_UPPER = 'RANGE'
RINGS = 'rings'
PATHS = 'paths'
POINTS = 'points'
CURVE_RINGS = 'curveRings'
CURVE_PATHS = 'curvePaths'
X = 'x'
Y = 'y'
Z = 'z'
COPY_RUNTIME_GDB_TO_FILE_GDB = 'CopyRuntimeGdbToFileGdb'
DEFAULT_VALUE = 'defaultValue'
SQL_GLOBAL_ID_EXP = 'NEWID() WITH VALUES'
SQL_AUTO_DATE_EXP = 'GetDate() WITH VALUES'

# admin
ADMIN_URL = 'adminURL'
ESRI_EVERYONE = 'esriEveryone'
IS_ALLOWED = 'isAllowed'
PRINCIPAL = 'principal'
CAPABILITIES = 'capabilities'
EDITOR_TRACKING_INFO = 'editorTrackingInfo'
CHANGE_TRACKING = 'ChangeTracking'
HAS_STATIC_DATA = 'hasStaticData'
ROLES = 'roles'
USERS = 'users'
ROLENAME = 'rolename'
PRIVILEGE = 'privilege'
SUCCESS = 'success'
PERMISSIONS = 'permissions'
PERMISSION = 'permission'
CONSTRAINT = 'constraint'

# AGOL
USER = 'user'
AGOL_BASE = 'www.arcgis.com'
AGOL_TOKEN_SERVICE = 'https://www.arcgis.com/sharing/rest/generateToken'
AGOL_PORTAL_SELF = 'https://www.arcgis.com/sharing/portals/self'
IS_AGOL = 'isAGOL'
IS_PORTAL = 'isPortal'
IS_ADMIN = 'isAdmin'
PORTAL_USER = 'portalUser'
FULL_NAME = 'fullName'
PORTAL_INFO = 'portalInfo'
URL_KEY = 'urlKey'
ORG_MAPS = '.maps.arcgis.com'

# raster operations
RASTER_PLUS = 1
RASTER_MINUS = 2
RASTER_MULTIPLY = 3
CLIP_INSIDE = 1
CLIP_OUTSIDE = 2

# spatial relationships
ESRI_INTERSECT = 'esriSpatialRelIntersects'
ESRI_CONTAINS = 'esriSpatialRelContains'
ESRI_CROSSES = 'esriSpatialRelCrosses'
ESRI_ENVELOPE_INTERSECTS = 'esriSpatialRelEnvelopeIntersects'
ESRI_INDEX_INTERSECTS = 'esriSpatialRelIndexIntersects'
ESRI_OVERLAPS = 'esriSpatialRelOverlaps'
ESRI_TOUCHES = 'esriSpatialRelTouches'
ESRI_WITHIN = 'esriSpatialRelWithin'
ESRI_RELATION = 'esriSpatialRelRelation'

# esri units, caution - some operations use different kind of units
#  the below are safe for mapservice/layer/feature layer queries
ESRI_METER = 'esriSRUnit_Meter'
ESRI_MILE = 'esriSRUnit_StatuteMile'
ESRI_FOOT = 'esriSRUnit_Foot'
ESRI_KILOMETER = 'esriSRUnit_Kilometer'
ESRI_NAUTICAL_MILE = 'esriSRUnit_NauticalMile'
ESRI_US_NAUTICAL_MILE = 'esriSRUnit_USNauticalMile'

# admin constants (AGOL)
ADD_TO_DEFINITION = 'addToDefinition'
DELETE_FROM_DEFINITION = 'deleteFromDefinition'
STATUS = 'status'
REFRESH = 'refresh'
UPDATE_DEFINITION = 'updateDefinition'
TRUNCATE = 'truncate'
ATTACHMENT_ONLY = 'attachmentOnly'
SUPPORTS_TRUNCATE = 'supportsTruncate'

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
    BLOB_FIELD: 'BLOB'
}

EXTRA = {
    OID: 'OID@',  
    SHAPE: 'SHAPE@'
}

G_DICT = {
    ESRI_POLYGON: 'Polygon',
    ESRI_POINT: 'Point',
    ESRI_POLYLINE: 'Polyline',
    ESRI_MULTIPOINT: 'Multipoint',
    ESRI_ENVELOPE:'Envelope'
}

GEOM_DICT = {
    RINGS: ESRI_POLYGON,
    PATHS: ESRI_POLYLINE,
    POINTS: ESRI_MULTIPOINT,
    X: ESRI_POINT,
    Y: ESRI_POINT
}

FIELD_STRUCT = {ALIAS: NULL,
    NAME: NULL,
    TYPE: NULL,
    DOMAIN: NULL
}


JSON_DICT = {
    'rings': 'esriGeometryPolygon',
    'paths': 'esriGeometryPolyline',
    'points': 'esriGeometryMultipoint',
    'x': 'esriGeometryPoint',
    'y': 'esriGeometryPoint'
}

JSON_CODE = {v:k for k,v in six.iteritems(JSON_DICT)}

FIELD_KEYS_CREATE = [NAME, ALIAS, TYPE, DOMAIN, LENGTH]

GEOM_CODE = {v:k for k,v in six.iteritems(GEOM_DICT)}
BASE_PATTERN = 'http*://*/rest/services*'
PORTAL_BASE_PATTERN = 'http*://*/sharing/rest*'
PORTAL_SERVICES_PATTERN = 'http*://*/sharing/servers/*/rest/services/*'
VERSION = '2.2.1'
PACKAGE_NAME = 'restapi'
USER_AGENT = '{}/{} (Python)'.format(PACKAGE_NAME, VERSION)
GET = 'GET'
POST = 'POST'

PROTOCOL = ''

# WKID json files
try:
    JSON_PATH = os.path.dirname(__file__)
except:
    JSON_PATH = os.path.abspath(os.path.dirname(sys.argv[0]))

GEOJSON_POINT = 'Point'
GEOJSON_MULTIPOINT = 'MultiPoint'
GEOJSON_LINESTRING = 'LineString'
GEOJSON_MULTI_LINESTRING = 'MultiLineString'
GEOJSON_POLYGON = 'Polygon'
GEOJSON_MULTIPOLYGON = 'MultiPolygon'
