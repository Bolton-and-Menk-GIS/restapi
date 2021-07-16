from munch import munchify, Munch
import six

BASE_PATTERN = 'http*://*/rest/services*'
PORTAL_BASE_PATTERN = 'http*://*/sharing/rest*'
PORTAL_SERVICES_PATTERN = 'http*://*/sharing/servers/*/rest/services/*'
VERSION = '1.1'
PACKAGE_NAME = 'restapi'
USER_AGENT = '{} (Python)'.format(PACKAGE_NAME)

def _setter(lst=[]):
    if isinstance(lst, dict):
        return munchify(lst)

    d = {}
    if isinstance(lst, list):
        for i in lst:
            if isinstance(i, dict):
                for k,v in six.iteritems(i):
                    d[k] = v
            elif isinstance(i, six.string_types):
                d[i] = i
    return munchify(d)

geometry = _setter([
    {
        'type': 'geometryType',
        'precision': 'geometryPrecision',
        'polygon': 'esriGeometryPolygon',
        'point': 'esriGeometryPoint',
        'polyline': 'esriGeometryPolyline',
        'multipoint': 'esriGeometryMultipoint',
        'envelope': 'esriGeometryEnvelope',
        'extent': _setter([
            'xmax',
            'ymax',
            'xmin',
            'ymin',
            {
                'initial': 'initialExtent',
                'full': 'fullExtent'
            }
        ])
    }, {
        'relationships': {
            'intersect': 'esriSpatialRelIntersects',
            'contains': 'esriSpatialRelContains',
            'crosses': 'esriSpatialRelCrosses',
            'envelopeIntersects': 'esriSpatialRelEnvelopeIntersects',
            'overlaps': 'esriSpatialRelOverlaps',
            'touches': 'esriSpatialRelTouches',
            'within': 'esriSpatialRelWithin',
            'relation': 'esriSpatialRelRelation'
        }
    },
    'null',
    'spatialReference',
    'rings',
    'paths',
    'points',
    'curveRings',
    'curvePaths',
    'x',
    'y',
])

admin = _setter([
    'user',
    'adminUrl',
    'permission',
    'permissions',
    'esriEveryone',
    'isAllowed',
    'principal',
    'privilege',
    'rolename',
    'roles',
    'users',
    'addToDefinition',
    'deleteFromDefinition',
    'updateDefinition',
])

agol = _setter([
    {
        'urls': {
            'base': 'www.arcgis.com',
            'sharingRest': 'https://www.arcgis.com/sharing/rest',
            'tokenService': 'https://www.arcgis.com/sharing/rest/generateToken',
            'self': 'https://www.arcgis.com/sharing/portals/self',
            'orgMaps': '.maps.arcgis.com'
        }
    },
    'isAgol',
    'isPortal',
    'isAdmin',
    'portalUser',
    'fullName',
    'user',
    'urlKey',
])

spatialReference = _setter([
    'spatialReference',
    'wkid',
    'wkt'
])

service = _setter([
    'extent',
    'initialExtent',
    'fullExtent',
    'spatialReference',
    'supportsTruncate',
    'truncate'
])

# geometryLookup = munchify({v: k for k,v in six.iteritems(geometry) if k})

fields = _setter([
    {
        'oid': 'esriFieldTypeOID',
        'shape': 'esriFieldTypeGeometry',
        'globalId': 'esriFieldTypeGlobalID',
        'text': 'esriFieldTypeString',
        'string': 'esriFieldTypeString', #alias
        'date': 'esriFieldTypeDate',
        'float': 'esriFieldTypeSingle',
        'double': 'esriFieldTypeDouble',
        'short': 'esriFieldTypeSmallInteger',
        'long': 'esriFieldTypeInteger',
        'guid': 'esriFieldTypeGUID',
        'raster': 'esriFieldTypeRaster',
        'blob': 'esriFieldTypeBlob',
        'sql': 'sqlType',
        'sqlOther': 'sqlTypeOther',
        'shapeToken': 'SHAPE@',
        'oidToken': 'OID@',
        'oidField': 'objectIdFieldName',
        'globalId': 'globalIdFieldName',
    }, {
        'lookup': {
            'esriFieldTypeDate': 'DATE',
            'esriFieldTypeString': 'TEXT',
            'esriFieldTypeSingle': 'FLOAT',
            'esriFieldTypeDouble': 'DOUBLE',
            'esriFieldTypeSmallInteger': 'SHORT',
            'esriFieldTypeInteger': 'LONG'
        },
        'skip': {
            'esriFieldTypeRaster': 'RASTER',
            'esriFieldTypeBlob': 'BLOB'
        }
    },
    'nullable',
    'editable',
    'length',
    'domain',
])

domain = _setter([
    'codedValues',
    'code',
    'range',
    {
        'codedUpper': 'CODED',
        'rangeUpper': 'RANGE'
    }
])

headers = _setter(
   { 'referer': 'Referer' },
)

cookies = _setter([
    'agstoken'
])

operations = _setter([
    'exportImage'
])

featureSet = _setter([
    'geometry',
    'attributes',
    'displayFieldName',
    'fieldAliases',
    'fields'
])

formats = _setter([
    'json',
    'pjson',
    'geojson',
    'sqlite',
    'kmz',
    'tiff',
    'esriJSON',
    'geoJSON',
])

serviceInfo = _setter([
    'tables',
    'layers',
    'layerDefs',
    'subLayerIds'
])

geocoding = _setter([
    'location',
    'locations',
    'singleLine',
    'langCode',
    'sourceCounty',
    'addresses',
    'numOfResults',
    'candidates',
    'score',
    'address'
])

types = _setter({
    'featureCollection': 'FeatureCollection',
    'featureLayer': 'FeatureLayer',
    'table': 'Table',
    'layer': 'Layer',
})

gpService = _setter([
    {'operations': _setter([
        'submitJob',
        'execute'
    ])},
    'esriExecutionTypeSynchronous',
    'esriExecutionTypeAsynchronous'
])


service = _setter([
    'supportsCalculate',


])

params =  _setter([
    'f',
    'json',
    'pjson',
    'username',
    'client',
    'expiration',
    'token',
    'fields',
    'inSR',
    'outSR',
    'where',
    'time',
    'objectIds',
    'outFields',
    'geometry',
    'geometries',
    'units',
    'adds',
    'updates',
    'deletes',
    'bboxSR',
    'bbox',
    'bufferSR',
    'size',
    'format',
    'dpi',
    'unionResults',
    'currentVersion',
    'dataFormat',
    'relationParam',
    'maxRecordCount',
    'returnZ',
    'returnM',
    'returnTrueCurves',
    'returnIdsOnly',
    'resultRecordCount'
    'returnAttachments',
    'hasAttachments',
    'attachmentIds',
    'attachmentId',
    'definitionExpression',
    'supportsApplyEditsWithGlobalIds',
    'returnAttachmentsDataByUrl',
    'returnCatalogItems',
    'rollbackOnFailure',
    'useGlobalIds',
    'async',
    'syncModel',
    'unit',
    'distanceUnit',
    'distances',
    'transformation',
    'transformForward',
    'geodesic',
    'transportType',
    'category',
    'extentOfInterest'
])

response = _setter([
    'summary',
    'results',
    'attachment',
    'attachments',
    'attachmentInfos',
    'statusUrl',
    'success',
    'status',
    'error',
    'message',
    'globalIdField',
    'objectIdField',
    'parentGlobalId'
    'failedOIDs',
    'prototype',
    'templates',
    'description',
    'sqlite',
    'records',
    'addAttachmentResult',
    'addResults',
    'updateResults',
    'deleteResults',
    'addAttachmentResult',
    'updateAttachmentResult',
    'deleteAttachmentResults',
    'relatedRecordGroups',
    'relationships',
    'relatedRecords',
    'relationshipId',
    'updloadId',
    'contentType',
    'data',
    'location',
    'locations'
])


raster = _setter([
    'adjustAspectRatio',
    'noData',
    'noDataInterpretation',
    'esriNoDataMatchAll',
    'esriNoDataMatchAny',
    'mosaicRule',
    'renderingRule',
    'interpolation',
    'compression',
    'compressionQuality',
    'bandIds',
    'size',
    'imageSR',
    'transparent',
   {
        'bilinearInterpolation': 'RSP_BilinearInterpolation',
        'operations': {
           'plus': 1,
           'minus': 2,
           'multiply': 3,
           'clipInside': 1,
           'clipOutside': 2
       }
   }
])

misc = _setter([
    'crs',
    'urlWithToken',
    'proxy'
])

editing = _setter([
    {
        'info': 'editingInfo' ,
        'trackingInfo': 'editorTrackingInfo',
        'changeTracking': 'ChangeTracking'
    },
    'lastEditDate',
])

auth = _setter([
    'username',
    'password',
    'client',
    'expiration',
    'expires',
    'token',
    'referer',
    # spelling confusion alias
    {
        'referrer': 'referer',
        'info': 'authInfo'
    },
    'requestip',
    'tokenServicesUrl',
    'shortLivedTokenValidity'
])


linearUnits = munchify({
    'meter': 'esriSRUnit_Meter',
    'foot': 'esriSRUnit_Foot',
    'mile': 'esriSRUnit_StatuteMile',
    'kilometer': 'esriSRUnit_Kilometer',
    'nauticalMile': 'esriSRUnit_NauticalMile',
    'usNauticalMile': 'esriSRUnit_USNauticalMile'
})