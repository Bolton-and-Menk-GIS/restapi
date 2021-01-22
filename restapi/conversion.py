import six
import numbers
from ._strings import *
from .rest_utils import FeatureCollection, FeatureSet
from munch import munchify

ESRI_GEOMETRY_PROPS = [RINGS, POINTS, PATHS, X, Y, Z]
ESRI_GEOMETRY_TYPES = [ESRI_POINT, ESRI_POLYGON, ESRI_MULTIPOINT, ESRI_ENVELOPE]
GEOMJSON_GEOMETRY_PROPS = [COORDINATES]
GEOJSON_GEOMETRY_TYPES = [GEOJSON_LINESTRING, GEOJSON_POINT, GEOJSON_MULTI_LINESTRING, GEOJSON_POLYGON, GEOJSON_MULTIPOLYGON, GEOJSON_MULTIPOINT]
EXTENT_PROPS = [XMIN, XMAX, YMIN, YMAX]
OID_ATTRIBUTES = [OBJECTID, 'objectid', FID]

GEOJSON_POLYGON_TYPES = [GEOJSON_POLYGON, GEOJSON_MULTIPOLYGON]
GEOJSON_LINE_TYPES = [GEOJSON_LINESTRING, GEOJSON_MULTI_LINESTRING]

GEOJSON_GEOMETRY_MAPPING = {
    GEOJSON_POINT: ESRI_POINT,
    GEOJSON_POLYGON: ESRI_POLYGON,
    GEOJSON_MULTIPOLYGON: ESRI_POLYGON,
    GEOJSON_LINESTRING: ESRI_POLYLINE,
    GEOJSON_MULTI_LINESTRING: ESRI_POLYLINE,
    GEOJSON_MULTIPOINT: ESRI_MULTIPOINT
}


def is_geojson(g):
    """ checks if json geometry is geojson format

    Args:
        dct (dict): json structure

    Returns:
        bool: returns True if json structure is a geojson type
    """
    # duck type check for restapi.Geometry
    if hasattr(g, JSON):
        g = getattr(g, JSON)
    if isinstance(g, dict):
        if TYPE in g:
            return g.get(TYPE) in GEOJSON_GEOMETRY_TYPES
        return COORDINATES in g
    return False


def is_arcgis(g):
    """ checks if json geometry is arcgis format

    Args:
        dct (dict): json structure

    Returns:
        bool: returns True if json structure is an arcgis type
    """
    # duck type check for restapi.Geometry
    if hasattr(g, JSON):
        g = getattr(g, JSON)
    if isinstance(g, dict):
        if TYPE in g:
            return g.get(TYPE) in ESRI_GEOMETRY_TYPES
        return any(map(lambda x: g.get(x), ESRI_GEOMETRY_PROPS))
    return False


def is_feature_set(dct):
    """ checks if json structure is a feature set

    Args:
        dct (dict): json structure

    Returns:
        bool: returns True if json structure is a feature set
    """
    if isinstance(dct, FeatureSet):
        return True
    if isinstance(dct, dict):
        return all(map(lambda x: dct.get(x), [FEATURES, FIELDS]))
    return False


def is_feature_collection(dct):
    """ checks if json structure is a feature collection

    Args:
        dct (dict): json structure

    Returns:
        bool: returns True if json structure is a feature collection
    """
    if isinstance(dct, FeatureCollection):
        return True
    if isinstance(dct, dict):
        return dct.get(TYPE) == FEATURE_COLLECTION
        # return all(map(lambda x: dct.get(x), [FEATURES, TYPE]))
    return False


def get_oid(dct):
    """ gets an OID

    Args:
        dct (dict): json structure to search for objectid

    Returns:
        int: the object id
    """
    if not isinstance(dct, dict):
        return None
    if ATTRIBUTES in dct:
        return get_oid(dct.get(ATTRIBUTES))
    elif PROPERTIES in dct:
        return get_oid(dct.get(PROPERTIES))
    for attr in OID_ATTRIBUTES:
        if attr in dct:
            return dct.get(attr)
    return None

def pointsEqual(a, b):
    """
    checks if 2 [x, y] points are equal
    https://github.com/chris48s/arcgis2geojson
    """
    for i in range(0, len(a)):
        if a[i] != b[i]:
            return False
    return True


def closeRing(coordinates):
    """
    checks if the first and last points of a ring are equal and closes the ring
    https://github.com/chris48s/arcgis2geojson
    """
    if not pointsEqual(coordinates[0], coordinates[len(coordinates) - 1]):
        coordinates.append(coordinates[0])
    return coordinates


def ringIsClockwise(ringToTest):
    """
    determine if polygon ring coordinates are clockwise. clockwise signifies
    outer ring, counter-clockwise an inner ring or hole.

    https://github.com/chris48s/arcgis2geojson
    """

    total = 0
    i = 0
    rLength = len(ringToTest)
    pt1 = ringToTest[i]
    pt2 = None
    for i in range(0, rLength - 1):
        pt2 = ringToTest[i + 1]
        total += (pt2[0] - pt1[0]) * (pt2[1] + pt1[1])
        pt1 = pt2

    return (total >= 0)

def vertexIntersectsVertex(a1, a2, b1, b2):
    """ functionality taken from arcgis2geojson library.

    https://github.com/chris48s/arcgis2geojson
    """
    uaT = (b2[0] - b1[0]) * (a1[1] - b1[1]) - (b2[1] - b1[1]) * (a1[0] - b1[0])
    ubT = (a2[0] - a1[0]) * (a1[1] - b1[1]) - (a2[1] - a1[1]) * (a1[0] - b1[0])
    uB = (b2[1] - b1[1]) * (a2[0] - a1[0]) - (b2[0] - b1[0]) * (a2[1] - a1[1])

    if uB != 0:
        ua = uaT / uB
        ub = ubT / uB

        if ua >= 0 and ua <= 1 and ub >= 0 and ub <= 1:
            return True

    return False


def arrayIntersectsArray(a, b):
    """ functionality taken from arcgis2geojson library.

    https://github.com/chris48s/arcgis2geojson
    """
    for i in range(0, len(a)-1):
        for j in range(0, len(b)-1):
            if vertexIntersectsVertex(a[i], a[i + 1], b[j], b[j + 1]):
                return True

    return False


def coordinatesContainPoint(coordinates, point):
    """ functionality taken from arcgis2geojson library.

    https://github.com/chris48s/arcgis2geojson
    """

    contains = False
    l = len(coordinates)
    i = -1
    j = l - 1
    while ((i + 1) < l):
        i = i + 1
        ci = coordinates[i]
        cj = coordinates[j]
        if ((ci[1] <= point[1] and point[1] < cj[1]) or (cj[1] <= point[1] and point[1] < ci[1])) and\
           (point[0] < (cj[0] - ci[0]) * (point[1] - ci[1]) / (cj[1] - ci[1]) + ci[0]):
            contains = not contains
        j = i
    return contains


def coordinatesContainCoordinates(outer, inner):
    """ functionality taken from arcgis2geojson library.

    https://github.com/chris48s/arcgis2geojson
    """
    intersects = arrayIntersectsArray(outer, inner)
    contains = coordinatesContainPoint(outer, inner[0])
    if not intersects and contains:
        return True
    return False


def convertRingsToGeoJSON(rings):
    """ functionality taken from arcgis2geojson library.

    https://github.com/chris48s/arcgis2geojson
    """

    outerRings = []
    holes = []
    x = None  # iterator
    outerRing = None  # current outer ring being evaluated
    hole = None  # current hole being evaluated

    # for each ring
    for r in range(0, len(rings)):
        ring = closeRing(rings[r])
        if len(ring) < 4:
            continue

        # is this ring an outer ring? is it clockwise?
        if ringIsClockwise(ring):
            polygon = [ring[::-1]]
            outerRings.append(polygon)  # wind outer rings counterclockwise for RFC 7946 compliance
        else:
            holes.append(ring[::-1])  # wind inner rings clockwise for RFC 7946 compliance

    uncontainedHoles = []

    # while there are holes left...
    while len(holes):
        # pop a hole off out stack
        hole = holes.pop()

        # loop over all outer rings and see if they contain our hole.
        contained = False
        x = len(outerRings) - 1
        while (x >= 0):
            outerRing = outerRings[x][0]
            if coordinatesContainCoordinates(outerRing, hole):
                # the hole is contained push it into our polygon
                outerRings[x].append(hole)
                contained = True
                break
            x = x-1

        # ring is not contained in any outer ring
        # sometimes this happens https://github.com/Esri/esri-leaflet/issues/320
        if not contained:
            uncontainedHoles.append(hole)

    # if we couldn't match any holes using contains we can try intersects...
    while len(uncontainedHoles):
        # pop a hole off out stack
        hole = uncontainedHoles.pop()

        # loop over all outer rings and see if any intersect our hole.
        intersects = False
        x = len(outerRings) - 1
        while (x >= 0):
            outerRing = outerRings[x][0]
            if arrayIntersectsArray(outerRing, hole):
                # the hole is contained push it into our polygon
                outerRings[x].append(hole)
                intersects = True
                break
            x = x-1

        if not intersects:
            outerRings.append([hole[::-1]])

    if len(outerRings) == 1:
        return {
            'type': 'Polygon',
            'coordinates': outerRings[0]
        }
    else:
        return {
            'type': 'MultiPolygon',
            'coordinates': outerRings
        }


def orientRings(poly):
    """ makes sure polygon rings or clockwise, ported from: 
    https://github.com/Esri/arcgis-to-geojson-utils

    Args:
        rings (list): polygon ring arrays

    Returns:
        list: flattened polygon rings
    """
    output = []
    toClose, polygon = poly[0], poly[1:]
    outerRing = closeRing(toClose)
    if len(outerRing) >= 4:
        if not ringIsClockwise(outerRing):
            outerRing = outerRing[::-1]
        output.append(outerRing)
        for p in polygon:
            hole = closeRing(p[:])
            if len(hole) >= 4:
                if ringIsClockwise(hole):
                    hole = hole[::-1]
                output.append(hole)
    return output

def flattenMultiplePolygonRings(rings):
    """ flattens polygon rings, ported from: 
    https://github.com/Esri/arcgis-to-geojson-utils

    Args:
        rings (list): polygon ring arrays

    Returns:
        list: flattened polygon rings
    """
    output = []
    for r in rings:
        poly = orientRings(r)
        # output.extend(poly)
        for ring in poly[::-1]:
            output.append(ring[:])
    return output

def arcgis_to_geojson(arcgis):
    """ converts an argis json geometry to geojson

    Args:
        arcgis (dict): esri geometry json structure

    Raises:
        TypeError: TypeError

    Returns:
        dict: geojson geometry
    """
    if is_geojson(arcgis):
        return munchify(arcgis)
    if is_arcgis(arcgis):
        geojson = {}

        # handle polygon
        if RINGS in arcgis:
            parts = arcgis.get(RINGS)[:]
            return munchify(convertRingsToGeoJSON(parts))

        # handle line
        elif PATHS in arcgis:
            try:
                parts = arcgis.get(PATHS)[:]
            except IndexError:
                return { TYPE: GEOJSON_LINESTRING, COORDINATES: [] }
            
            if len(parts) == 1:
                geojson[TYPE] = GEOJSON_LINESTRING
                geojson[COORDINATES] = parts[0]
            else:
                geojson[TYPE] = GEOJSON_MULTI_LINESTRING
                geojson[COORDINATES] = parts

        # handle multi points
        elif POINTS in arcgis:
            parts = arcgis.get(POINTS)[:]
            geojson[TYPE] = GEOJSON_MULTIPOINT
            geojson[COORDINATES] = parts

        # handle point
        elif X in arcgis and Y in arcgis:
            geojson[TYPE] = GEOJSON_POINT
            geojson[COORDINATES] = [arcgis.get(X), arcgis.get(Y)]
            if Z in arcgis:
                geojson[COORDINATES].append(arcgis.get(Z))

        # handle envelope
        elif all(map(lambda x: isinstance(arcgis.get(x), (int, float)), EXTENT_PROPS)):
            geojson[TYPE] = GEOJSON_POLYGON
            geojson[COORDINATES] = [[
                [arcgis.get(XMAX), arcgis.get(YMAX)],
                [arcgis.get(XMIN), arcgis.get(YMAX)],
                [arcgis.get(XMIN), arcgis.get(YMIN)],
                [arcgis.get(XMAX), arcgis.get(YMIN)],
                [arcgis.get(XMAX), arcgis.get(YMAX)]
            ]]

        return munchify(geojson)

    raise TypeError('Input is not a valid esri json format')


def geojson_to_arcgis(geojson, spatialReference=None):
    """ converts a geojson geometry to arcgis geometry

    Args:
        geojson (dict): geojson geometry structure

    Raises:
        TypeError: TypeError

    Returns:
        dict: arcgis geometry
    """
    if is_arcgis(geojson):
        return munchify(geojson)

    if is_geojson(geojson):
        arcgis = {}
        coords = geojson.get(COORDINATES)[:]
        geometryType = geojson.get(TYPE)

        # handle point
        if geometryType == GEOJSON_POINT:
            arcgis[X], arcgis[Y] = coords
            if len(coords) == 3:
                arcgis[Z] = coords[2]

        # handle multipoint
        elif geometryType == GEOJSON_MULTIPOINT:
            arcgis[HAS_M] = False
            arcgis[HAS_Z] = len(coords) == 3
            arcgis[POINTS] = coords
        
        # handle polygon types 
        elif geometryType in GEOJSON_POLYGON_TYPES + GEOJSON_LINE_TYPES:
            if geometryType == GEOJSON_LINESTRING:
                coords = [ coords ]
            
            if geometryType in GEOJSON_POLYGON_TYPES: 
                arcgis[RINGS] = orientRings(coords) if geometryType == GEOJSON_POLYGON else flattenMultiplePolygonRings(coords[:])

            elif geometryType in GEOJSON_LINE_TYPES:
                arcgis[PATHS] = coords

            arcgis[HAS_M] = False
            arcgis[HAS_Z] = False
            if len(coords):
                part = coords[0]
                if part:
                    arcgis[HAS_Z] = len(part) == 3
        
        if spatialReference:
            if isinstance(spatialReference, dict):
                accgis[SPATIAL_REFERENCE] = spatialReference
            elif isinstance(spatialReference, int):
                arcgis[SPATIAL_REFERENCE] = { WKID: spatialReference }
            elif isinstance(spatialReference, six.string_types):
                arcgis[SPATIAL_REFERENCE] = { WKT: spatialReference }

        return munchify(arcgis)

    raise TypeError('Input is not a valid geoJson format')


def featureSet_to_featureCollection(fs):
    """ converts a feature set to a feature collection

    Args:
        fs (dict): arcgis feature set

    Returns:
        dict: feature collection json structure
    """
    if is_feature_collection(fs):
        return FeatureCollection(fs)

    if not isinstance(fs, FeatureSet):
        fs = FeatureSet(fs)
    return FeatureCollection({ 
        TYPE: FEATURE_COLLECTION,
        FEATURES: [
            { 
                TYPE: FEATURE,
                ID: get_oid(ft.get(ATTRIBUTES)) or i+1,
                CRS: fs.getCRS(),
                PROPERTIES: ft.get(ATTRIBUTES), 
                GEOMETRY: arcgis_to_geojson(ft.get(GEOMETRY))
            } for i,ft in enumerate(fs.get(FEATURES, []))
        ] 
    })


def featureCollection_to_featureSet(fc, fields):
    """ converts a feature set to a feature collection

    Args:
        fs (dict): arcgis feature set
        fields (list): list of fields as json for feature set

    Returns:
        dict: feature collection json structure
    """
    if is_feature_set(fc):
        return FeatureSet(fc)

    if not isinstance(fc, FeatureCollection):
        fc = FeatureCollection(fc)

    return FeatureSet({ 
        FIELDS: fields,
        SPATIAL_REFERENCE: fc._spatialReference,
        FEATURES: [
            { 
                ATTRIBUTES: ft.get(PROPERTIES), 
                GEOMETRY: geojson_to_arcgis(ft.get(GEOMETRY))
            } for ft in fc.get(FEATURES, [])
        ] 
    })


if __name__ == '__main__':
    geoms = [
        { "type": "Point", "coordinates": [100.0, 0.0] },
        { "type": "LineString","coordinates": [ [100.0, 0.0], [101.0, 1.0] ]},
        {"type":"Polygon","coordinates":[[[100,0],[101,0],[101,1],[100,1],[100,0]],[[100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2]]]}, # with holes
        {"type":"Polygon","coordinates":[[[100,0],[101,0],[101,1],[100,1],[100,0]]]},
        { "type": "MultiPoint","coordinates": [ [100.0, 0.0], [101.0, 1.0] ]}, 
        {"type":"MultiLineString","coordinates":[[[100,0],[101,1]],[[102,2],[103,3]]]},
        {"type":"MultiPolygon","coordinates":[[[[102,2],[103,2],[103,3],[102,3],[102,2]]],[[[100,0],[101,0],[101,1],[100,1],[100,0]],[[100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2]]]]}

    ]
    print('orient yo: ', orientRings([[[100,0],[101,0],[101,1],[100,1],[100,0]]]))
    for d in geoms:
        print(d)
        arc = geojson_to_arcgis(d, 102100)
        print(json.dumps(arc))#, indent=2))
        print(json.dumps(arcgis_to_geojson(arc)))#, indent=0))
        print('\n\n***************************************************\n\n')