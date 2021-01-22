#-------------------------------------------------------------------------------
# Name:        restapi
# Purpose:     provides helper functions for Esri's ArcGIS REST API
#              -Designed for external usage
#
# Author:      Caleb Mackey
#
# Created:     10/29/2014
# Copyright:   (c) calebma 2014
# Licence:     BMI
#-------------------------------------------------------------------------------
from . import _strings
from .common_types import *
from . import enums
try:
    from . import admin
except:
    pass


__all__ = ['MapServiceLayer',  'ImageService', 'Geocoder', 'FeatureService', 'FeatureLayer', 'has_arcpy', '__opensource__',
           'exportFeatureSet', 'exportReplica', 'exportFeaturesWithAttachments', 'Geometry', 'GeometryCollection',
           'GeocodeService', 'GPService', 'GPTask', 'do_post', 'MapService', 'ArcServer', 'Cursor', 'FeatureSet',
           'generate_token', 'mil_to_date', 'date_to_mil', 'guessWKID', 'validate_name', 'exportGeometryCollection',
           'GeometryService', 'GeometryCollection', 'getFeatureExtent', 'JsonReplica', 'SQLiteReplica', 'force_open_source',
           'requestClient', 'set_request_client', 'get_request_client', 'get_request_method'] + \
           [d for d in dir(_strings) if not d.startswith('__')]

# package info
__author__ = 'Caleb Mackey'
__organization__ = 'Bolton & Menk, Inc.'
__author_email__ = 'Caleb.Mackey@bolton-menk.com'
__website__ = 'https://github.com/Bolton-and-Menk-GIS/restapi'
__version__ = _strings.VERSION
__documentation__ = 'http://gis.bolton-menk.com/restapi-documentation/restapi-module.html'
__keywords__ = ['rest', 'arcgis-server', 'requests', 'http', 'administration', 'rest-services']
__description__ = 'Python API for working with ArcGIS REST API. This package has been designed to ' + \
    'work with arcpy or open source and does not require arcpy. It will try to use arcpy if available ' + \
    'for some data conversions, otherwise will use open source options. Also included is a subpackage ' + \
    'for administering ArcGIS Server Sites.'

def getHelp():
    """call this function to open help documentation in a new tab"""
    import webbrowser
    webbrowser.open_new_tab(__documentation__)
    return __documentation__

get_help = getHelp

def open_geometry_helper():
    import webbrowser
    import os
    index = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'geometry-helper', 'index.html').replace('\\', '/')
    webbrowser.open('file:///{}'.format(index))
