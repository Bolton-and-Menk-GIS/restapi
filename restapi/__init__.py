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
from rest_utils import FeatureService, FeatureLayer, GeocodeService, GPService, GPTask

# look for arcpy access, otherwise use open source version
# open source version may be faster.
try:
    import imp
    imp.find_module('arcpy')
    from arc_restapi import Cursor, MapService, MapServiceLayer, ArcServer, ImageService, Geocoder
except ImportError:
    from open_restapi import Cursor, MapService, MapServiceLayer, ArcServer, ImageService, Geocoder

# package info
__author__ = 'Caleb Mackey'
__organization__ = 'Bolton & Menk, Inc.'
__author_email__ = 'calebma@bolton-menk.com'
__website__ = 'https://github.com/Bolton-and-Menk-GIS/restapi'
__version__ = '0.1'
