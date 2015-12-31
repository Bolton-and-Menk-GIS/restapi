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
import admin

# look for arcpy access, otherwise use open source version
# open source version may be faster.
try:
    import imp
    imp.find_module('arcpy')
    from arc_restapi import *
except ImportError:
    from open_restapi import *

# package info
__author__ = 'Caleb Mackey'
__organization__ = 'Bolton & Menk, Inc.'
__author_email__ = 'calebma@bolton-menk.com'
__website__ = 'https://github.com/Bolton-and-Menk-GIS/restapi'
__version__ = '0.1'
__package__ = 'restapi'
__documentation__ = 'http://gis.bolton-menk.com/restapi-documentation/restapi-module.html'

def getHelp():
    """call this function to open help documentation in a new tab"""
    import webbrowser
    webbrowser.open_new_tab(__documentation__)
    return __documentation__
