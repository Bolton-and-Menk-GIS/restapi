#-------------------------------------------------------------------------------
# Name:        restapi_samples
# Purpose:
#
# Author:      Caleb Mackey
#
# Created:     12/14/2014
# Copyright:   (c) calebma 2014
# Licence:     <your licence>
# Disclaimer:  The test services used below will be SLOW! The speed for the restapi
#   module depends on your connection speeds, the capabilities of the servers that
#   are being accessed, and the load the server is receiving at the time of each
#   request.
# help docs can be found @:
#   http://gis.bolton-menk.com/restapi-documentation/restapi-module.html
#-------------------------------------------------------------------------------
import sys
import restapi
import os

# connect USGS ArcGIS Server Instance
usgs_rest_url = 'http://services.nationalmap.gov/ArcGIS/rest/services'

# no authentication is required, so no username and password are supplied
ags = restapi.ArcServer(usgs_rest_url)

# get folder and service properties
print 'Number of folders: {}'.format(len(ags.folders))
print 'Number of services: {}'.format(len(ags.services))

# walk thru directories
for root, folders, services in ags.walk():
    print root
    print folders
    print services
    print '\n'

# access "Structures" service
structures = ags.get_MapService('structures')
print structures.url #print MapService url

# print layer names
print structures.list_layers()

# access "College/University" layer
col = structures.layer('college/university')

# list fields from col layer
print col.list_fields()

# run search cursor for colleges in Nebraska (maximimum limit may be 1000 records)
query = "STATE = 'NE'"
for row in col.cursor(where=query):
    print row

# Note: can also do this from the MapService level like this:
# cursor = structures.cursor('college/university', where=query)

# export Nebraska "College/University" layer to feature class
# make scratch folder first
folder = os.path.join(os.environ['USERPROFILE'], r'Desktop\restapi_test_data')
if not os.path.exists(folder):
    os.makedirs(folder)

# export layer to shapefile
output = os.path.join(folder, 'Nebraska_Universities.shp')
col.layer_to_fc(output, where=query)

# export to KMZ
col.layer_to_kmz()

# clip col layer by polygon (Sacramento area)
esri_json = {"rings":[[[-121.5,38.6],[-121.4,38.6],
                      [-121.3,38.6],[-121.2,38.6],
                      [-121.2,38.3],[-121.5,38.3],
                      [-121.5,38.6]]],
            "spatialReference":
                {"wkid":4326,"latestWkid":4326}}

# clip by polygon (can use polygon shapefile or feature class as well)
cali = os.path.join(folder, 'Sacramento_Universities.shp')
col.clip(esri_json, cali)

# hennepin county, MN geocoder
henn = 'http://gis.hennepin.us/arcgis/rest/services/Locators/HC_COMPOSITE/GeocodeServer'
geocoder = restapi.Geocoder(henn)
# find target field, use the SingleLine address field by default
geoResult = geocoder.findAddressCandidates('353 N 5th St, Minneapolis, MN 55403')

# export results to shapefile
print 'found {} candidates'.format(len(geoResult))
geocoder.exportResults(geoResult, os.path.join(folder, 'target_field.shp'))

# geocoder
esri_url = 'http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Locators/ESRI_Geocode_USA/GeocodeServer'
esri_geocoder = restapi.Geocoder(esri_url)
# find candidates using key word arguments (**kwargs) to fill in locator fields, no single line option
candidates = esri_geocoder.findAddressCandidates(Address='380 New York Street', City='Redlands', State='CA', Zip='92373')
print 'Number of address candidates: {}'.format(len(candidates))
for candidate in candidates:
    print candidate.location

# export results to shapefile
out_shp = os.path.join(folder, 'Esri_headquarters.shp')
geocoder.exportResults(candidates, out_shp)
