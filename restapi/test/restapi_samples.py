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
#-------------------------------------------------------------------------------
import restapi
import os

# connect USGS ArcGIS Server Instance
usgs_rest_url = 'http://services.nationalmap.gov/ArcGIS/rest/services'

# no authentication is required, so no username and password are supplied
ags = restapi.ArcServer(usgs_rest_url)

### get folder and service properties
print ags.folders
print ags.services

# access "Structures" service
structures = ags.get_MapService('structures')
print structures.url #print MapService url

# print layer names
print structures.list_layers()

# access "College/University" layer
col = structures.layer('college/university')

# list fields from col layer
print col.list_fields()

### run search cursor for colleges in Nebraska (maximimum limit may be 1000 records)
query = "STATE = 'NE'"
for row in col.cursor(where=query).rows():
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


