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
# or by calling restapi.getHelp()
#-------------------------------------------------------------------------------
import sys
import restapi
import os

# open help documentation
restapi.getHelp()

#----------------------------------------------------------------------------------------------------#
# Get Service properties
# connect USGS ArcGIS Server Instance
##usgs_rest_url = 'http://services.nationalmap.gov/ArcGIS/rest/services'
##
### no authentication is required, so no username and password are supplied
##ags = restapi.ArcServer(usgs_rest_url)
##
### get folder and service properties
##print 'Number of folders: {}'.format(len(ags.folders))
##print 'Number of services: {}'.format(len(ags.services))
##
### walk thru directories
##for root, folders, services in ags.walk():
##    print root
##    print folders
##    print services
##    print '\n'
##
### access "Structures" service
##structures = ags.get_MapService('structures')
##print structures.url #print MapService url
##
### print layer names
##print structures.list_layers()
##
### access "College/University" layer
##col = structures.layer('college/university')
##
### list fields from col layer
##print col.list_fields()
##
###----------------------------------------------------------------------------------------------------#
### search cursor
### run search cursor for colleges in Nebraska (maximimum limit may be 1000 records)
##query = "STATE = 'NE'"
##for row in col.cursor(where=query):
##    print row
##
### Note: can also do this from the MapService level like this:
### cursor = structures.cursor('college/university', where=query)
##
### export Nebraska "College/University" layer to feature class
### make scratch folder first
folder = os.path.join(os.environ['USERPROFILE'], r'Desktop\restapi_test_data')
if not os.path.exists(folder):
    os.makedirs(folder)

### export layer to shapefile
##output = os.path.join(folder, 'Nebraska_Universities.shp')
##col.layer_to_fc(output, where=query)
##
### export to KMZ
##kmz = os.path.join(folder, 'Nebraska_Universities.kmz')
##col.layer_to_kmz(kmz)
##
### clip col layer by polygon (Sacramento area)
##esri_json = {"rings":[[[-121.5,38.6],[-121.4,38.6],
##                      [-121.3,38.6],[-121.2,38.6],
##                      [-121.2,38.3],[-121.5,38.3],
##                      [-121.5,38.6]]],
##            "spatialReference":
##                {"wkid":4326,"latestWkid":4326}}
##
### clip by polygon (can use polygon shapefile or feature class as well)
##cali = os.path.join(folder, 'Sacramento_Universities.shp')
##col.clip(esri_json, cali)

#----------------------------------------------------------------------------------------------------#
# Geocoding examples
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

#----------------------------------------------------------------------------------------------------#
# feature service with attachment testing
fs_url = 'http://sampleserver3.arcgisonline.com/ArcGIS/rest/services/SanFrancisco/311Incidents/FeatureServer'
fs = restapi.FeatureService(fs_url)

# layer method returns a restapi.FeatureLayer, different from restapi.MapServiceLayer
incidents = fs.layer('incidents')

# grab 10 OID's
oids = incidents.getOIDs(max_recs=10)

# add new feature
adds = [{"attributes":
   	        {
        	"req_type":"Damaged Property",
        	"req_date":"07/19/2009",
        	"req_time":"11:08",
        	"address":"173 RESTAPI DR",
        	"x_coord":"-122.4167",
        	"y_coord":"37.7833",
        	"district":"7",
        	"status":1},
        "geometry": {"x":-122.4167,
                      "y":37.7833,
                      "spatialReference":
                          {"wkid":4326}}}]

result = incidents.addFeatures(adds)

# add attachment, get new OID from add results
oid = result.addResults[0]

# download python image
url = 'http://www.cis.upenn.edu/~lhuang3/cse399-python/images/pslytherin.png'
im = urllib.urlopen(url).read()
incidents.addAttachment(oid, im)

# get attachment info from service and download it
attachments = incidents.attachments(oid)

for attachment in attachments:
    print attachment
    attachment.download(folder) # download attachment into restapi_test_data folder on Desktop

# update the feature we just added
adds[0]['attributes']['address'] = 'Address Not Available'
adds[0]['attributes']['objectid'] = oid
incidents.updateFeatures(adds)

# now delete feature
incidents.deleteFeatures(oid)

# if sync enabled, we could create a replica like this:
# can pass in layer ID (0) or name ('incidents', not case sensative)
#replica = fs.createReplica(0, 'test_replica', geometry=adds[0]['geometry'], geometryType='esriGeometryPoint', inSR=4326)

# now export the replica object to file geodatabase (if arcpy access) or shapefile with hyperlinks (if open source)
#restapi.exportReplica(replica, folder)

#----------------------------------------------------------------------------------------------------#
# test image service
url = 'http://pca-gis02.pca.state.mn.us/arcgis/rest/services/Elevation/DEM_1m/ImageServer'
im = restapi.ImageService(url)

# clip DEM
geometry = {"rings":[[
                [400746.51698926091,157991.24543891847],
                [401243.04476705194,157991.24543891847],
                [401243.04476705194,157765.55099448562],
                [400746.51698926091,157765.55099448562],
                [400746.51698926091,157991.24543891847]]],
            "spatialReference":{"wkid":103793,"latestWkid":103793}}

tif = os.path.join(folder, 'dem.tif')
im.clip(geometry, tif)

# test point identify
x, y = 400994.780878, 157878.398217
elevation = im.pointIdentify(x=x, y=y, sr=103793)
print elevation

#----------------------------------------------------------------------------------------------------#
# Test Geoprocessing Service
gp_url = 'http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Network/ESRI_DriveTime_US/GPServer/CreateDriveTimePolygons'
gp = restapi.GPTask(gp_url)

point = {"geometryType":"esriGeometryPoint",
         "features":[
             {"geometry":{"x":-10603050.16225853,"y":4715351.1473399615,
                          "spatialReference":{"wkid":102100,"latestWkid":3857}}}],
         "sr":{"wkid":102100,"latestWkid":3857}}

# run task, passing in gp parameters as keyword arguments (**kwargs)
res = gp.run(Input_Location=str(point), Drive_Times = '1 2 3', inSR = 102100)
print res.results[0]['dataType']

