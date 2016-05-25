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
#
# ERRORS COULD BE CAUSED BY SERVICES CHANGING URL's OR BEING SHUT OFF, THINK OF THIS
#  AS A SNAPSHOT IN TIME.  THIS IS MEANT ONLY TO SHOW CODE SAMPLES ON HOW TO ACCESS
#  DIFFERENT CLASSES!
#-------------------------------------------------------------------------------
import sys
import os
sys.path.append(os.path.dirname(sys.path[0]))
sys.path.append(os.path.dirname(os.path.dirname(sys.path[0])))
import restapi
import urllib

# open help documentation
restapi.getHelp()

#----------------------------------------------------------------------------------------------------#
# Get Service properties
# connect NOAA ArcGIS Server Instance
rest_url = 'http://gis.srh.noaa.gov/arcgis/rest/services'

# no authentication is required, so no username and password are supplied
ags = restapi.ArcServer(rest_url)

# get folder and service properties
print 'Number of folders: {}'.format(len(ags.folders))
print 'Number of services: {}'.format(len(ags.services))

# walk thru directories
for root, folders, services in ags.walk():
    print root
    print folders
    print services
    print '\n'

# access "ahps_gauges" service (stream gauges)
gauges = ags.get_MapService('ahps_gauges')
print gauges.url #print MapService url

# print layer names
print gauges.list_layers()

# access "observed river stages" layer
lyr = gauges.layer('observed_river_stages') #not case sensitive, also supports wildcard search (*)

# list fields from col layer
print lyr.list_fields()

#----------------------------------------------------------------------------------------------------#
# search cursor
# run search cursor for gauges in California
# (maximimum limit may be 1000 records, can use get_all=True to exceed transfer limit)
# can filter fields by putting a field list, can use actual shape field name to get
#  geometry or use the ArcGIS-like token "SHAPE@"
query = "state = 'CA'"
for row in lyr.cursor(where=query, fields=['SHAPE@', u'gaugelid', u'status', u'location']):
    print row

# Note: can also do this from the MapService level like this:
# cursor = gauges.cursor('observed_river_stages', where=query)

# export Nebraska "College/University" layer to feature class
# make scratch folder first
folder = os.path.join(os.environ['USERPROFILE'], r'Desktop\restapi_test_data')
if not os.path.exists(folder):
    os.makedirs(folder)

# export layer to shapefile (can also call from Map Service)
output = os.path.join(folder, 'California_Stream_Gauges.shp')
lyr.layer_to_fc(output, where=query, sr=102100) #override spatial reference with web mercator

# export to KMZ
kmz = output.replace('.shp', '.kmz')
lyr.layer_to_kmz(kmz, where=query)

# clip lyr by polygon (Sacramento area)
esri_json = {"rings":[[[-121.5,38.6],[-121.4,38.6],
                      [-121.3,38.6],[-121.2,38.6],
                      [-121.2,38.3],[-121.5,38.3],
                      [-121.5,38.6]]],
            "spatialReference":
                {"wkid":4326,"latestWkid":4326}}

# clip by polygon and filter fields (can use polygon shapefile or feature class as well)
sac = os.path.join(folder, 'Sacramento_gauges.shp')
lyr.clip(esri_json, sac, fields=['gaugelid', 'location'])

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

# Esri geocoder
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
tmp = os.path.join(os.path.dirname(sys.argv[0]), 'python.png')
with open(tmp, 'wb') as f:
    f.write(im)

# add attachment
incidents.addAttachment(oid, tmp)
os.remove(tmp)

# get attachment info from service and download it
attachments = incidents.attachments(oid)

for attachment in attachments:
    print attachment
    print attachment.contentType, attachment.size
    attachment.download(folder) # download attachment into restapi_test_data folder on Desktop

# update the feature we just added
adds[0]['attributes']['address'] = 'Address Not Available'
adds[0]['attributes']['objectid'] = oid
incidents.updateFeatures(adds)

# now delete feature
incidents.deleteFeatures(oid)

# if sync were enabled, we could create a replica like this:
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
                [240006.00808044084, 4954874.19629429],
                [240157.31010183255, 4954868.8053006204],
                [240154.85966611796, 4954800.0316874133],
                [240003.55764305394, 4954805.4226145679],
                [240006.00808044084, 4954874.19629429]]],
            "spatialReference":{"wkid":26915,"latestWkid":26915}}

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

# get a list of gp parameters (so we know what to pass in as kwargs)
print '\nGP Task "{}" parameters:\n'.format(gp.name)
for p in gp.parameters:
    print '\t', p.name, p.dataType

point = {"geometryType":"esriGeometryPoint",
         "features":[
             {"geometry":{"x":-10603050.16225853,"y":4715351.1473399615,
                          "spatialReference":{"wkid":102100,"latestWkid":3857}}}],
         "sr":{"wkid":102100,"latestWkid":3857}}

# run task, passing in gp parameters as keyword arguments (**kwargs)
gp_res = gp.run(Input_Location=str(point), Drive_Times = '1 2 3', inSR = 102100)

# returns a GPResult() object, can get at the first result by indexing (usually only one result)
# can test if there are results by __nonzero__()
if gp_res:
    result = gp_res.results[0]

    # this returned a GPFeatureRecordSetLayer as an outputParameter, so we can export this to polygons
    print '\nOutput Result: "{}", data type: {}\n'.format(result.paramName, result.dataType)

    # now export the result value to fc (use the value property of the GPResult object from run())
    drive_times = os.path.join(folder, 'drive_times.shp')
    restapi.exportFeatureSet(drive_times, gp_res.value)

