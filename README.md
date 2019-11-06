# restapi
This is a Python API for working with ArcGIS REST API, ArcGIS Online, and Portal/ArcGIS Enterprise.  This package has been designed to work with [arcpy](https://pro.arcgis.com/en/pro-app/arcpy/get-started/what-is-arcpy-.htm) when available, or the included open source module [pyshp](https://pypi.org/project/pyshp/).  It will try to use arcpy if available for some data conversions, otherwise will use open source options. Also included is a subpackage for administering ArcGIS Server Sites.  This is updated often, so continue checking here for new functionality!

### Why would you use this package?
Esri currently provides the [ArcGIS API for Python](https://developers.arcgis.com/python/) which provides complete bindings to the ArcGIS REST API.  This package has less coverage of the REST API, but has many convience functions not available in the ArcGIS API for Python.  This package will also support older versions of Python (i.e. 2.7.x) whereas Esri's package only supports 3.x.


## Installation
`restapi` is supported on Python 2.7 and 3.x. It can be found on [Github](https://github.com/Bolton-and-Menk-GIS/restapi) and [PyPi](https://pypi.org/project/bmi-arcgis-restapi/). To install using pip:  
````py
pip install bmi-arcgis-restapi
````

After installation, it should be available to use in Python:  
````py
import restapi
````

## A note about `arcpy`
By default, `restapi` will import Esri's `arcpy` module if available. However, this module is not required to use this package.  `arcpy` is only used when available to write data to disk in esri specific formats (file geodatabase, etc) and working with `arcpy` Geometries.  When `arcpy` is not availalbe, the  [pyshp](https://pypi.org/project/pyshp/) module is used to write data (shapefile format only) and work with `shapefile.Shape` objects (geometry).  Also worth noting is that open source version is much faster than using `arcpy`.

That being said, there may be times when you want to force `restapi` to use the open source version, even when you have access to `arcpy`.  Some example scenarios being when you don't need to write any data in an Esri specific format, you want the script to execute very fast, or you are working in an environment where `arcpy` may not play very nicely ([Flask](https://palletsprojects.com/p/flask/), [Django](https://www.djangoproject.com/), etc.).  To force `restapi` to use the open source version, you can simply create an environment variable called `RESTAPI_USE_ARCPY` and set it to `FALSE` or `0`.  This variable will be checked before attempting to import `arcpy`.

Here is an example on how to force open source at runtime:

```py
import os
os.environ['RESTAPI_USE_ARCPY'] = 'FALSE'

# now import restapi
import restapi
```




## Connecting to an ArcGIS Server
One of the first things you might do is to connect to a services directory (or catalog):


Connect to external services

````py
# connect NOAA ArcGIS Server Instance
rest_url = 'https://gis.ngdc.noaa.gov/arcgis/rest/services'

# no authentication is required, so no username and password are supplied
ags = restapi.ArcServer(rest_url)

# get folder and service properties
print('Number of folders: {}'.format(len(ags.folders)))
print('Number of services: {}'.format(len(ags.services)))

# walk thru directories
for root, folders, services in ags.walk():
    print(root)
    print(folders)
    print(services)
    print('\n')
````

Connecting to a map service from within the ArcServer object
````py
# access "ahps_gauges" service (stream gauges)
gauges = ags.getService('ahps_gauges')
print(gauges.url) #print(MapService url

# print(layer names
print(gauges.list_layers())

# access "observed river stages" layer
lyr = gauges.layer('observed_river_stages') #not case sensitive, also supports wildcard search (*)

# list fields from col layer
print(lyr.list_fields())
````

You can also query the layer and get back arcpy.da Cursor like access

````py
# run search cursor for gauges in California
# (maximimum limit may be 1000 records, can use get_all=True to exceed transfer limit)
# can filter fields by putting a field list, can use actual shape field name to get
#  geometry or use the ArcGIS-like token "SHAPE@"
# all fields are gathered by the default ("*") and fields can be filtered by providing a list
query = "state = 'CA'"
for row in lyr.cursor(where=query, fields=['SHAPE@', u'gaugelid', u'status', u'location']):
    print(row)

# Note: can also do this from the MapService level like this:
# cursor = gauges.cursor('observed_river_stages', where=query)
````

The layer can also be exported to a shapefile or KMZ

````py
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
````

Clipping a layer is also easy

````py
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
````

You can also connect to a MapService directly

```py
url = 'http://gis.srh.noaa.gov/arcgis/rest/services/ahps_gauges/MapServer'
gauges = restapi.MapService(url)
```

Working with Feature Layers
---------------

### query examples
```py

# create FeatureLayer
url = 'https://services.arcgis.com/V6ZHFr6zdgNZuVG0/arcgis/rest/services/Hazards_Uptown_Charlotte/FeatureServer/0'
hazards = restapi.FeatureLayer(url)

# QUERY EXAMPLES

# query all features, to fetch all regardless of `maxRecordCount` 
# use `exceed_limit=true` keyword arg
fs = hazards.query()
print('All Hazards Count: {}'.format(fs.count))

# query features that are "High" Priority
high_priority = hazards.query(where="Priority = 'High'")
print('High Priority Hazards count: {}'.format(high_priority.count))
```

### download features
```py
# download features - choosing a geodatbase output will bring over domain 
# info (when you have access to arcpy), whereas a shapefile output will 
# just bring over the domain values
shp = os.path.join(test_data_folder, 'hazards.shp')
    
# export layer to shapefile in WGS 1984 projection
hazards.export_layer(shp, outSR=4326)
```

## feature editing

### add features using `FeatureLayer.addFeatures()`
```py
# add new records via FeatureLayer.addFeatures()
desc = "restapi edit test"
new_ft = {
    "attributes": {
        "HazardType": "Flooding",
        "Description": desc,
        "SpecialInstructions": None,
        "Status": "Active",
        "GlobalID": "416f04e5-0ae9-4444-8d0c-d4e9b44e7f87",
        "Priority": "Moderate"
    },
    "geometry": create_random_coordinates()
}

# add new feature
results = hazards.addFeatures([new_ft])
print(results)
```
### using `restapi` cursors

`restapi` also supports cursors similar to what you get when using `arcpy`.  However, these work directly with the REST API and JSON features while also supporting `arcpy` and `shapefile` geometry types.  See the below example on how to use an `insertCursor` to add new records:

```py
# add 3 new features using an insert cursor 
# using this in a "with" statement will call applyEdits on __exit__
fields = ["SHAPE@", 'HazardType', "Description", "Priority"]
with hazards.insertCursor(fields) as irows:
    for i in range(3):
        irows.insertRow([create_random_coordinates(), "Wire Down", desc, "High"])
```

records can be updated with an `updateCursor` and a where clause.  Note that the `OBJECTID` field must be included in the query to indicate which records will be updated.  The `OID@` field token can be used to retreive the `objectIdFieldName`:

```py
# now update records with updateCursor
whereClause = "Description = '{}'".format(desc)

with hazards.updateCursor(["Priority", "OID@"], where=whereClause) as rows:
    for row in rows:
        row[0] = "Low"
        rows.updateRow(row)
```

Deleting features can be done with a simple `where` clause:

```py
# now delete the records we added
hazards.deleteFeatures(where=whereClause)
```

We can also add attachments
````py
# add attachment, get new OID from add results
oid = result.addResults[0]  # must get an OID to add attachment to

# download python image online and add it to the featuer we just added above
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
    print(attachment)
    print attachment.contentType, attachment.size)
    attachment.download(folder) # folder is a user specified output directory
````
Update feature and delete features

````py
# update the feature we just added
adds[0]['attributes']['address'] = 'Address Not Available'
adds[0]['attributes']['objectid'] = oid
incidents.updateFeatures(adds)

# now delete feature
incidents.deleteFeatures(oid)
````

Offline capabilities (Sync)

````py
# if sync were enabled, we could create a replica like this:
# can pass in layer ID (0) or name ('incidents', not case sensative)
replica = fs.createReplica(0, 'test_replica', geometry=adds[0]['geometry'], geometryType='esriGeometryPoint', inSR=4326)

# now export the replica object to file geodatabase (if arcpy access) or shapefile with hyperlinks (if open source)
restapi.exportReplica(replica, folder)
````

Working with Image Services
---------------------------

````py
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
print(elevation)
````

Geocoding
---------

````py
# hennepin county, MN geocoder
henn = 'http://gis.hennepin.us/arcgis/rest/services/Locators/HC_COMPOSITE/GeocodeServer'
geocoder = restapi.Geocoder(henn)
# find target field, use the SingleLine address field by default
geoResult = geocoder.findAddressCandidates('353 N 5th St, Minneapolis, MN 55403')

# export results to shapefile
print('found {} candidates'.format(len(geoResult))
geocoder.exportResults(geoResult, os.path.join(folder, 'target_field.shp'))

# Esri geocoder
esri_url = 'http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Locators/ESRI_Geocode_USA/GeocodeServer'
esri_geocoder = restapi.Geocoder(esri_url)

# find candidates using key word arguments (**kwargs) to fill in locator fields, no single line option
candidates = esri_geocoder.findAddressCandidates(Address='380 New York Street', City='Redlands', State='CA', Zip='92373')
print('Number of address candidates: {}'.format(len(candidates)))
for candidate in candidates:
    print(candidate.location)

# export results to shapefile
out_shp = os.path.join(folder, 'Esri_headquarters.shp')
geocoder.exportResults(candidates, out_shp)
````

Geoprocessing Services
----------------------

````py
# test esri's drive time analysis GP Task
gp_url = 'http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Network/ESRI_DriveTime_US/GPServer/CreateDriveTimePolygons'
gp = restapi.GPTask(gp_url)

# get a list of gp parameters (so we know what to pass in as kwargs)
print('\nGP Task "{}" parameters:\n'.format(gp.name)
for p in gp.parameters:
    print('\t', p.name, p.dataType)

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
    print('\nOutput Result: "{}", data type: {}\n'.format(result.paramName, result.dataType))

    # now export the result value to fc (use the value property of the GPResult object from run())
    drive_times = os.path.join(folder, 'drive_times.shp')
    restapi.exportFeatureSet(drive_times, gp_res.value)
````

A note about input Geometries
-----------------------------

restapi will try to use arcpy first if you have it, otherwise will defer to open source.  Both
support the reading of shapefiles to return the first feature back as a restapi.Geometry object

It also supports arcpy Geometries and shapefile.Shape() objects
````py
>>> shp = r'C:\TEMP\Polygons.shp' # a shapefile on disk somewhere
>>> geom = restapi.Geometry(shp)
>>> print(geom.envelope())
-121.5,38.3000000007,-121.199999999,38.6000000015
````

Token Based Security
--------------------

restapi also supports secured services.  This is also session based, so if you sign in once to an
ArcGIS Server Resource (on the same ArcGIS Site), the token will automatically persist via the 
IdentityManager().

There are 3 ways to authticate:

````py
# **kwargs for all accessing all ArcGIS resources are
# usr   -- username
# pw    -- password
# token -- token (as string or restapi.Token object)
# proxy -- url to proxy

# secured url
secured_url = 'http://some-domain.com/arcgis/rest/services'

# 1. username and password
ags = restapi.ArcServer(url, 'username', 'password')  # token is generated and persists

# 2. a token that has already been requested
ags = restapi.ArcServer(url, token=token)  # uses a token that is already active

# 3. via a proxy (assuming using the standard esri proxy)
#   this will forward all subsequent requests through the proxy
ags = restapi.ArcServer(url, proxy='http://some-domain.com/proxy.ashx')
````

You can even just generate a token and let the IdentityManager handle the rest.  It is even smart enough to handle multiple tokens for different sites:

```py
# login to instance 1
usr = 'username'
pw = 'password'

# urls to two different ArcGIS Server sites
url_1 = 'http://some-domain.com/arcserver1/rest/services'
url_2 = 'http://domain2.com/arcgis/rest/services'

# generate tokens
tok1 = restapi.generate_token(url_1, usr, pw)
tok2 = restapi.generate_token(url_2, usr, pw)

# now we should be able to access both ArcGIS Server sites via the IdentityManager
arcserver1 = restapi.ArcServer(url_1) # tok1 is automatically passed in and handled
arcserver2 = restapi.ArcServer(url_2) # tok2 is used here
```

The admin Subpackage
--------------------

restapi also contains an administrative subpackage (warning: most functionality has not been tested!).  You can import this module like this:

```py
from restapi import admin
```


### Connecting to a Portal
```py
url = 'https://domain.gis.com/portal/home'
portal = restapi.admin.Portal(url, 'username', 'password')

# get servers
servers = portal.servers

# stop sample cities service
server = servers[0]

service = server.service('SampleWorldCities.MapServer')
service.stop()

```

To connect to an ArcGIS Server instance that you would like to administer you can do the following:

```py
# test with your own servers
url = 'localhost:6080/arcgis/admin/services' #server url
usr = 'username'
pw = 'password'

# connect to ArcGIS Server instance
arcserver = admin.ArcServerAdmin(url, usr, pw)
```

To list services within a folder, you can do this:

```py
folder = arcserver.folder('SomeFolder')  # supply name of folder as argument
for service in folder.iter_services():
    print(service.serviceName, service.configuredState

    # can stop a service like this
    # service.stop()

    # or start like this
    # service.start()

print('\n' * 3)

# show all services and configured state (use iter_services to return restapi.admin.Service() object!)
for service in arcserver.iter_services():
    print(service.serviceName, service.configuredState)
```
Security
--------

You can set security at the folder or service level.  By default, the addPermssion() method used by Folder and Service objects will make the service unavailable to the general public and only those in the administrator role can view the services.  This is done by setting the 'esriEveryone' principal "isAllowed" value to false.  You can also assign permissions based on roles.

```py
arcserver.addPermission('SomeFolder')  # by default it will make private True 

# now make it publically avaiable (unsecure)
arcserver.addPermission('SomeFolder', private=False)

# secure based on role, in this case will not allow assessor group to see utility data
#   assessor is name of assessor group role, Watermain is folder to secure
arcserver.addPermission('Watermain', 'assessor', False)  

# note, this can also be done at the folder level:
folder = arcserver.folder('Watermain')
folder.addPermission('assessor', False)
```

Stopping and Starting Services
------------------------------

Services can easily be started and stopped with this module.  This can be done from the ArcServerAdmin() or Folder() object:

```py
# stop all services in a folder
arcserver.stopServices(folderName='SomeFolder') # this can take a few minutes

# look thru the folder to check the configured states, should be stopped
for service in arcserver.folder('SomeFolder').iter_services():
    print(service.serviceName, service.configuredState)

# now restart
arcserver.startServices(folderName='SomeFolder') # this can take a few minutes

# look thru folder, services should be started
for service in arcserver.folder('SomeFolder').iter_services():
    print(service.serviceName, service.configuredState)
    
# to do this from a folder, simply get a folder object back
folder = arcserver.folder('SomeFolder')
folder.stopServices()
for service in folder.iter_services():
    print(service.serviceName, service.configuredState)
```

Updating Service Properties
---------------------------

The admin package can be used to update the service definitions via JSON.  By default, the Service.edit() method will pass in the original service definition as JSON so no changes are made if no arguments are supplied.  The first argument is the service config as JSON, but this method also supports keyword arguments to update single properties (**kwargs).  These represent keys of a the dictionary in Python.

```py
# connect to an individual service (by wildcard) - do not need to include full name, just
# enough of the name to make it a unique name query
service = arcserver.service('SampleWorldCities') #provide name of service here

# get original service description
description = service.description

# now edit the description only by using description kwarg (must match key exactly to update)
service.edit(description='This is an updated service description')

# edit description again to set it back to the original description
service.edit(description=description)
```

There are also some helper methods that aren't available out of the box from the ArcGIS REST API such as enabling or disabling extensions:

```py
# disable Feature Access and kml downloads
service.disableExtensions(['FeatureServer', 'KmlServer'])

# you can also list enabled/disabled services
print(service.enabledExtensions)
# [u'KmlServer', u'WFSServer', u'FeatureServer']

service.disabledExtensions
# [u'NAServer', u'MobileServer', u'SchematicsServer', u'WCSServer', u'WMSServer']

# Edit service extension properites
# get an extension and view its properties
fs_extension = service.getExtension('FeatureServer')

print(fs_extension) # will print as pretty json
```

For Service objects, all properties are represented as pretty json.  Below is what the FeatureService Extension looks like:

```py
{
  "allowedUploadFileTypes": "", 
  "capabilities": "Query,Create,Update,Delete,Uploads,Editing", 
  "enabled": "true", 
  "maxUploadFileSize": 0, 
  "properties": {
    "allowGeometryUpdates": "true", 
    "allowOthersToDelete": "false", 
    "allowOthersToQuery": "true", 
    "allowOthersToUpdate": "false", 
    "allowTrueCurvesUpdates": "false", 
    "creatorPresent": "false", 
    "dataInGdb": "true", 
    "datasetInspected": "true", 
    "editorTrackingRespectsDayLightSavingTime": "false", 
    "editorTrackingTimeInUTC": "true", 
    "editorTrackingTimeZoneID": "UTC", 
    "enableOwnershipBasedAccessControl": "false", 
    "enableZDefaults": "false", 
    "maxRecordCount": "1000", 
    "realm": "", 
    "syncEnabled": "false", 
    "syncVersionCreationRule": "versionPerDownloadedMap", 
    "versionedData": "false", 
    "xssPreventionEnabled": "true", 
    "zDefaultValue": "0"
  }, 
  "typeName": "FeatureServer"
}
```

Setting properties for extensions is also easy:

```py
# set properties for an extension using helper method, use **kwargs for setting capabilities
service.setExtensionProperties('FeatureServer', capabilities='Query,Update,Delete,Editing')

# verify changes were made
print(fs_extension.capabilities
# 'Query,Update,Delete,Editing'

# alternatively, you can edit the service json directly and call the edit method
# change it back to original settings
fs_extension.capabilities = 'Query,Create,Update,Delete,Uploads,Editing'
service.edit()

# verify one more time...
print(fs_extension.capabilities)
# 'Query,Create,Update,Delete,Uploads,Editing'
```

Access the Data Store
---------------------

You can iterate through the data store items easily to read/update/add items:

```py
# connect to the server's data store
ds = arcserver.dataStore

# iterate through all items of data store
for item in ds:
    print(item.type, item.path
    # if it is an enterprise database connection, you can get the connection string like this
    if item.type == 'egdb':
        print(item.info.connectionString)
    # else if a folder, print(server path
    elif item.type == 'folder':
        print(item.info.path)
    print('\n')
```

User and Role Stores
--------------------

When viewing usernames/roles you can limit the number of names returned using the "maxCount" keyword argument.  To view and make changes to Role Store:

```py
# connect to role store
rs = arcserver.roleStore

# print roles
for role in rs:
    print(role)

# find users within roles
for role in rs:
    print(role, 'Users: ', rs.getUsersWithinRole(role))

# add a user to role
rs.addUsersToRole('Administrators', 'your-domain\\someuser')

# remove user from role
rs.removeUsersFromRole('Administrators', 'your-domain\\someuser')

# remove an entire role
rs.removeRole('transportation')
```

To view and make changes to the User Store:

```py
# connect to user store
us = arcserver.userStore

# get number of users
print(len(us)

# iterate through first 10 users
for user in us.searchUsers(maxCount=10):
    print(user)
    
# add new user
us.addUser('your-domain\\someuser', 'password')

# assign roles by using comma separated list of role names
us.assignRoles('your-domain\\someuser', 'Administrators,Publishers')

# get privileges from user
us.getPrivilegeForUser('your-domain\\someuser')

# remove roles from user 
us.removeRoles('your-domain\\someuser', 'Administrators,Publishers')
```

Log Files
---------

You can easily query server log files like this:

```py
import restapi
import datetime

# query log files (within last 3 days), need to convert to milliseconds
threeDaysAgo = restapi.date_to_mil(datetime.datetime.now() - relativedelta(days=3))
for log in arcserver.queryLogs(endTime=threeDaysAgo, pageSize=25):
    print(log.time
    for message in log:
        print(message)
    print('\n')
```

A note about verbosity
----------------------

When using the admin subpackage you will likely be making changes to services/permissions etc.  On operations that change a configuration, the @passthrough decorator will report back if the operation is successful and return results like this:

```py
{u'status': u'SUCCESS'}
```

The printing of these messages can be shut off by changing the global "VERBOSE" variable so these messages are not reported.  This can be disabled like this:

```py
admin.VERBOSE = False 
```
