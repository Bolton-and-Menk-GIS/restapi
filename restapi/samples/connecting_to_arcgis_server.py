import os
import env

# disable certificate verification and arcpy (use open source)
env_flags = ['RESTAPI_VERIFY_CERT','RESTAPI_USE_ARCPY', ]
for flag in env_flags:
    os.environ[flag] = 'FALSE'

# now we can import restapi with these presets
import restapi

# connect to esri's sample server 6
rest_url = 'https://sampleserver6.arcgisonline.com/arcgis/rest/services'

# connect to restapi.ArcServer instance 
# no authentication is required, so no username and password are supplied
ags = restapi.ArcServer(rest_url)

# get folder and service properties
print('Number of folders: {}'.format(len(ags.folders)))
print('Number of services: {}\n'.format(len(ags.services)))

# walk thru directories
# for root, services in ags.walk():
#     print('Folder: "{}"'.format(root))
#     print('Services: {}\n'.format(services))


# connect to a specific service
# using just the service name (at the root)
usa = ags.getService('USA') #/USA/MapServer -> restapi.common_types.MapService

# using the relative path to a service in a folder
census = ags.getService('AGP/Census') #/AGP/Census/MapServer -> restapi.common_types.MapService

# can also just use the service name, but be weary of possible duplicates
infastructure = ags.getService('Infrastructure') #/Energy/Infrastructure/FeatureServer -> restapi.common_types.FeatureService

# using a wildcard search
covid_cases = ags.getService('*Covid19Cases*') #/NYTimes_Covid19Cases_USCounties/MapServer -> restapi.common_types.MapService

for service in [usa, census, infastructure, covid_cases]:
    print('name: "{}"'.format(service.name))
    print('repr: "{}"'.format(repr(service)))
    print('url: {}\n'.format(service.url))


# get access to the "Cities" layer from USA Map Service
cities = usa.layer('Cities') # or can use layer id: usa.layer(0)

# query the map layer for all cities in California with population > 100000
where = "st = 'CA' and pop2000 > 100000"

# the query operation returns a restapi.FeatureSet or restapi.GeoJSONFeatureSet
featureSet = cities.query(where=where)

# get result count, can also use len(featureSet)
print('Found {} cities in California with Population > 100K'.format(featureSet.count))

# if you don't want the json/FeatureSet representation, you can use restapi cursors
# for the query which are similar to the arcpy.da cursors
cursor = cities.cursor(fields=['areaname', 'pop2000', 'SHAPE@'], where=where)
for row in cursor:
    print(row)



