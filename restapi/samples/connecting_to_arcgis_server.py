import os
import env

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
for root, services in ags.walk():
    print('Folder: {}'.format(root))
    print('Services: {}\n'.format(services))


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