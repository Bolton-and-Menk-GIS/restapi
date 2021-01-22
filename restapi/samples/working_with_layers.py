import env
import os
import restapi

# connect to esri's sample server 6
rest_url = 'https://sampleserver6.arcgisonline.com/arcgis/rest/services'

# connect to restapi.ArcServer instance 
# no authentication is required, so no username and password are supplied
ags = restapi.ArcServer(rest_url)

# connect to a specific service
# using just the service name (at the root)
usa = ags.getService('USA') #/USA/MapServer -> restapi.common_types.MapService

# get access to the "Cities" layer from USA Map Service
cities = usa.layer('Cities') # or can use layer id: usa.layer(0)

# query the map layer for all cities in California with population > 100000
where = "st = 'CA' and pop2000 > 100000"

# the query operation returns a restapi.FeatureSet or restapi.FeatureCollection depending on the return format
featureSet = cities.query(where=where)

# get result count, can also use len(featureSet)
print('Found {} cities in California with Population > 100K'.format(featureSet.count))

# print first feature (restapi.Feature).  The __str__ method is pretty printed JSON
print(featureSet[0])

# querying with exceed limit
# fetch first 1000
first1000 = cities.query()
print('count without exceed_limit: {}'.format(first1000.count)) # can also use len()

# fetch all by exceeding limit
allCities = cities.query(exceed_limit=True)
print('count with exceed_limit: {}'.format(len(allCities)))

# if you don't want the json/FeatureSet representation, you can use restapi cursors
# for the query which are similar to the arcpy.da cursors
with cities.cursor(fields=['areaname', 'pop2000', 'SHAPE@'], where=where) as cursor:
   for row in cursor:
       print(row)
       

# the above cursor() method will actually make the API query call again, 
# a cursor can also be instantiated from a restapi.Cursor by passing in a feature set:
# cursor = restapi.Cursor(featureSet, ['areaname', 'pop2000', 'SHAPE@'])

# exporting features 
out_folder = os.path.join(os.path.expanduser('~'), 'Documents', 'restapi_samples')
if not os.path.exists(out_folder):
   os.makedirs(out_folder)
shp = os.path.join(out_folder, 'CA_Cities_100K.shp')

# export layer to a shapefile
cities.export_layer(shp, where=where)

# if there is an existing feature set, you can also export that directly
# restapi.exportFeatureSet(featureSet, shp)

# export a kmz
kmz = shp.replace('.shp', '.kmz')
cities.export_kmz(kmz, where=where)

# select layer by location
universities_url = 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Colleges_and_Universities/FeatureServer/0'

# get feature layer
universities = restapi.FeatureLayer(universities_url)
print('universities: ', repr(universities))

# form geometry (do not have to cast to restapi.Geometry, this will happen under the hood automatically)
geometry = restapi.Geometry({
  "spatialReference": {
    "latestWkid": 3857,
    "wkid": 102100
  },
  "rings": [
    [
      [
        -10423340.4579098,
        5654465.8453829475
      ],
      [
        -10324889.565478457,
        5654465.8453829475
      ],
      [
        -10324889.565478457,
        5584449.527473665
      ],
      [
        -10423340.4579098,
        5584449.527473665
      ],
      [
        -10423340.4579098,
        5654465.8453829475
      ]
    ]
  ]
})

# make selection
featureCollection = universities.select_by_location(geometry)
print('Number of Universities in Twin Cities area: {}'.format(featureCollection.count))

# can also export the feature collection directly or call the clip() method (makes a new call to server)
universities_shp = os.path.join(out_folder, 'TwinCities_Univiersities.shp')
universities.clip(geometry, universities_shp)
