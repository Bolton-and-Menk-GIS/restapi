import env
import os
import restapi
import time

# connect to esri's San Francisco 311 Sample Feature Layer for incidents
url = 'https://sampleserver6.arcgisonline.com/arcgis/rest/services/SF311/FeatureServer/0'

lyr = restapi.FeatureLayer(url)

# create a new feature as json
feature = {
  "attributes": {
    "req_type": "Sewer Issues",
    "req_date": time.strftime('%m/%d/%Y'),
    "req_time": "15:24",
    "address": "127 Lawton St",
    "district": "4",
    "status": 1
  },
  "geometry": {
    "x": -122.464385,
    "y": 37.75760024
  }
}

# add json feature
addResults = lyr.addFeatures([ feature ])