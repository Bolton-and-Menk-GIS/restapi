import os
from env import test_data_folder
import restapi

url = 'https://services.arcgis.com/V6ZHFr6zdgNZuVG0/arcgis/rest/services/Hazards_Uptown_Charlotte/FeatureServer/0'

# create FeatureLayer
hazards = restapi.FeatureLayer(url)

# QUERY EXAMPLES

# # query all features, to fetch all regardless of `maxRecordCount` 
# # use `exceed_limit=true` keyword arg
# fs = hazards.query()
# print('All Hazards Count: {}'.format(fs.count))

# # query features that are "High" Priority
# high_priority = hazards.query(where="Priority = 'High'")
# print('High Priority Hazards count: {}'.format(high_priority.count))

# download features - choosing a geodatbase output will bring over domain 
# info (when you have access to arcpy), whereas a shapefile output will 
# just bring over the domain values
shp = os.path.join(test_data_folder, 'hazards2.shp')

# delete if exists
if os.path.exists(shp):
    try:
        os.remove(shp)
        for ext in ['.shx', '.dbf', '.prj']:
            otherFile = shp.replace('.shp', ext)
            if os.path.exists(otherFile):
                os.remove(otherFile)
    except:
      pass
    
# export in WGS 1984 projection
hazards.export_layer(shp, outSR=4326)

