import os
from env import test_data_folder, delete_shapefile
import restapi
import random

def create_random_coordinates(xmin=-89000000, xmax=-9000000, ymin=4000000, ymax=4200000):
    return {
        "x": random.randint(xmin, xmax),
        "y": random.randint(ymin, ymax)
    }

url = 'https://services.arcgis.com/V6ZHFr6zdgNZuVG0/arcgis/rest/services/Hazards_Uptown_Charlotte/FeatureServer/0'

# create FeatureLayer
hazards = restapi.FeatureLayer(url)

# QUERY EXAMPLES

# query all features, to fetch all regardless of `maxRecordCount` 
# use `exceed_limit=true` keyword arg
fs = hazards.query()
print('All Hazards Count: {}'.format(fs.count))

# query features that are "High" Priority
high_priority = hazards.query(where="Priority = 'High'")
print('High Priority Hazards count: {}'.format(high_priority.count))

# download features - choosing a geodatbase output will bring over domain 
# info (when you have access to arcpy), whereas a shapefile output will 
# just bring over the domain values
shp = os.path.join(test_data_folder, 'hazards.shp')
delete_shapefile(shp)
    
# export layer to shapefile in WGS 1984 projection
hazards.export_layer(shp, outSR=4326)

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

# add 3 new features using an insert cursor 
# using this in a "with" statement will call applyEdits on __exit__
fields = ["SHAPE@", 'HazardType', "Description", "Priority"]
with hazards.insertCursor(fields) as irows:
    for i in range(3):
        irows.insertRow([create_random_coordinates(), "Wire Down", desc, "High"])

# now update records with updateCursor
whereClause = "Description = '{}'".format(desc)

with hazards.updateCursor(["Priority", "OID@"], where=whereClause) as rows:
    for row in rows:
        row[0] = "Low"
        rows.updateRow(row)

# now delete the records we added
hazards.deleteFeatures(where=whereClause)




