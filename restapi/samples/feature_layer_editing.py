import env
import os
import restapi
import random
import time

def create_random_coordinates(xmin=-89000000, xmax=-9000000, ymin=4000000, ymax=4200000):
    return {
      "spatialReference": {
        "latestWkid": 3857,
        "wkid": 102100
      },
        "x": random.randint(xmin, xmax),
        "y": random.randint(ymin, ymax)
    }

# connect to esri's Charlotte Hazards Sample Feature Layer for incidents
url = 'https://services.arcgis.com/V6ZHFr6zdgNZuVG0/ArcGIS/rest/services/Hazards_Uptown_Charlotte/FeatureServer/0'

# instantiate a FeatureLayer
hazards = restapi.FeatureLayer(url)

# # create a new feature as json
# feature = {
#   "attributes" : { 
#     "HazardType" : "Road Not Passable", 
#     "Description" : "restapi test", 
#     "SpecialInstructions" : "Contact Dispatch", 
#     "Priority" : "High",
#     "Status": "Active"
#   }, 
#   "geometry": create_random_coordinates()
# }

# # add json feature
# adds = hazards.addFeatures([ feature ])
# print(adds)

# # add attachment to new feature using the OBJECTID
# oid = adds.addResults[0].objectId
# image = os.path.join(os.path.abspath('...'), 'docs', 'images', 'geometry-helper.png')
# attRes = hazards.addAttachment(oid, image)
# print(attRes)

# # now query attachments for new feature
# attachments = hazards.attachments(oid)
# print(attachments)

# # update the feature we just added
# updatePayload = [
#   { 
#     "attributes": { 
#       "OBJECTID": r.objectId, 
#       "Description": "restapi update" 
#     } 
#   } for r in adds.addResults
# ]
# updates = hazards.updateFeatures(updatePayload)
# print(updates)

# # delete feature
# deletePayload = [r.objectId for r in updates.updateResults]
# deletes = hazards.deleteFeatures(deletePayload)
# print(deletes)

# working with cursors
# add 5 new features using an insert cursor 
# using this in a "with" statement will call applyEdits on __exit__
fields = ["SHAPE@", 'HazardType', "Description", "Priority"]
# with hazards.insertCursor(fields) as irows:
with restapi.InsertCursor(hazards, fields) as irows:
    for i in list(range(1,6)):
        desc = "restapi insert cursor feature {}".format(i)
        irows.insertRow([create_random_coordinates(), "Wire Down", desc, "High"])

# we can always view the results by calling FeatureLayer.editResults which stores
# an array of edit results for each time applyEdits() is called.
print(hazards.editResults)

# now update records with updateCursor for the records we just added. Can use the 
# editResults property of the feature layer to get the oids of our added features
addedOids = ','.join(map(str, [r.objectId for r in hazards.editResults[0].addResults]))
whereClause = "{} in ({})".format(hazards.OIDFieldName, addedOids)
# with hazards.updateCursor(["Priority", "Description", "OID@"], where=whereClause) as rows:
with restapi.UpdateCursor(hazards, ["Priority", "Description", "OID@"], where=whereClause) as rows:
    for row in rows:
        if not row[2] % 2:
            # print('updating row with even id: ', row[2])
            row[0] = "Low"
            rows.updateRow(row)
        else:
            # delete odd OBJECTID rows
            # print('deleting row with odd objectid: ', row[2])
            rows.deleteRow(row)
          
       
# now delete the rest of the records we added
whereClause = "Description like 'restapi%'"
with hazards.updateCursor(["Description", "Priority", "OID@"], where=whereClause) as rows:
    for row in rows:
        rows.deleteRow(row)