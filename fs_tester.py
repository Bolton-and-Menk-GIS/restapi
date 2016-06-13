import restapi
import os
import urllib

print restapi.__file__

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
atts = incidents.addAttachment(oid, tmp)
os.remove(tmp)

# get attachment info from service and download it
attachments = incidents.attachments(oid)

for attachment in attachments:
    print attachment
    print attachment.contentType, attachment.size
##    attachment.download(folder) # download attachment into restapi_test_data folder on Desktop

# update the feature we just added
adds[0]['attributes']['address'] = 'Address Not Available'
adds[0]['attributes']['objectid'] = oid
updated = incidents.updateFeatures(adds)

# now delete feature
deleted = incidents.deleteFeatures(oid)
