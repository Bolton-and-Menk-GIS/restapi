import restapi
print restapi.__file__

import _ags

url = 'http://gis.bolton-menk.com/bmigis/rest/services'
usr, pw = _ags.creds()
##token = restapi.generate_token(url, usr, pw)
##ags = restapi.ArcServer(url)

gp_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/ChickenPermits/GPServer'
ms_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/Permits/MapServer'
fs_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/Permits/FeatureServer'
ms = restapi.rest_utils.BaseMapService(ms_url)
##ms = restapi.MapService(ms_url)
lyr = restapi.rest_utils.BaseMapServiceLayer(ms_url + '/1')
fts = restapi.FeatureService(fs_url)

fields=['Num_Chickens',  'OBJECTID', 'Primary_Address']
fs = lyr.query(fields=fields)
print fs.fields
cursor = restapi.Cursor(fs.json, fields + ['shape@'])
rows = cursor.get_rows()
row = rows.next()

gp = restapi.rest_utils.GPService(gp_url)


