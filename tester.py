import restapi
print restapi.__file__

import _ags

usr, pw = _ags.creds()
url = 'http://gis.bolton-menk.com/bmigis/rest/services'

t = restapi.rest_utils.RESTEndpoint(url, usr, pw)
