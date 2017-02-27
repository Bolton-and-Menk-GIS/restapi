import restapi
import os
from bmi import clean_filename, SR
sr = SR('cass').factoryCode
print restapi.__file__

url = 'http://cassweb3.co.cass.mn.us/arcgis/rest/services/Basic_Layers2/MapServer'
ms = restapi.MapService(url)
out_path = r'C:\Users\calebma\Desktop\New_Shapefile\data'
boun = r'C:\Users\calebma\Desktop\New_Shapefile\New_Shapefile.shp'

lnames = [
          'Township', 'Sections', "Road ROW's",
          'Plats', 'Lakes', 'Rivers & Streams']
lnames = ['Parcels']

for n in lnames:
    print n
    lyr = ms.layer(n)
    out = os.path.join(out_path, clean_filename(n) + '.shp')
    lyr.clip(boun, out, out_sr=sr)
