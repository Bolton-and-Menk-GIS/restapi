import os
import sys
parentDir = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

if parentDir not in sys.path:
    sys.path.append(parentDir)

test_data_folder = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'testData')
if not os.path.exists(test_data_folder):
    os.makedirs(test_data_folder)


def delete_shapefile(shp):
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
