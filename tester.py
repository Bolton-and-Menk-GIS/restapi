import restapi
print restapi.__file__
import json
import tempfile
import arcpy
import _ags
import time
import os
from datetime import datetime
import requests
import shutil
null = None
true = True
false = False

url = 'http://gis.bolton-menk.com/bmigis/rest/services'
usr, pw = _ags.creds()
##token = restapi.generate_token(url, usr, pw)
##ags = restapi.ArcServer(url)

ags_url = 'http://arcserver4.bolton-menk.com:6080/arcgis/admin/services'
gp_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/ChickenPermits/GPServer'
ms_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/Permits/MapServer'
fs_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/Permits/FeatureServer'
im_url = 'http://gis.bolton-menk.com/bmigis/rest/services/SSTP/dem_1m_ft/ImageServer'
##ms = restapi.MapService(ms_url)
##ms = restapi.MapService(ms_url)
##lyr = restapi.MapServiceLayer(ms_url + '/1')
##fts = restapi.FeatureService(fs_url)
##
##fields=['Num_Chickens',  'OBJECTID', 'Primary_Address']
##fs = lyr.query(fields=fields)

rep_url = 'https://gis.bolton-menk.com/bmigis/rest/directories/arcgisoutput/TEST/PhotAttachmentSync_MapServer/_Ags_Fs8727a336b86144379c2d56bd816e1fab.json'
r = requests.get(rep_url, verify=False).json()
rep = restapi.common_types.JsonReplica(r)
##cursor = restapi.Cursor(fs.json, fields + ['shape@'])
##rows = cursor.get_rows()
##row = rows.next()
##
##gp = restapi.GPService(gp_url)
##
##lcur = lyr.cursor(['Owner_Name', 'SHAPE@'])
##cur = cur = restapi.Cursor(fs)
##
##out = r'C:\TEMP\water_resources.gdb\fs_test3'
##flyr = fts.layer(1)
##flyr.layer_to_fc(out)
##gc = restapi.GeometryCollection(fs)
##im = restapi.ImageService(im_url)
##print ms.compatible_with_version(10.3)
##
##gs = restapi.GeometryService()
##g = gc[0]
##g2 = gs.project(g, g.spatialReference, outSR=4326)

gd = {
    "displayFieldName": "",
    "fieldAliases": {
        "OBJECTID": "OBJECTID",
        "SHAPE_Length": "SHAPE_Length",
        "SHAPE_Area": "SHAPE_Area"
    },
    "geometryType": "esriGeometryPolygon",
    "spatialReference": {
        "wkid": 26915,
        "latestWkid": 26915
    },
    "fields": [{
        "name": "OBJECTID",
        "type": "esriFieldTypeOID",
        "alias": "OBJECTID"
    }, {
        "name": "SHAPE_Length",
        "type": "esriFieldTypeDouble",
        "alias": "SHAPE_Length"
    }, {
        "name": "SHAPE_Area",
        "type": "esriFieldTypeDouble",
        "alias": "SHAPE_Area"
    }],
    "features": [{
        "attributes": {
            "OBJECTID": 1,
            "SHAPE_Length": 1025.350653028964,
            "SHAPE_Area": 65637.154262839947
        },
        "geometry": {
            "rings": [
                [
                    [497210.14209999982, 4967322.7083000001],
                    [497459.16550000012, 4967337.2524999995],
                    [497488.79050000012, 4967081.7770000007],
                    [497223.35140000004, 4967070.0364999995],
                    [497210.14209999982, 4967322.7083000001]
                ]
            ]
        }
    }]
}
geometries = {
    "geometries": [{
        "rings": [
            [
                [497488.79050000012, 4967081.7770000007],
                [497223.35140000004, 4967070.0364999995],
                [497210.14209999982, 4967322.7083000001],
                [497459.16550000012, 4967337.2524999995],
                [497488.79050000012, 4967081.7770000007]
            ]
        ]
    }]
}

pt_fs = {
    "displayFieldName": "",
    "fieldAliases": {
        "OBJECTID": "OBJECTID",
        "Source": "Source"
    },
    "geometryType": "esriGeometryPoint",
    "spatialReference": {
        "wkid": 102100,
        "latestWkid": 3857
    },
    "fields": [{
        "name": "OBJECTID",
        "type": "esriFieldTypeOID",
        "alias": "OBJECTID"
    }, {
        "name": "Source",
        "type": "esriFieldTypeString",
        "alias": "Source",
        "length": 4
    }],
    "features": [{
        "attributes": {
            "OBJECTID": 1,
            "Source": null
        },
        "geometry": {
            "x": -10356070.168500001,
            "y": 5598088.307099998
        }
    }]
}

pts_json = {
    "displayFieldName": "",
    "fieldAliases": {
        "OBJECTID": "OBJECTID",
        "Source": "Source"
    },
    "geometryType": "esriGeometryPoint",
    "spatialReference": {
        "wkid": 102100,
        "latestWkid": 3857
    },
    "fields": [{
        "name": "OBJECTID",
        "type": "esriFieldTypeOID",
        "alias": "OBJECTID"
    }, {
        "name": "Source",
        "type": "esriFieldTypeString",
        "alias": "Source",
        "length": 4
    }],
    "features": [{
        "attributes": {
            "OBJECTID": 1,
            "Source": null
        },
        "geometry": {
            "x": -10356082.612,
            "y": 5599101.4914999977
        }
    }, {
        "attributes": {
            "OBJECTID": 2,
            "Source": null
        },
        "geometry": {
            "x": -10356428.8583,
            "y": 5599008.4096999988
        }
    }, {
        "attributes": {
            "OBJECTID": 3,
            "Source": null
        },
        "geometry": {
            "x": -10356507.791099999,
            "y": 5599311.6425999999
        }
    }, {
        "attributes": {
            "OBJECTID": 4,
            "Source": null
        },
        "geometry": {
            "x": -10356148.273800001,
            "y": 5599194.0146000013
        }
    }, {
        "attributes": {
            "OBJECTID": 5,
            "Source": null
        },
        "geometry": {
            "x": -10356339.975400001,
            "y": 5598693.1138999984
        }
    }]
}

##gc2 = restapi.GeometryCollection(geometries)
##e = im.pointIdentify(pt_fs)
##im.clip(gd, r'C:\TEMP\new_rst_dem.tif')
fs3 = restapi.FeatureSet(pts_json)

gs = restapi.GeometryService('http://gis.bolton-menk.com/bmigis/rest/services/Utilities/Geometry/GeometryServer')
buffers = gs.buffer(fs3, 3857, 100)
##pros = gs.project(fs3, 3857, 26915)
##gc3 = restapi.GeometryCollection(geometries)
##test = r'C:\TEMP\testing.gdb\rest_buffers3'
##restapi.exportGeometryCollection(buffers, test)
##@restapi.common_types.geometry_passthrough
##def test(geom):
##    """this is a test docstring"""
##    return geom
##
##gc_dec = test(geometries)
##
# feature editing tests
##
##    
##can_url = 'http://gis.bolton-menk.com/arcgis/rest/services/CANB/Canb_Editor_REST/FeatureServer/0'
##relatedID = 1277
##can = restapi.FeatureLayer(can_url, usr, pw)
##rr = can.query_related_records(1277, 0)
##ft =  rr.get_related_records(1277)[0]
##
##
##print_gp = restapi.GPService('http://gis.bolton-menk.com/bmigis/rest/services/Utilities/PrintingTools/GPServer/Export%20Web%20Map%20Task')
##with open(r'\\ArcServer1\GIS\MPWD\_Basemap\ESRI\Scripts\Toolbox\mpwd_lib\bin\maplewood_webmap.json', 'r') as f:
##    webmap = json.load(f)
##
##print_gp.run(Web_Map_as_JSON=webmap, Format='PNG32', Layout_Template=r'\\ArcServer1\GIS\MPWD\_Basemap\ESRI\Scripts\Toolbox\mpwd_lib\bin\template2.mxd', r'C:\Users\calebma\Desktop\map_from_rest.png')
##
##ext = restapi.getFeatureExtent(buffers)
##ext2 = restapi.getFeatureExtent(geometries)
##
##ags = restapi.admin.ArcServerAdmin(ags_url, usr, pw)
##
##print ext
##env = restapi.Geometry(ext)
##url = 'https://maps.co.ramsey.mn.us/arcgis/rest/services/MapRamsey/MapRamseyOperationalLayersAll/MapServer/33'
##url = 'http://gis.bolton-menk.com/bmigis/rest/services/METC/METC_Intersection_Study_Webmap/MapServer/20'
##url = 'http://gis.bolton-menk.com/bmigis/rest/services/BMI/BMI_Photo_Logger/FeatureServer/0'
##lyr = restapi.MapServiceLayer(url, usr, pw)
##oids = lyr.getOIDs()
##att = lyr.attachments(oids[0])[0]
##print att.url
##import urllib
##with open(r'C:\Users\calebma\Desktop\test.jgp', 'wb') as f:
##    f.write(urllib.urlopen(att.url).read())
##print 'done'
##ms_lyr = restapi.MapServiceLayer(url)
##pars = r'C:\TEMP\testing.gdb\photos2'
##pars = r'C:\TEMP\test.gdb\ram_pars'
##lyr.layer_to_fc(pars, records=3000, exceed_limit=True, include_domains=True)
##fs = ms_lyr.query(exceed_limit=True, records=15000)
##print 'shape ', fs.SHAPE
##restapi.exportFeatureSet(fs, pars)
##print 'starting'
##st = datetime.now()
##lyr.layer_to_fc(pars, exceed_limit=True, records=20000)
##print 'done, elapsed: {}'.format(datetime.now() - st)


##tmpd = tempfile.mkdtemp()
##
##tmp = os.path.join(tmpd, 'tmp_{}.json'.format(time.strftime('%Y%m%d%H%M%S')))
##tmp = tempfile.NamedTemporaryFile(suffix='.json', prefix='restapi_')
##print tmp.name
####fs.dump(tmp)
####print tmp
##st = datetime.now()
##arcpy.conversion.JSONToFeatures(tmp, pars)
##print 'done: {}'.format(datetime.now() - st)
##shutil.rmtree(tmpd)
