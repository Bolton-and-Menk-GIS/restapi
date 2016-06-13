import restapi
print restapi.__file__

import _ags
null = None
true = True
false = False

url = 'http://gis.bolton-menk.com/bmigis/rest/services'
usr, pw = _ags.creds()
##token = restapi.generate_token(url, usr, pw)
##ags = restapi.ArcServer(url)

gp_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/ChickenPermits/GPServer'
ms_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/Permits/MapServer'
fs_url = 'http://gis.bolton-menk.com/bmigis/rest/services/MPWD/Permits/FeatureServer'
im_url = 'http://gis.bolton-menk.com/bmigis/rest/services/SSTP/dem_1m_ft/ImageServer'
ms = restapi.MapService(ms_url)
##ms = restapi.MapService(ms_url)
lyr = restapi.MapServiceLayer(ms_url + '/1')
fts = restapi.FeatureService(fs_url)

fields=['Num_Chickens',  'OBJECTID', 'Primary_Address']
fs = lyr.query(fields=fields)
cursor = restapi.Cursor(fs.json, fields + ['shape@'])
rows = cursor.get_rows()
row = rows.next()

gp = restapi.GPService(gp_url)

lcur = lyr.cursor(['Owner_Name', 'SHAPE@'])
cur = cur = restapi.Cursor(fs)

##out = r'C:\TEMP\water_resources.gdb\fs_test3'
flyr = fts.layer(1)
##flyr.layer_to_fc(out)
gc = restapi.GeometryCollection(fs)
##im = restapi.ImageService(im_url)
##print ms.compatible_with_version(10.3)

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

gc2 = restapi.GeometryCollection(geometries)
##e = im.pointIdentify(pt_fs)
##im.clip(gd, r'C:\TEMP\new_rst_dem.tif')
fs3 = restapi.FeatureSet(pts_json)

gs = restapi.GeometryService('http://gis.bolton-menk.com/bmigis/rest/services/Utilities/Geometry/GeometryServer')
buffers = gs.buffer(fs3, 3857, 100, unionResults=True)
pros = gs.project(fs3, 3857, 26915)
gc3 = restapi.GeometryCollection(geometries)
##test = r'C:\TEMP\testing.gdb\rest_buffers3'
##restapi.exportGeometryCollection(buffers, test)
##@restapi.common_types.geometry_passthrough
##def test(geom):
##    """this is a test docstring"""
##    return geom
##
##gc_dec = test(geometries)

# feature editing tests

    

