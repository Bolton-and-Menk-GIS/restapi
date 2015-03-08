#-------------------------------------------------------------------------------
# Name:        restapi_test
# Purpose:     tests the restapi module.
#
# Author:      Caleb Mackey
#
# Created:     04/03/2015
# Copyright:   (c) calebma 2015
# Licence:     <your licence>
#
# Disclaimer:  The test services used below will be SLOW! The speed for the restapi
#   module depends on your connection speeds, the capabilities of the servers that
#   are being accessed, and the load the server is receiving at the time of each
#   request.
#-------------------------------------------------------------------------------
import unittest
import restapi as r

# globals for testing
SERVICE_DIR = 'http://services.nationalmap.gov/ArcGIS/rest/services'
TEST_MAPSERVICE_NAME = 'http://services.nationalmap.gov/ArcGIS/rest/services/structures/MapServer'

# test classes
ags = r.ArcServer(SERVICE_DIR)

class TestRestapiFunctions(unittest.TestCase):

    def tearDown(self):
        pass

    def test_ArcServer(self):
        self.assertEqual(SERVICE_DIR, ags.url)
        self.assertTrue(isinstance(ags, r.ArcServer))

    def test_MapService(self):
        self.structures = ags.get_MapService('structures')
        self.assertEqual(TEST_MAPSERVICE_NAME, self.structures.url)
        self.assertTrue(isinstance(self.structures, r.MapService))

    # to do - add more tests!

if __name__ == '__main__':
    unittest.main()
