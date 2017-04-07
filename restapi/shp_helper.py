#-------------------------------------------------------------------------------
# Name:        shp_helper
# Purpose:
#
# Author:      Caleb Mackey
#
# Created:     01/19/2015
#-------------------------------------------------------------------------------
from __future__ import print_function
import shapefile
import itertools
import datetime
import json
import unicodedata

# constants (from shapefile)
shp_dict = {
    'NULL' : 0,
    'POINT' : 1,
    'POLYLINE' : 3,
    'POLYGON' : 5,
    'MULTIPOINT' : 8,
    'POINTZ' : 11,
    'POLYLINEZ' : 13,
    'POLYGONZ' : 15,
    'MULTIPOINTZ' : 18,
    'POINTM' : 21,
    'POLYLINEM' : 23,
    'POLYGONM' : 25,
    'MULTIPOINTM' : 28,
    'MULTIPATCH' : 31
    }

shp_code = {v:k for k,v in shp_dict.iteritems()}

class ShpWriter(object):
    def __init__(self, shapeType='NULL', path=''):
        self.w = shapefile.Writer(shp_dict[shapeType.upper()] if isinstance(shapeType, basestring) else shapeType)
        self.shapeType = self.w.shapeType
        self.path = path

    def add_field(self, name, fieldType="C", size="50", decimal=0):
        """Adds a dbf field descriptor to the shapefile.

        Valid types for DBASE:

        B 	Binary, a string 	10 digits representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.
        C 	Character 	All OEM code page characters - padded with blanks to the width of the field.
        D 	Date 	8 bytes - date stored as a string in the format YYYYMMDD.
        N 	Numeric 	Number stored as a string, right justified, and padded with blanks to the width of the field.
        F 	Float/Double 	Number stored as a string, right justified, and padded with blanks to the width of the field.

        source: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm

        source: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
        """
        if not size:
            size = "50"
        field_name = field_name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore')
        self.w.field(field_name, fieldType, str(size), decimal) #field name cannot be unicode, must be str()

    def add_row(self, shape, attributes):
        """method to add a rec

        Required:
            shape -- shape geometry
            attributes -- tuple of attributes in order of fields
        """
        if isinstance(shape, shapefile.shapefile._Shape):
            self.w._shapes.append(shape)
        else:
            if self.shapeType in (1, 8, 11, 21, 25, 31):
                self.w.point(*shape)
            elif self.shapeType in (3, 13, 23):
                self.w.line(shape)
            else:
                self.w.poly(shape)
        self.w.record(*attributes)

    def save(self, path=''):
        if not path:
            if self.path:
                self.w.save(self.path)
        else:
            self.w.save(path)

class ShpEditor(object):
    def __init__(self, path):
        self.r = shapefile.Reader(path)
        self.__isBuilt = False
        self.fields = self.r.fields[1:]
        self.field_names = []
        self.field_indices = {n:i for i,n in enumerate(self.field_names)}
        self.records = self.r.records()
        self.shapes = list(self.r.shapes())
        self.shapeType = self.r.shapeType
        self.path = path
        self.__shapeHolder = shapefile.Writer(self.shapeType)
        self.w = shapefile.Writer(self.shapeType)
        for f in self.fields:
            self.w.field(*f)
            self.field_names.append(f[0])

    def addDefaults(self, attributes, default=" "):
        """adds default values to records to fill missing data.

        Requried:
            attributes -- list of attributes for a row
            default -- deafault value for field.  Default is nothing (' ')
        """
        if not isinstance(attributes, list):
            attributes = list(attributes)
        f_diff = len(self.fields) - len(attributes)

        if f_diff >= 1:
            return attributes + [default] * f_diff
        else:
            return attributes[:len(self.fields)]


    def add_field(self, name, fieldType="C", size="50", decimal=0, default=" "):
        """Adds a dbf field descriptor to the shapefile.

        Required:
            name -- name of new field
            fieldType -- type of field to add (valid values listed below)
            size -- size of field (only used for "C" (text) fields)
            decimal -- number of significant decimal places. Default is 0
            default -- deafault value for field.  Default is nothing (' ')

        Valid types for DBASE:

        B 	Binary, a string 	10 digits representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.
        C 	Character 	All OEM code page characters - padded with blanks to the width of the field.
        D 	Date 	8 bytes - date stored as a string in the format YYYYMMDD.
        N 	Numeric 	Number stored as a string, right justified, and padded with blanks to the width of the field.
        F 	Float/Double 	Number stored as a string, right justified, and padded with blanks to the width of the field.

        source: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
        """
        if not size:
            size = "50"
        field_name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore')
        self.w.field(field_name, fieldType, str(size), decimal) #field name cannot be unicode, must be str()
        self.fields.append([field_name, fieldType, int(size), int(decimal)])
        self.field_names.append(name)
        self.field_indices[name] = len(self.fields) - 1
        for rec in self.records:
            rec.append(default)

        self.__isBuilt = False

    def add_row(self, shape, attributes=[]):
        """method to add a record to shapefile in memory, this does
        not get applied until save() method is called.

        Required:
            shape -- shape geometry
            attributes -- tuple of attributes in order of fields
        """
        if isinstance(shape, shapefile.shapefile._Shape):
            self.shapes.append(shape)
            self.__shapeHolder._shapes.append(shape)
        else:
            if self.shapeType in (1, 8, 11, 21, 25, 31):
                self.__shapeHolder.point(*shape)
            elif self.shapeType in (3, 13, 23):
                addShp = self.__shapeHolder.line(shape)
            else:
                self.__shapeHolder.poly(shape)

        self.shapes.append(self.__shapeHolder.shapes()[-1])
        self.records.append(self.addDefaults(attributes))
        self.__isBuilt = False

    def write_row(self, shape, attributes=[]):
        """method to write a record

        Required:
            shape -- shape geometry
            attributes -- tuple of attributes in order of fields
        """
        if isinstance(shape, shapefile.shapefile._Shape):
            self.w._shapes.append(shape)
        else:
            if self.w.shapeType in (1, 8, 11, 21, 25, 31):
                self.w.w.point(*shape)
            elif self.w.shapeType in (3, 13, 23):
               self.w.line(shape)
            else:
                self.w.poly(shape)
        self.w.record(*self.addDefaults(attributes))

    def update_row(self, rowIndex=0, shape=None, *args, **attributes):
        """method to add a rec

        Required:
            rowIndex -- index for row
            shape -- shape geometry, only put a value here if you are
                updating the geometry
            attributes -- key word argument of attributes in (field_name="field_value")
        """
        # check if there is a shape edit, if not skip and do attribute update
        if shape:
            if not isinstance(shape, shapefile.shapefile._Shape):
                self.shapes[rowIndex].points = shape
            else:
                self.shapes[rowIndex] = shape

        if attributes:
            for f_name, f_value in attributes.iteritems():
                f_index = self.field_indices[f_name]
                if f_index >= len(self.records[rowIndex]):
                    self.records[rowIndex].append(f_value)
                else:
                    self.records[rowIndex][f_index] = f_value

        self.__isBuilt = False

    def delete(self, index):
        """deletes a record at an index

        Required:
            index -- index to delete record
        """
        try:
            self.shapes.pop(index)
            self.records.pop(index)
        except IndexError:
            print('No record found at index: {}'.format(index))

    def _rebuild(self):
        """adds all rows to shapefile"""
        for shape, record in iter(self):
            self.write_row(shape, record)
        self.__isBuilt = True

    def save(self, path=''):
        """saves the shapefile.  By default will save over existing
        shapefile.  If you want to save a copy, specify a path

        Optional:
            path -- optional path to save a new copy of the shapefile
        """
        if not self.__isBuilt:
            self._rebuild()
        if not path:
            self.w.save(self.path)
        else:
            if not path.endswith('.shp'):
                path = os.path.splitext(path)[0] + '.shp'
            self.w.save(path)

    def __iter__(self):
        """return generator for (shape, record) for each feature"""
        for feature in itertools.izip(self.shapes, self.records):
            yield feature
