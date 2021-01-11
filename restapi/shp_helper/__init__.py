#-------------------------------------------------------------------------------
# Name:        shp_helper
# Purpose:
#
# Author:      Caleb Mackey
#
# Created:     01/19/2015
#-------------------------------------------------------------------------------
from __future__ import print_function
from .. import shapefile
import datetime
import json
import unicodedata
import six
import munch


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

shp_code = {v:k for k,v in six.iteritems(shp_dict)}

class ShpWriter(object):
    """Class that writes a shapefile.
    
    Attributes:
        w: Shapefile writer object.
        shapeType: Shape type.
        path: Path of shapefile.
    """

    def __init__(self, path, shapeType='NULL', autoBalance=True):
        """Inits class with the shapefile.
        
        Args:
            shapeType: Type of shape. Defaults to 'NULL'.
            path: String for the path of the shapefile.
            autoBalance (bool): option to make sure the record and shape count is matched, default is True.
        """
        self.w = shapefile.Writer(path, 
            shapeType=shp_dict[shapeType.upper()] if isinstance(shapeType, six.string_types) else shapeType, 
            autoBalance=autoBalance
        )
        self.shapeType = self.w.shapeType
        self.path = path

    def add_field(self, name, fieldType="C", size="50", decimal=0):
        """Adds a dbf field descriptor to the shapefile.

        Args:
            name: Name of new field.
            fieldType: Type of field to add (valid values listed below). 
                Defaults to "C".
            size: Size of field (only used for "C" (text) fields). 
                Defaults to "50".
            decimal: Number of significant decimal places. Default is 0.
            
        Valid types for fieldType:

            B: Binary, a string 10 digits representing a .DBT block number. 
                The number is stored as a string, right justified and padded with blanks.
            C: Character,All OEM code page characters - padded with blanks to the 
                width of the field.
            D: Date, 8 bytes - date stored as a string in the format YYYYMMDD.
            N: Numeric, Number stored as a string, right justified, and padded 
                with blanks to the width of the field.
            F: Float/Double, Number stored as a string, right justified, and 
                padded with blanks to the width of the field.

        source: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
        """

        if not size:
            size = "50"
        # field_name = field_name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore')
        field_name = name.encode('utf-8')
        self.w.field(field_name, fieldType, str(size), decimal) #field name cannot be unicode, must be str()

    def add_row(self, shape, *attributes, **kwattributes):
        """Method to add a row.

        Args:
            shape: Shape geometry.
            attributes: Tuple of attributes in order of fields.
        """
        if isinstance(shape, shapefile.Shape):
            self.w.shape(shape)
        else:
            if self.shapeType in (1, 8, 11, 21, 25, 31):
                self.w.point(*shape)
            elif self.shapeType in (3, 13, 23):
                self.w.line(shape)
            else:
                self.w.poly(shape)
        
        self.w.record(*attributes, **kwattributes)

    def save(self, path=''):
        """Saves the file in the given path.

        Args:
            path: The path to be saved.
        """
        self.w.close()

class ShpEditor(object):
    """Class that handles the editing of shapefiles.

    Attributes:
        r: Shapefile reader.
        fields: List of fields in file.
        field_names: List of field names.
        field_indices: Dictionary of field indices.
        records: Records in shapefile.
        shapes: List of shapes.
        shapeType: Shape type.
        path: Path of file.
        w: Shapefile writer object.
    """

    def __init__(self, path):
        """Inits class with shapefile information.
        
        Args:
            path: Path for shapefile.
        """
        
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
        """Adds default values to records to fill missing data.

        Args:
            attributes: List of attributes for a row.
            default: Default value for field. Default is nothing (" ")
        
        Returns:
            Attributes in the row.
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

        Args:
            name: Name of new field.
            fieldType: Type of field to add (valid values listed below). 
                Default is "C".
            size: Size of field (only used for "C" (text) fields). Default is "50".
            decimal: Number of significant decimal places. Default is 0.
            default: Default value for field.  Default is nothing (' ').

        Valid types for fieldType:

            B: Binary, a string 10 digits representing a .DBT block number. 
                The number is stored as a string, right justified and padded with blanks.
            C: Character,All OEM code page characters - padded with blanks to the 
                width of the field.
            D: Date, 8 bytes - date stored as a string in the format YYYYMMDD.
            N: Numeric, Number stored as a string, right justified, and padded 
                with blanks to the width of the field.
            F: Float/Double, Number stored as a string, right justified, and 
                padded with blanks to the width of the field.

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
        """Method to add a row to shapefile in memory, this does
                not get applied until save() method is called.

        Args:
            shape: Shape geometry.
            attributes: Tuple of attributes in order of fields.
        """

        if isinstance(shape, shapefile.Shape):
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
        """Method to write a row.

        Args:
            shape: Shape geometry
            attributes: Tuple of attributes in order of fields.
        """

        if isinstance(shape, shapefile.Shape):
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
        """Method to update a row.

        Args:
            rowIndex: Index for row. Defaults to 0.
            shape: Shape geometry, only put a value here if you are
                updating the geometry. Defaults to None.
            **attributes: Key word argument of attributes in (field_name="field_value").
        """
        # check if there is a shape edit, if not skip and do attribute update
        if shape:
            if not isinstance(shape, shapefile.Shape):
                self.shapes[rowIndex].points = shape
            else:
                self.shapes[rowIndex] = shape

        if attributes:
            for f_name, f_value in six.iteritems(attributes):
                f_index = self.field_indices[f_name]
                if f_index >= len(self.records[rowIndex]):
                    self.records[rowIndex].append(f_value)
                else:
                    self.records[rowIndex][f_index] = f_value

        self.__isBuilt = False

    def delete(self, index):
        """Deletes a record/row at an index.

        Args:
            index: Index to delete record/row.

        Raises:
            IndexError: 'No record found at index: {}'.
        """

        try:
            self.shapes.pop(index)
            self.records.pop(index)
        except IndexError:
            print('No record found at index: {}'.format(index))

    def _rebuild(self):
        """Adds all rows to shapefile."""
        for shape, record in iter(self):
            self.write_row(shape, record)
        self.__isBuilt = True

    def save(self, path=''):
        """Saves the shapefile.  By default will save over existing
                shapefile.  If you want to save a copy, specify a path.

        Args:
            path: Optional path to save a new copy of the shapefile.
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
        """Returns generator for (shape, record) for each feature."""
        for feature in six.moves.zip(self.shapes, self.records):
            yield feature
