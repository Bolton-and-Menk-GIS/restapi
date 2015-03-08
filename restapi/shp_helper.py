#-------------------------------------------------------------------------------
# Name:        shp_helper
# Purpose:
#
# Author:      Caleb Mackey
#
# Created:     01/19/2015
#-------------------------------------------------------------------------------
import shapefile

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


class shp(object):
    def __init__(self, shapeType='NULL', path=''):
        self.w = shapefile.Writer(shp_dict[shapeType.upper()])
        self.shapeType = self.w.shapeType
        self.path = path

    def add_field(self, name, fieldType="C", size="50", decimal=0):
        """Adds a dbf field descriptor to the shapefile.

        Valid types for DBASE:

        B 	Binary, a string 	10 digits representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.
        C 	Character 	All OEM code page characters - padded with blanks to the width of the field.
        D 	Date 	8 bytes - date stored as a string in the format YYYYMMDD.
        N 	Numeric 	Number stored as a string, right justified, and padded with blanks to the width of the field.
        L 	Logical 	1 byte - initialized to 0x20 (space) otherwise T or F.
        M 	Memo, a string 	10 digits (bytes) representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.
        @ 	Timestamp 	8 bytes - two longs, first for date, second for time.  The date is the number of days since  01/01/4713 BC. Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
        I 	Long 	4 bytes. Leftmost bit used to indicate sign, 0 negative.
        + 	Autoincrement 	Same as a Long
        F 	Float 	Number stored as a string, right justified, and padded with blanks to the width of the field.
        O 	Double 	8 bytes - no conversions, stored as a double.
        G 	OLE 	10 digits (bytes) representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.

        source: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
        """
        if not size:
            size = "50"
        self.w.field(str(name), fieldType, str(size), decimal) #field name cannot be unicode, must be str()

    def add_row(self, shape, attributes):
        """method to add a rec

        Required:
            shape -- shape geometry
            attributes -- tuple of attributes in order of fields
        """
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

if __name__ == '__main__':
    pass
