#####################################################################
# this file contains classes and methods to access collections of
# geometries for the different neighbourhoods in Kitchener and
# Waterloo.  This information is helpful for planning and data
# aggregation functions. 
#####################################################################

from shapely.geometry import MultiPoint, Point
import sqlite3
from tinydb import TinyDB, Query

class Neighbourhoods():
    '''
    takes a TinyDB json object of neighbourhood geometries and sets up a 
    structure that can be used to determine which neighbourhood a given
    set of coodinates falls within
    '''
    def __init__(self):
        self.db = None
        self.kw_neighbourhoods = None
        self.nhood_shapes = {}

    def open_tinydb(self, path_to_tdb):
        '''
        initalize the tinydb object
        '''
        self.db = TinyDB(path_to_tdb)

    def set_kw_neighbourhoods(self):
        '''
        pull all the data out of the json tinydb file
        '''
        self.kw_neighbourhoods = self.db.all()

    def extract_shapes(self):
        '''
        take the json tinydb of neighbourhoods in Kitchener and Waterloo
        initialized and loaded by the open_tinydb and set_kw_neighbourhoods
        methods and pull out all of the geometries and stack them up in a dictionary 
        keyed off the neighbourhoods.  This methods gets at both Kitchener and Waterloo
        neighbourhoods
        '''
        for district in self.kw_neighbourhoods:
            name = district['name']
            shape = MultiPoint([(x[0], x[1]) for x in district['coords']]).convex_hull
            self.nhood_shapes[name] = shape
    
    def find_in_shapes(self, lat, lng):
        '''
        takes a lat, lng and iterates through the dictionary of shapes
        returns either the string name of the neighbourhood that the 
        points are in, or False
        '''
        location = (lat, lng)
        point = Point(location) # initialize a shapely Point
        in_neighbourhood = False
        for neighbourhood in self.nhood_shapes.keys():
            if point.within(self.nhood_shapes[neighbourhood]):
                in_neighbourhood = neighbourhood
        return in_neighbourhood # kitchener names r in CAPS, Waterloo Normal Case

    def __str__(self):
        return 'container for shapes populated with {} entries'.format(len(self.nhood_shapes))
    
    def __iter__(self):
        '''
        yields the neighbourhood names in the shapes dictionary
        '''
        for neighbourhood_name in self.nhood_shapes.keys():
            yield neighbourhood_name

if __name__ == '__main__':
    print('starting')
    kw = Neighbourhoods()
    kw.open_tinydb('City of Waterloo and Kitchener Planning district Geometry.json')
    kw.set_kw_neighbourhoods()
    kw.extract_shapes()
    print(kw)
    lat, lng = 43.456692, -80.511280 # the hospital
    zone = kw.find_in_shapes(lat, lng)
    print(zone) # should be KW HOSPITAL


