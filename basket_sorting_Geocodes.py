from collections import namedtuple
from collections import defaultdict
from math import radians, cos, sin, asin, sqrt
import csv
import sqlite3
import logging
import datetime

logging.basicConfig(filename='Logging/route_sorting.log',level=logging.INFO)
logging.info('Running new session {}'.format(datetime.datetime.now()))

Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')

box_mask = {'0': 1,
            '1' : 1,
            '2' : 1,
            '3' : 1,
            '4' : 1,
            '5': 2,
            '6' : 2,
            '7' : 2,
            '8' : 3,
            '9' : 3,
            '10': 3,
            '11': 3,
            '12': 4,
            '13':4,
            '14':4,
            '15':4,
            '16':4,
            '17':4,
            '18':4
            }

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Thanks Stack Overflow!
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km


class Route_Database():
    '''
    This is a database of households sorted into neighbourhoods in one table
    and households sorted into routes in another table
    It is the central database that will recieve routes, reproduce them
    and return route specific information when needed
    '''
    def __init__(self, path_name):
        self.path_name = path_name
        self.conn = None
        self.cur = None
        if self.path_name:
            self.conn = sqlite3.connect(path_name)
            self.cur = self.conn.cursor()
            self.cur.execute('''CREATE TABLE IF NOT EXISTS routes (file_id INT, route_number INT,
                       route_letter TEXT)''')
            self.conn.commit()
            self.cur.execute('''CREATE TABLE IF NOT EXISTS applicants (file_id
                             INT, f_name TEXT, l_name TEXT, family_size INT, phone TEXT,
                             email TEXT, address_1 TEXT, address_2 TEXT, city
                             TEXT, diet TEXT)''')
            self.conn.commit()

    def add_route(self, file_id, rn, rl):
        '''
        logs a route in the database
        '''
        db_tple = (file_id, rn, rl)
        self.cur.execute("INSERT INTO routes VALUES (?, ?, ?)", db_tple)
        self.conn.commit()   
        
    def add_family(self, family_tple):
        '''
        this adds a household to the applicants table
        '''
        self.cur.execute("INSERT INTO applicants VALUES (?,?,?,?,?,?,?,?,?,?)",
                         family_tple)
        self.conn.commit()
        
    def __iter__():
        '''
        returns a package of tuples from the database for each household that
        has been logged.
        the package is a tuple of household, route info tuples
        
        '''
        self.cur.execute("SELECT * FROM applicants")
        applicants = self.cur.fetchall()
        for household in applicants:
            fid = household[0]
            self.cur.execute("SELECT * FROM routes WHERE file_id = ?",(fid,))
            rt_tple = self.cur.fetchone()
            package = (household, rt_tple)
            yield package

    def close_db(self):
        '''
        closes db connection

        '''
        self.conn.close()
        print('db connection closed')

class Delivery_Household():
    '''
    a collection of datapoints needed to assemble a delivery route
    :TO DO:
    Decisions need to be made regarding keying off applicant ID or HH_ID and 
    what implications that may have for creating errors
    '''
    def __init__(self, file_id, hh_id, family_size, lat, lng, rn=None, rl=None):
        self.main_app_ID = file_id
        self.household_ID = hh_id
        self.hh_size = family_size
        # used by the Delivery_Routes().sort_method()
        self.geo_tuple = Geolocation(float(lat), float(lng)) 
        self.route_number = rn
        self.route_letter = rl

    def return_hh(self):
        '''
        returns the input values and route
        '''
        lat, lng = self.geo_tuple
        return (self.main_app_ID, 
                self.household_ID, 
                self.hh_size, 
                lat, 
                lng,
                self.route_number,
                self.route_letter)

    def add_routing(self, number, letter):
        '''
        a method to add a route number and letter to the household
        '''
        self.route_number = number
        self.route_letter = letter

    def routed(self):
        '''
        returns True or False if it has a route number and letter designation
        '''
        return all([self.route_number, self.route_letter])
    
    def return_route(self):
        '''
        returns a tuple of file id and routing info for the household
        '''
        return (self.main_app_ID ,self.route_number, self.route_letter)

class Delivery_Household_Collection():
    '''
    A collection of Delivery_Household() objects
    This object is used to supply the Delivery_Routes class with Households to
    sort into routes
    '''
    def __init__(self):
        self.hh_dict = {}
        self.fids_routed = set()

    def add_household(self, file_id, hh_id, family_size, lat, lng):
        '''
        add a household object to the dictionary 
        '''
        self.hh_dict[file_id] = Delivery_Household(file_id, hh_id, family_size,
                                                   lat, lng)
    def HH_set(self):
        '''
        returns a set of file id's
        '''
        return set(self.hh_dict.keys())
    
    def has_been_routed(self, fid):
        '''
        calls the .routed() method of the Delivery_Household contained
        in the self.hh_dict dictionary which returns either True or False
        '''
        return self.hh_dict[fid].routed()

    def label_route(self, route_key, route):
        '''
        Takes a route number and route and then labels the Delivery Household
        objects that correspond to route
        '''
        r_letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L'] 
        letter_map = zip(route, r_letters) # [(fid1, 'A'), (fid2, 'B')]

        for fid_lttr in letter_map:
            fid, lttr = fid_lttr
            self.hh_dict[fid].add_routing(route_key, lttr)
    
    def return_size(self, fid):
        '''
        returns the family size of a Delivery_Household in the dictionary
        '''
        fam_object = self.hh_dict.get(fid, 'None')
        return fam_object.hh_size


    def __iter__(self):
        for hh in self.hh_dict:
            yield self.hh_dict[hh]

class Delivery_Routes():
    '''
    takes a collection of households and parses them into individual routes

    '''    
    def __init__(self, max_boxes = 7, start_count = 1):
        self.max_boxes = max_boxes # max number of boxes per/route
        self.start_count = start_count # what we start counting routes at
        self.hh_dict = None # a dictionary of Delivery_Household() objects
        self.route_collection = None # a dictionary of all the routes and the hh they contain 1: [12345,2341234,123412,62345]

    def get_status(self):
        '''
        returns the status of the object.  Is it ready to parse points and return a route list?
        '''
        if not self.hh_dict:
            print('not ready.  please add a data source to populate the dictionary')
        else:
            print('ready with {}'.format(type(self.hh_dict)))

    def get_route_collection(self):
        '''
        returns the collection of routes held in the object
        '''
        if self.route_collection:
            return self.route_collection
        else:
            print('the routes have not been created yet')
    
    def set_hh_dict(self, dict_of_DH_objects):
        '''
        sets the datastructure from an external source
        '''
        self.hh_dict  = dict_of_DH_objects
        print('Dictionary of Households set')

    def sort_method(self, households):
        '''
        takes a dictionary of {"file_id" : Delivery_Household} objects
        it takes the first household off the pile and calculates the distance 
        between it and all of the other households.
        It then loops through the nearest households and based off their family size
        adds them to the route until it reaches the max_box size, upon which, the
        route is complete.  It then repeats the process will all of the subsequent 
        households until all have been bundled into routes.  It then sets the
        route_collection class variable = to the list of routes
        '''
        max_box_count = self.max_boxes
        route_counter = self.start_count # this is where we start counting the routes
        routes = {} # labeled routes and the families they contain
        assigned = set() # container to add hh that have been assigned
        print('starting sort_method')
        # for key in dictionary of households in the
        # Delivery_Households_Collection class...
        for applicant in households:
            print(applicant)
            h1_lat, h1_long = applicant.geo_tuple # a tuple of (lat, lng)
            applicant_route = [] # the working container that we will then add to the route dictionary
            size = str(applicant.hh_size) # turn the size into a string so we can...
            boxes = box_mask[size] # start building the route with the household
            app_file_id = applicant.main_app_ID
            if not applicant.routed():
                # add them to list of assigned HH to avoid adding them again
                #assigned.add(applicant) 
                # start by adding the household we are starting with to the container for this route
                applicant_route.append(app_file_id)
                # build a container to add {calculated distances: households} to
                # this will allow us to make a sorted list of the shortest distances and the HH
                # that are at that distance            
                distance_hh_dictionary = defaultdict(list)                                                                                                             
                # ITERATE THROUGH THE HOUSEHOLDS AND CALCULATE DISTANCES FROM THE CHOSEN STARTING HH
                for HH in households: # iterate through the keys to find the distances of remaining households                    
                    if HH.main_app_ID != app_file_id: # if this is a new household
                        if not HH.routed(): # and we have not already used them in a route
                            ident = HH.main_app_ID # their file number
                            # TO DO - clarify how to access households in this
                            # block.  Should we iterate through objects or file
                            # ids and then grab the object?
                            h2_lat, h2_long = HH.geo_tuple # their lat,long
                            # caculated the distance between the two households
                            distance_between = haversine(h1_long, h1_lat, h2_long, h2_lat) # returns float distance in KM                        
                            d_key = str(distance_between) # convert to string so we can use it as a dictionary key
                            distance_hh_dictionary[d_key].append(ident) # update dictionary of distances: HH identifier
                # now we have calculated all the distances from Route #X A to all of the other households in the caseload
                # sort a list of all the distances so we can skim the shortest off
                distances = sorted([float(k) for k in distance_hh_dictionary.keys()])
                # NOW WE WILL ITERATE THROUGH THE DISTANCES AND TRY AND PUT A ROUTE TOGETHER
                for float_value in distances: # for distance in sorted listed of distances
                    key = str(float_value) # convert the float to a string so we can use it in the distance : families at that distance dictionary
                    # now we need to iterate through the list of HH at this distance.
                    for fam in distance_hh_dictionary[key]: # for the individual or family in the list of households at this distance
                        if not households.has_been_routed(fam): # if we haven't sorted them into a route yet
                            # TODO we need a method to access the HH objects data
                            # here
                            fam_size = households.return_size(fam) # determine family size
                            box_num = box_mask[fam_size] # determine number of boxes
                            # do math to determine if we can add them to the route
                            # then if there are still more families at this distance we need to pop the one we just added
                            # and evaluate the next one and so on down the chain until we max out box count per route or exaust
                            # remaining households
                            if box_num + boxes <= max_box_count: # if we added this family to the pile and they didn't add too many boxes
                                boxes += box_num
                                assigned.add(fam) # add them to the assigned list
                                applicant_route.append(fam) # add them to the route
                                # log the info for verification later household 1, route number, family added to route, distance from H1
                                logging.info('{} {} {} {}'.format(applicant, route_counter, fam, key))
            
            if applicant_route:
                print('we have iterated and made a route! It is {}'.format(applicant_route))
                r_key = str(route_counter)
                #routes[r_key] = applicant_route
                # if we record what route each HH is in do we need a separate
                # data structure of routes in this class?  We can just iterate
                # through the Delivery_Household_Collection and strip out the
                # necessary information
                
                # this step records the routes on the households
                households.label_route(r_key, applicant_route)
                route_counter += 1
        #self.route_collection = routes
   
    def create_route_db(self, db_name):
        '''
        create a database to dump values into
        '''
        self.route_db = db_name
        self.conn = sqlite3.connect(db_name)
        self.cur = self.conn.cursor()
        self.cur.execute('''CREATE TABLE routes (file_id INT PRIMARY KEY, route_number INT,
                       route_letter TEXT)''')
        self.conn.commit()
        self.conn.close()


    def log_route_in_db(self, db_name=None):
        '''
        This method logs the route dictionary into a database for portability etc.
        r_num = route #
        r_list = [fileid1, fileid2, ...]
        db_name allows you to override the last db connected to
        '''
        r_letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
        
        if db_name:
            self.route_db = db_name
        self.conn = sqlite3.connect(self.route_db)
        self.cur = self.conn.cursor()
        print('connection to {} established'.format(self.route_db))
        
        for r_number in self.route_collection.keys():
            fids = self.route_collection[r_number]
            letter_map = zip(fids, r_letters) # [(fid1, 'A'), (fid2, 'B')]

            for fid_lttr in letter_map:
                db_tple = (fid_lttr[0], # file ID
                           r_number,     # route number
                           fid_lttr[1])   # route letter
                self.cur.execute("INSERT INTO routes VALUES (?, ?, ?)", db_tple)
                self.conn.commit()   
        self.conn.close()


