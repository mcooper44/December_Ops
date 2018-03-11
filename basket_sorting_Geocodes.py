from random import randint
from collections import namedtuple
from collections import defaultdict
from math import radians, cos, sin, asin, sqrt
import csv
import sqlite3

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

class Delivery_Household():
    '''
    a collection of datapoints needed to assemble a delivery route
    :TO DO:
    Decisions need to be made regarding keying off applicant ID or HH_ID and 
    what implications that may have for creating errors
    '''
    def __init__(self, file_id, hh_id, family_size, lat, lng):
        self.main_app_ID = file_id
        self.household_ID = hh_id
        self.hh_size = family_size
        # used by the Delivery_Routes().sort_method()
        self.geo_tuple = Geolocation(float(lat), float(lng)) 

class Delivery_Routes():
    '''
    takes a collection of households and parses them into individual routes

    '''    
    route_collection = None # a dictionary of all the routes and the hh they contain 1: [12345,2341234,123412,62345]

    def __init__(self, max_boxes, start_count):
        self.max_boxes = max_boxes # max number of boxes per/route
        self.start_count = start_count # what we start counting routes at
        self.hh_dict = None # a dictionary of Delivery_Household() objects
        self.conn = None # connection to the route db
        self.cur = None # database cursor
        self.route_db = None # the path to the last database we connected to

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
        if Delivery_Routes.route_collection:
            return Delivery_Routes.route_collection
        else:
            print('the routes have not been created yet')

    def set_hh_data_structure(self, data_structure):
        '''
        recieves a dictionary of file id keys: and Delivery_Household objects to use in generating routes
        '''
        self.hh_dict = data_structure
        print('structure set with {} values'.format(len(self.hh_dict)))

    def sort_method(self):
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
        households = self.hh_dict
        routes = {} # labeled routes and the families they contain
        assigned = [] # container to add hh that have been assigned
        print('starting sort_method')
        for applicant in households: # for key in dictionary of households
            
            h1_lat, h1_long = households[applicant].geo_tuple # a tuple of (lat, lng)
            applicant_route = [] # the working container that we will then add to the route dictionary
            size = str(households[applicant].hh_size) # turn the size into a string so we can...
            boxes = box_mask[size] # start building the route with the household
            
            if applicant not in assigned:
                # add them to list of assigned HH to avoid adding them again
                assigned.append(applicant) 
                # start by adding the household we are starting with to the container for this route
                applicant_route.append(applicant)                            
                # build a container to add {calculated distances: households} to
                # this will allow us to make a sorted list of the shortest distances and the HH
                # that are at that distance            
                distance_hh_dictionary = defaultdict(list)                                                                                                             
                # ITERATE THROUGH THE HOUSEHOLDS AND CALCULATE DISTANCES FROM THE CHOSEN STARTING HH
                for HH in households.keys(): # iterate through the keys to find the distances of remaining households                    
                    if HH != applicant: # if this is a new household
                        if HH not in assigned: # and we have not already used them in a route
                            ident = HH # their file number
                            h2_lat, h2_long = households[HH].geo_tuple # their lat,long
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
                        if fam not in assigned: # if we haven't sorted them into a route yet
                            fam_size = str(households[fam].hh_size) # determine family size
                            box_num = box_mask[fam_size] # determine number of boxes
                            # do math to determine if we can add them to the route
                            # then if there are still more families at this distance we need to pop the one we just added
                            # and evaluate the next one and so on down the chain until we max out box count per route or exaust
                            # remaining households
                            if box_num + boxes <= max_box_count: # if we added this family to the pile and they didn't add too many boxes
                                boxes += box_num
                                assigned.append(fam) # add them to the assigned list
                                applicant_route.append(fam) # add them to the route            
            if applicant_route:
                print('we have iterated and made a route! It is {}'.format(applicant_route))
                r_key = str(route_counter)
                routes[r_key] = applicant_route
                route_counter += 1
        Delivery_Routes.route_collection = routes
   
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
            letter_map = list(zip(fids, r_letters)) # [(fid1, 'A'), (fid2, 'B')]

            for fid_lttr in letter_map:
                db_tple = (fid_lttr[0], # file ID
                           r_number,     # route number
                           fid_lttr[1])   # route letter
                self.cur.execute("INSERT INTO routes VALUES (?, ?, ?)", db_tple)
                self.conn.commit()   
        self.conn.close()

class Source_File_Object():
    '''
    Can parse csv or db data sources to make routes by parsing the contents 
    and populating the Delivery_Routes() class dict with the key data points
    #####
    NB:
    This object will need to be rebuilt 
    #####
    '''
    def __init__(self, source_path, keyword_flag):
        self.data_source = source_path # path to where the source data can be found
        self.source_type = keyword_flag # either 'csv' or 'db'     
        self.hh_data_structure = None

    def parse_source_into_delivery_hh(self):
        '''
        opens a database or csv of [client_id, size, geocodes] as a source 
        for the route sorting method
        path type takes 'csv' or 'db' as keywords
        it then parses the source line by line into a dictionary of 
        file_id : Delivery_Household() contained in a Delivery_Routes() object
        this allows the Delivery_Routes() object to begin 
        '''
        hh_dict = {}
        if self.source_type == 'csv':
            with open(self.data_source, newline='') as f:
                visits = csv.reader(f)
                next(visits, None) # skip headers
                print('file open. starting the parse operation')
                for visit in visits:
                    file_id = str(visit[0])
                    family_size = visit[1]
                    household_id = visit[2]
                    lt = visit[3] # lat
                    lg = visit[4] # long
                    # then add the key information to the .route object hh_dict
                    hh_dict[file_id] = Delivery_Household(file_id, household_id, family_size, lt, lg)
        self.hh_data_structure = hh_dict

        if self.source_type == 'db':            
            pass

    def return_hh_data_structure(self):
        '''
        returns the dictionary of file_id keys: to Delivery_Household ojbects
        and their associated data points
        '''
        if self.hh_data_structure:
            return self.hh_data_structure
        else:
            print('the dictionary has not been initialized')


