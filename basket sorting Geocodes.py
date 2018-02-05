from random import randint
from collections import namedtuple
from collections import defaultdict
from math import radians, cos, sin, asin, sqrt
import csv

csv_file_path = '2016Christmas.csv'

Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')

box_mask = {'0': 1,
            '1' : 1,
            '2' : 1,
            '3' : 1,
            '4' : 1,
            '5': 2,
            '6' : 2,
            '7' : 3,
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

def use_csv(csv_file_path):
    '''
    opens a csv file of households and their key data points
    iterates through it and extracts {file id # : family size, geocodes}
    as a named tuple
    Client = namedtuple('Client', 'size location')
    location is a named tuple:
    Geolocation = namedtuple('Geolocation', 'lat long')
    '''
    hh_dictionary = {}
    with open(csv_file_path, newline='') as f:
        visits = csv.reader(f)
        next(visits, None) # skip headers
        for visit in visits:
            file_id = str(visit[6])
            family_size = visit[7]
            geocodes = Geolocation(float(visit[11]),float(visit[12])) # because they are picked as strings
            hh_dictionary[file_id] = Client(family_size, geocodes)
    return hh_dictionary

class Delivery_Routes():
    '''
    Can parse csv or db data sources to make routes
    Can open route db's and operate on the contents
    either open and present or add to existing routes
    '''

    hh_dict = None
    route_collection = None
    route_db = None

    def __init__(self, source_path, keyword_flag):
        self.data_source = source_path # path to where the source data can be found
        self.path_type = keyword_flag # either 'csv' or 'db'     

    def open_source(self, data_source=None, path_type=None):
        '''
        opens a database or csv of [client_id, size, geocodes] as a source 
        for the route sorting method
        path type takes 'csv' or 'db' as keywords
        '''
        if path_type == 'csv':
            csv_source = use_csv(data_source) # open the csv and parse its contents
            hh_dict = csv_source # structured HH data that we need to build a route
        if path_type == 'db':
            pass
        if not path_type:
            print('no data source supplied.')

    def get_status(self, hh_structure=hh_dict):
        '''
        returns the status of the object.  Is it ready to parse points and return a route list?
        '''
        if not hh_structure:
            print('not ready.  please add a data source')
        else:
            print('ready with {}'.format(type(hh_structure)))

    def sort_method(self, households=hh_dict):
        '''
        takes the dictionary created by generate_familes and returns a list of sorted
        households who are in proximity to eachother
        we are just going to brute force this because why the fuck not!
        go through the entire list and create a sorted list of all the distances
        and then skim off as many of the shortest distance HH as will fill up to the
        max # of boxes
        '''
        max_box_count = 8
        route_counter = 1
        routes = {} # labeled routes and the families they contain
        assigned = [] # container to add hh that have been assigned
        #cycle_count = 0
        for applicant in households: # for key in dictionary of households
            h1geocodes = households[applicant].location
            h1_lat, h1_long = households[applicant].location # a tuple of (lat, lng)
            applicant_route = [] # the working container that we will then add to the route dictionary
            size = str(households[applicant].size) # turn the size into a string so we can...
            boxes = box_mask[size] # start building the route with the household
            
            if applicant in assigned:
                #print('we have done this one already')
                pass
            else:
                #print('Applicant {} has not been added to a route yet'.format(applicant))
                # add them to list of assigned HH to avoid adding them again
                assigned.append(applicant) 
                #print('we will add them to the list, which currently has {} households in it'.format(len(assigned)))
                # start by adding the household we are starting with to the container for this route
                applicant_route.append(applicant)                            
                # build a container to add {calculated distances: households} to
                # this will allow us to make a sorted list of the shortest distances and the HH
                # that are at that distance            
                distance_hh_dictionary = defaultdict(list)                                                                                                             
                #print('We will start building a dictionary of distances and the HH that live at them')
                # ITERATE THROUGH THE HOUSEHOLDS AND CALCULATE DISTANCES FROM THE CHOSEN STARTING HH
                for HH in households.keys(): # iterate through the keys to find the distances of remaining households
                    #print('HH under consideration for addition to the route: {}'.format(HH))
                    
                    if HH != applicant: # if this is a new household
                        if HH not in assigned: # and we have not already used them in a route
                            #print('this household has not been added a route')
                            ident = HH # their file number
                            h2_lat, h2_long = households[HH].location # their lat,long
                            #print('they have a geocode of: {} and file ID of {}'.format((h2_lat, h2_long), ident))
                            # caculated the distance between the two households
                            distance_between = haversine(h1_long, h1_lat, h2_long, h2_lat) # returns float distance in KM                        
                            d_key = str(distance_between) # convert to string so we can use it as a dictionary key
                            distance_hh_dictionary[d_key].append(ident) # update dictionary of distances: HH identifier
                            #print('we have added them to the dictionary')
                # now we have calculated all the distances from Route #X A to all of the other households in the caseload
                # sort a list of all the distances so we can skim the shortest off
                distances = sorted([float(k) for k in distance_hh_dictionary.keys()]) 
                #print('we have calculated {} distances'.format(len(distances)))
                
                # NOW WE WILL ITERATE THROUGH THE DISTANCES AND TRY AND PUT A ROUTE TOGETHER
                for float_value in distances: # for distance in sorted listed of distances
                    
                    key = str(float_value) # convert the float to a string so we can use it in the distance : families at that distance dictionary
                    #print('looking at distance {} it contains {}'.format(key, distance_hh_dictionary[key]))
                    # now we need to iterate through the list of HH at this distance.
                    for fam in distance_hh_dictionary[key]: # for the individual or family in the list of households at this distance
                        
                        if fam not in assigned: # if we haven't sorted them into a route yet
                            #print('looking at family {}'.format(fam))
                            fam_size = str(households[fam].size) # determine family size
                            #print('family {} has size {}'.format(fam, fam_size))
                            box_num = box_mask[fam_size] # determine number of boxes
                            #print('they will have {} boxes made up for them'.format(box_num))
                            # do math to determine if we can add them to the route
                            # then if there are still more families at this distance we need to pop the one we just added
                            # and evaluate the next one and so on down the chain until we max out box count per route or exaust
                            # remaining households
                            if box_num + boxes <= max_box_count: # if we added this family to the pile and they didn't add too many boxes
                                #print('adding them to the route will not add too many boxes, so lets add them')
                                boxes += box_num
                                #print('we are currently at {} boxes and have {} spaces remaining'.format(boxes, max_box_count - boxes))
                                assigned.append(fam) # add them to the assigned list
                                applicant_route.append(fam) # add them to the route
            
            if applicant_route:
                print('we have iterated and made a route! It is {}'.format(applicant_route))
                r_key = str(route_counter)
                routes[r_key] = applicant_route
                route_counter += 1
                #cycle_count +=1        
        route_collection = routes
    


test_2018 = Delivery_Routes(None, None)
test_2018.get_status()
test_2018.open_source()


'''
for x in test.keys():
    print('route {} contains {}'.format(x, test[x]))


for x in applicants.keys():
    peeps = str(applicants[x].size)
    print('family {} contains {} people which is {} boxes'.format(x, applicants[x].size, box_mask[peeps]))
'''

''' for k in test.keys():
    total_b = 0
    locations = []
    distances = []
    for fam_ in test[k]:
        size_ = applicants[fam_].size
        local = applicants[fam_].location
        boxes_ = box_mask[str(size_)]
        total_b += boxes_
        locations.append(local)
    print('{} has {} boxes and this range of locations {}'.format(k, total_b, locations))
 '''























