# coding: utf-8

import csv
import geocoder
import time
from collections import namedtuple
import usaddress
import string
import sqlite3
import config	
<<<<<<< HEAD
=======
import logging
>>>>>>> 8788c061940b7eea47d3e358fbd14f25ea07c74a

#api key
myapikey = config.api_key

logging.basicConfig(file_name='address_parse.log',level=logging.INFO)

#containers
Point = namedtuple('Point', 'housenumber street city lat lng county accuracy status') # setup a container type for null returns
null_list = Point('error','error','error','error','error','error','error','error') # an object that will not crash the program if nothing can be coded
ggeo_points = namedtuple('ggeo_points', 'lat, long') # for database retrievals of the geopoints

# address header pointers
add_line = 51 # address header location
city_line = 52 # city header location

# address inserts
province = 'Ontario' # province to append to the string passed to google
country = 'Canada' # coutnry to append to the string passed to google

# client header pointers
client_id = 12
HH_size = 10
diet = 2

# request header pointers THESE ARE ALL WRONG ON THIS SCRIPT
request_id = 1 # CH request ID
req_location = 1 # should return the location that the person applied
food_req = 2 # should return 'Christmas Hamper'
food_req_agency = 3 # should return an agency or sponsor
gift_req = 4 # should return 'Gifts'
gift_req_agency = 5 # should return an agency or sponsor


address_errors = []

# databases
address_db = 'addresses.sqlite'

#files
raw_file_to_process = r'final2017export.csv'
file_to_output = 'xmax caseload to geocode.csv'


# ## Address Parsing

def street_number_parser(number_string):
    '''
    used by the address_builder function to remove '-' from apartment-street number
    formatted addressess i.e. taking the "123-44" in  123-44 Main Street and returning
    the street number 44 only.
    this is important so that as we parse addresses to geocode we cut down on redundant
    api calls for the same address with different apartment numbers
    '''
    updown = string.ascii_uppercase + string.ascii_lowercase # lower and upper case letters
    space = number_string.split(' ') 
    dash = space[-1].split('-')
    out = dash[-1].strip().split(' ')
    return out[-1].strip().strip(updown) # strip white space and any letters i.e. 124B will now = 124

def address_builder(parsed_string):
    '''
    takes the ordered dictionary object (parsed_string) created by the usaddress.tag method 
    and builds a string out of the relevant tagged elements, stripping out the address number
    from the 123-44 Main Street formatted addresses using the street_number_parser() function
    '''
    parse_keys = parsed_string.keys()
    built_string = ''  
    
    if 'AddressNumber' in parse_keys:
        street_number = street_number_parser(parsed_string['AddressNumber'])
        built_string = built_string + street_number + ' '
        #print(built_string)    
    if 'StreetNamePreDirectional' in parse_keys:
        street_number = street_number_parser(parsed_string['StreetNamePreDirectional'])
        built_string = built_string + street_number + ' '
        #print(built_string)
    if 'StreetName' in parse_keys:
        built_string = built_string + parsed_string['StreetName'] + ' '
        #print(built_string)
    if 'StreetNamePostType' in parse_keys:
        built_string = built_string + parsed_string['StreetNamePostType'] + ' '
        #print(built_string)
    if 'StreetNamePostDirectional' in parse_keys:
        built_string = built_string + parsed_string['StreetNamePostDirectional']
        #print(built_string)
    if 'StateName' in parse_keys:
        if 'PlaceName' in parse_keys:
            built_string = built_string + parsed_string['PlaceName'] + ' ' + parsed_string['StateName']
        
    return built_string.strip()  # strip out the leading white space

def full_address_parser(addr):
    '''
    takes a street address e.g. 123 Main Street and attempts to break it into 
    the relevant chunks
    returns a tuple (True, original address, parsed address) or (False, original address, error code)       
    '''
    usaparsed_street_address = namedtuple('usaparsed_street_address','flag original return_value')
    if addr:
        try:
            tagged_address, address_type = usaddress.tag(addr)
            if address_type == 'Street Address':
                # parse the address with the other helper functions
                p_add = address_builder(tagged_address)
                # print('ORIGINAL: {}   PARSED: {}'.format(addr, p_add))
                return usaparsed_street_address(True, addr, p_add)
            else:                
                #print('parsing error 1')
                #print('address_type Error: STRING INPUT WAS {}'.format(addr))
                # log address format error and flag for manual follow up
                return usaparsed_street_address(False, addr, 'address_type Error')
                address_errors.append(addr)
        except usaddress.RepeatedLabelError as e:
            #print('parsing error 2')
            #print('RepeatedLabelError: PARSED: {} ORIGINAL: {}'.format(e.parsed_string, e.original_string))
            # log address format error and flag for manual follow up            
            return usaparsed_street_address(False, addr, 'RepeatedLabelError')
            address_errors.append(addr)
        except KeyError:
            #print('parsing error 3')
            #print('KeyError: {} STRING INPUT WAS {}'.format(address_type, addr))
            # log address format error - could not parse properly
            return usaparsed_street_address(False, addr, 'KeyError')
            address_errors.append(addr)
            
    else:
        #print('parsing error 4')
        return usaparsed_street_address(False, addr, 'Blank Field Error')
        # we can just skip blank lines


# ## Geocoding



def coroutine_decorator(func):
    # supplies the generator with a next so that it can progress to point of taking values
    def wrap(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return wrap

def geolookupfunction(address):
    """
    this function takes an address from the map_address() function and passes it
    to googles geocoder function
    :param address: an address that will be fed into google's api
    :return: [latitude, longditude]
    """    
    if address:
        try:            
            time.sleep(1)
            address_string = str(address)
            print('attempting to ping google api with {}'.format(address_string))
            result = geocoder.google(address_string,key=myapikey)
            if result is not None:
                if result != 'ZERO_RESULTS':
                    print('ping a success with status: {}'.format(result.status))
                    return result
                else:
                    logging.info('ZERO_RESULTS {}'.format(address)
                    return null_list
            else:
                print('no result, returning null_list.  Status is: {}'.format(result.status))
                logging.info('Result is None with {} on {}'.format(result.status, address))
                return null_list
        except Exception as boo:
            logging.info('Exception {} raised'.format(boo))
            return null_list
    else:
        print('Error: there was no address')
        return null_list

# ## Hamper Class and file data parsing

def diet_parser(diet_string):
    '''
    this function returns the string of redundant dietary conditions
    minus the redundant conditions
    '''
    diet = str() # what will be returned
    diet_conditions = set(diet_string.split(',')) # a unique list of conditions split on the commas
    if 'Other' in diet_conditions:
        diet_conditions.remove('Other') # we don't want this because it has zero information
    
    for condition in diet_conditions: # for each issue/preference
        if condition != '': # if it's not blank
            diet = diet + condition + ', ' # add it to the string that will be returned        

    return diet[:-2]  # return the string minus the trailing comma

def street_from_buzz(address_string):
    '''
    helper function for Hamper.  takes an address string and parses out the second line addition from the
    l2f export
    '''
    split_line = address_string.partition(',')
    return split_line[0]

class Hamper(object):
    '''
    a usefull collection of fields from the export and methods to return select points for geocoding and
    writing the data to the output file
    '''
    def __init__(self,a_line):
        #self.app_agency = a_line[req_location] # where they applied
        #self.food = a_line[food_req] # food request
        #self.food_provider = a_line[food_req_agency]
        #self.gift = a_line[gift_req]
        #self.gift_provider = a_line[gift_req_agency]
                
        # list formated points of interest to be written out about hamper request
        self.req_data = a_line
        self.req_id = a_line[request_id]
        self.cl_id = a_line[client_id]
        self.cl_HH = a_line[HH_size]
        self.cl_diet = 'No diet info'
        self.parsed_l2f_address = full_address_parser(street_from_buzz(a_line[add_line])) # (True, original address, parsed address) or ...
                                                                        # (False, original address, error code) in the format ...
                                                                        # 'flag original return_value'
                  
        # list formated points of interest to be written out about client
        self.cl_data = a_line # CH #, ID #, HH size, diet
        self.address = street_from_buzz(a_line[add_line]) # address field i.e. 123 main street 
        self.city = a_line[city_line] # city field        
        self.l2f_address = '{} {}'.format(self.address, self.city)
        self.google_points = ['error', 'error'] # default to error, but set by the g_code() method
        
    def address_to_geocode(self):
        '''
        takes the parsed address and outputs a string with the province and country so that we get the correct
        geopoints.  Sometimes Google will confuse cities/counties with similar street names and output Geopoints
        for the wrong place King Street, Waterloo ON Canada vs. King Street, Waterloo, Wisconson
        '''              
        paddress = self.parsed_l2f_address
        return '{}, {}, Ontario, Canada'.format(paddress.return_value, self.city)

    def out_put(self):
        '''
        This function takes the points from the input file that we want to output with geo points into the output file
        and outputs them as strings that can be written to a csv
        '''
        req_details = self.req_data
        client_details = self.cl_data
        parsed_tuple = self.parsed_l2f_address
        returned_plus_city = [self.address, self.city]
        out_put_string = req_details + client_details + returned_plus_city
        return out_put_string                      

    def coded_in_db(self):
        '''
        this method provides direction for the geocoding pipeline
        to either pass this one by because we have already coded it or have found an error (and log that error) with it
        or to procede with coding it    
        if it has already been coded it takes the lat, long and stores it for later use
        '''
        # conn = sqlite3.connect(address_db) # connect to address database
        # c = conn.cursor() # cursor object
        
        parsed_tuple = self.parsed_l2f_address
        # print('parsed_tuple = {}, {}, {}'.format(parsed_tuple.flag, parsed_tuple.original, parsed_tuple.return_value))
        
        return_val = False # flag to signify if we need to code it or not
        
        # check to see if we can parse down the address
        if parsed_tuple.flag: # flag = True, therefore, we have parsed the address and can procede
            returned_plus_city = "{}, {}".format(parsed_tuple.return_value, self.city)
            
            # see if we have coded it before
            conn = sqlite3.connect(address_db) # connect to address database
            
            c = conn.cursor() # cursor object
            c.execute("SELECT * FROM Coded WHERE o_combined=?",(returned_plus_city,))
            
            id_exists = c.fetchone()
            
            if id_exists:
                print('found database entry - retrieving geopoints')
                # capture the geopoints logged in the database
                l_l = ggeo_points(id_exists[5], id_exists[6])
                self.google_points = [l_l.lat, l_l.long] # set the geopoints so they can be accessed
                
                conn.close()
                return_val = False # yes, we have coded it in the database, so return False
            
            else:
                # see if we have logged the address as an error
                c.execute("SELECT * FROM Errors WHERE o_street=?",(self.address,))
                error_exists = c.fetchone()
                
                if error_exists:
                    #print('found entry in error database')
                    # we have logged this as an error the database
                    conn.close()
                    return_val = False # yes, it is an messed up address and we should not try to code it
                    address_errors.append(self.address)
                else:
                    # no, we have not coded it and the address did not return an error when we tried to parse it, therefore
                    # we should look it up and log the geocodes into the database
                    conn.close()
                    return_val = True
            
        else: # we have returned an error with the address when we tried to parse it
            i_values = [self.cl_id, self.address, self.city, parsed_tuple.return_value]
            
            conn = sqlite3.connect(address_db) # connect to address database
            c = conn.cursor() # cursor object
            c.execute("INSERT INTO Errors (client_id, o_street, o_city, error_string) VALUES (?,?,?,?)",i_values)
            conn.commit()
            conn.close()
            # we have logged it as an error so return False 
            return_val = False            
       
        return return_val
    
    def enter_in_db(self, latitude, long, g_street, g_city):
        '''
        takes latitude and long and enters those values in the database of addresses
        '''
              
        parsed_tuple = self.parsed_l2f_address
        o_street = parsed_tuple.return_value
        returned_plus_city = '{}, {}'.format(parsed_tuple.return_value, self.city)
        o_city = self.city
        o_combined = '{}, {}'.format(o_street, o_city)  
        insert_values = [returned_plus_city, o_street, o_city, g_street, g_city, latitude, long]
                
        conn = sqlite3.connect(address_db) # connect to address database
        c = conn.cursor() # cursor object
        # (o_combined, o_street, o_city, g_street, g_city, lat, long)
        c.execute("INSERT INTO Coded VALUES(?,?,?,?,?,?,?)",insert_values)
        #print('wrote to Coded')
        conn.commit()
        conn.close()
    
    def log_error_in_db(self, g_error_code):
        '''
        when google gets an error back we need to log it in the database to prevent wasted calls to the api
        '''
        i_values = [self.cl_id, self.address, self.city, g_error_code]

        conn = sqlite3.connect(address_db) # connect to address database
        c = conn.cursor() # cursor object
        c.execute("INSERT INTO Errors (client_id, o_street, o_city, error_string) VALUES (?,?,?,?)",i_values)
        #print('wrote to Errors with google api error')
        conn.commit()
        conn.close()
    
    def g_points(self):
        '''
        returns the geocodes extracted from the database
        '''        
        return self.google_points

@coroutine_decorator
def router():
    '''
    takes in a line from a file, parses it, geocodes it, and then writes
    a reformatted line into a file to use or a file of errors
    '''
    over_limit = False
    count_value = 0
    try:
        while True:
            line = yield
            
            visit = Hamper(line) # initialize the Hamper class
            g_flag = visit.coded_in_db() # either True (we need to code it) or False (we have coded it or logged it as an error)
            if g_flag: # if we don't have the points coded already
                
                if not over_limit: # and the limit flag is still False
                    result = geolookupfunction(visit.address_to_geocode()) # geocode the address
                    if result.status == 'OK': # if something good comes back
                        geo_points = [result.lat,result.lng] # capture the lat,long points
                        #print('Result {OK} \nGeo points are: {A} for {B}'.format(OK=result.status,B=result.address,A=geo_points))
                        result_g_city = result.city 
                        result_address = result.address # 123 Main St, City, ON A1A 1A1, Canada
                        result_house = result.housenumber
                        result_street = result.street
                        result_g_street = '{} {}'.format(result_house, result_street)
                        visit.enter_in_db(geo_points[0], # lat
                                          geo_points[1], # long
                                          result_g_street, # 123 Main St
                                          result_g_city # City
                                         ) # log them in dbase                        
                        #print('logged {} in database'.format(result_address))
                        
                        try:
                            data_line = visit.out_put() + [visit.address_to_geocode()] + geo_points # make output line
                            output_line.send(data_line) # write the line into the file
                        except (RuntimeError, TypeError, NameError):
                            print('Error with {} {} {}'.format(visit.out_put(), visit.address_to_geocode(), geo_points))
 
                    elif result.status == 'OVER_QUERY_LIMIT':
                        print('At Query Limit!')
                        over_limit = True # flag us as over limit
                        yet_2_do.send(line) # send to file to parse when our timer has counted down
                        print('line written to file yet to do')
                    else:
                        # write error to database
                        visit.log_error_in_db(result.status) 
                        print('logged google error')
                        error_string = visit.out_put() + [result.status] # capture result status error
                        error_line.send(error_string) # write the line into the error file
                        #print('the following error was logged: \n{}'.format(result.status))
            else: # if we've looked this up already don't ask google again for the info
                #print('entry in database. Attempting to write: \n{}'.format(visit.l2f_address))
                d_string = visit.out_put() + visit.g_points()
                #print(d_string)
                #output_line.send(d_string) # write the line into the file
            count_value += 1
            print(count_value)
            
    except GeneratorExit:
        print('file_write functions are closing')
        output_line.close()
        error_line.close()
        yet_2_do.close()
        print('script complete')
        
@coroutine_decorator
def file_write(filename):
    try:
        with open(filename,'a',newline='') as file:
            w = csv.writer(file)
            while True:
                line = yield
                w.writerow(line)
    except GeneratorExit:
        file.close()
        print('{} file created'.format(file_to_output))


# ## Execution




output_line = file_write(file_to_output)
error_line = file_write('error.csv')
yet_2_do = file_write('still to geocode.csv')
router = router()
try:
    print('starting....')
    with open(raw_file_to_process, newline='') as f:
        address_file = csv.reader(f)
        print('input file open')
        next(address_file, None) # skip headers
        for address in address_file:
            router.send(address)
except Exception as error:
    print('Error opening file:  {}'.format(error))
    raise
router.close()
print('router function closed')
        
for x in address_errors:
    print('{}'.format(x))

print(len(address_errors))






