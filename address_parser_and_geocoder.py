#!/usr/bin/python3.6
'''
this script provides classes and methods to access the 
database of geocoded address data and runs through a 
L2F export attempting to geocode all the addressses.  
It also creates error logs that can be parsed later to find 
bad source address inputs that can be looked up and 
manually corrected in l2f 

it is in need of some major refactoring love
the database should probably become a separate file?
'''

import csv
import geocoder
from collections import namedtuple, defaultdict
import usaddress
import string
import sqlite3
from time import gmtime, strftime, sleep
import logging
import re
import config	# secret api key and file target source 
from db_data_models import Field_Names
from db_data_models import Visit_Line_Object
from db_data_models import Export_File_Parser
from address_audit_tools import parse_post_types
from address_audit_tools import evaluate_post_types
from address_audit_tools import flag_checker
from address_audit_tools import two_city_parser
from address_audit_tools import post_type_logger
from address_audit_tools import two_city_logger
from address_audit_tools import write_to_logs
from address_audit_tools import boundary_checker
from address_audit_tools import boundary_logger
from address_audit_tools import missing_unit_logger

#api key
myapikey = config.api_key

# error logging is handled by functions that carry out parsing and geocoding
# they pass False or None on to the objects using them and are handled by the objects

geocoding_logger = logging.getLogger('geocoder')
geocoding_logger.setLevel(logging.INFO)
#geocoding_log_formatter = logging.Formatter('%(asctime)s:%(filename)s:%(funcName)s:%(name)s:%(message)s')
geocoding_log_formatter = logging.Formatter('%(message)s')
geocoding_log_file_handler = logging.FileHandler(r'Logging/geocoding.log')
geocoding_log_file_handler.setFormatter(geocoding_log_formatter)
geocoding_logger.addHandler(geocoding_log_file_handler)

address_str_parse_logger = logging.getLogger('address_parser')
address_str_parse_logger.setLevel(logging.INFO)
#address_str_parse_log_formatter = logging.Formatter('%(asctime)s:%(filename)s:%(funcName)s:%(name)s:%(message)s')
address_str_parse_log_formatter = logging.Formatter('%(message)s')
address_str_parse_log_file_handler = logging.FileHandler(r'Logging/address_str_parse_functions.log')
geocoding_log_file_handler.setFormatter(address_str_parse_log_formatter)
address_str_parse_logger.addHandler(address_str_parse_log_file_handler)

meta_log = logging.getLogger('meta')
meta_log.setLevel(logging.INFO)
meta_log_format = logging.Formatter('%(asctime)s:,%(message)s')
meta_log_fh = logging.FileHandler(r'Logging/meta.log')
meta_log_fh.setFormatter(meta_log_format)
meta_log.addHandler(meta_log_fh)

## Address Parsing

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
    if not out:
        return number_string
    else:
        return out[-1].strip().strip(updown) # strip white space and any letters i.e. 124B will now = 124

def address_builder(parsed_string):
    '''
    takes the ordered dictionary object (parsed_string) created by the usaddress.tag method 
    and builds a string out of the relevant tagged elements, stripping out the address number
    from the 123-44 Main Street formatted addresses using the street_number_parser() function
    refer to usaddress documentation for the full definitions of all the tags
    the flags var is for helping to flag addresses that have a direction or multiunits
    this is important because it allows us to identify addresses that are missing key
    features for down stream delivery i.e. We are provided 123 Main Street when
    123 Main street could be [...] East or West
    as such, we return a tuple of the parsed address and the flags variable
    '''
    parse_keys = parsed_string.keys()
    built_string = str()  

    flags = {'MultiUnit': False, 'Direction': False, 'PostType': False} # for diffing key aspects of addresses

    if 'AddressNumber' in parse_keys:
        source_value = parsed_string['AddressNumber']
        street_number = street_number_parser(source_value)
        
        if len(str(source_value)) > len(str(street_number)): # if we needed to parse out some extra junk
            flags['MultiUnit'] = True # flag this address as a multiunit building
        
        built_string = '{} {}'.format(built_string, street_number)
        
    if 'StreetNamePreDirectional' in parse_keys: # a direction before a street name e.g. North Waterloo Street
        built_string = '{} {}'.format(built_string, parsed_string['StreetNamePreDirectional'])
        
    if 'StreetName' in parse_keys:
        built_string = '{} {}'.format(built_string, parsed_string['StreetName'])
        
    if 'StreetNamePostType' in parse_keys: # a street type that comes after a street name, e.g. ‘Avenue’
        flags['PostType'] = True
        built_string = '{} {}'.format(built_string, parsed_string['StreetNamePostType'])
        
    if 'StreetNamePostDirectional' in parse_keys: # a direction after a street name, e.g. ‘North’        
        flags['Direction'] = True
        built_string = '{} {}'.format(built_string, parsed_string['StreetNamePostDirectional'])

    if 'PlaceName' in parse_keys: # City
        built_string = '{} {}'.format(built_string, parsed_string['PlaceName'])

    if 'StateName' in parse_keys:
        built_string = '{}, {}'.format(built_string, parsed_string['StateName'])
        if 'PlaceName' in parse_keys: # City
            built_string = '{} {}, {}'.format(built_string, parsed_string['PlaceName'], parsed_string['StateName'])
        
    final_string = built_string.strip()    
    return (final_string, flags)  # strip out the leading white space, return the flags

def scrub_bad_formats_from(address):
    '''
    This function scrubs common garbage inputs from the address string and makes
    minor corrections to improve outcomes further down the pipeline
    '''
    good_address = address.translate({ord('.'): '',
                              ord('#'): '',
                              ord("`"): '',
                              ord(','): ''
                              })
    good_address = re.sub(r'\([^)]*\)', '', good_address)
    bad_formats = [' - ', ' -  ', '- ', ' -', '  - ', '  -  ']
    bad_features = ['-A', '-B','-C', '-D', '-E']
    for bad in bad_formats: # grrr.  I wish I knew who spammed this garbage in the db
        good_address = good_address.replace(bad, '-')
    for feature in bad_features:
        good_address = good_address.replace(feature, '')
    if good_address:
        return good_address
    else:
        return address

def full_address_parser(address, file_id):
    '''
    takes a street address e.g. 123 Main Street and attempts to break it into 
    the relevant chunks
    usaddress.tag() returns a tuple of (OrderedDict, str) with the str being a designator of typex
    :returns: a tuple (True, original address, (parsed address, error_flags)) 
              or (False, original address, error code)       
    '''
    
    addr = scrub_bad_formats_from(address)
    usaparsed_street_address = namedtuple('usaparsed_street_address','flag original return_value')
    if addr:
        try:
            tagged_address, address_type = usaddress.tag(addr)
            if address_type == 'Street Address':
                # parse the address with the other helper functions
                p_add = address_builder(tagged_address) # tuple of (parsed address, flags)
                address_str_parse_logger.info('##80## Parsed {} with result {}'.format(file_id, p_add[0]))
                return usaparsed_street_address(True, addr, p_add)
            else:                
                # log address format error and flag for manual follow up
                address_str_parse_logger.error('##71## Could not derive Street Address from {}'.format(file_id))
                return usaparsed_street_address(False, addr, 'address_type Error')
                
        except usaddress.RepeatedLabelError:
            # log address format error and flag for manual follow up
            address_str_parse_logger.error('##72## RepeatedLabelError from {}'.format(file_id))            
            return usaparsed_street_address(False, addr, 'RepeatedLabelError')
        except KeyError:
            address_str_parse_logger.error('##73## KeyError from {}'.format(file_id))
            return usaparsed_street_address(False, addr, 'KeyError')            
            
    else:
        address_str_parse_logger.error('##74## Blank Field Error from {}'.format(file_id))
        return usaparsed_street_address(False, addr, 'Blank Field Error')
        # we can just skip blank lines

def returnGeocoderResult(address, myapikey, second_chance=True):
    """
    this function takes an address and passes it to googles geocoding
    api with the help of the Geocoder Library.
    it returns a 2 tuple of (True, geocoder object wrapped around the json response) OR
    (False, None) if we are at the free query limit or some major exception 
    happens in the try block
    or (None, None) if there is an error with the api (sometimes it just does
    not work)
    """
    try_again = second_chance    
    try:            
        sleep(1)
        result = geocoder.google(address, key=myapikey)
        print(result.status)
        print(result.lat, result.lng)
        if result is not None:
            if result.status == 'OK':
                geocoding_logger.info('##500## {} is {}'.format(address, result.status))
                return (True, result)
            elif result.status == 'OVER_QUERY_LIMIT':
                geocoding_logger.error('##402## {} yeilded {}'.format(address,result.status))
                return (False, None)
            else:
                geocoding_logger.error('##403## Result is not OK or OVER with {} at {}'.format(result.status, address))
                return (None, None)
        else:
            geocoding_logger.error('##401## Result is None with status {} on {}'.format(result.status, address))
            if try_again:
                print('got None from google api, trying again for {}'.format(address))
                sleep(10) # wait and try again once more after waiting
                returnGeocoderResult(address, myapikey=config, second_chance=False)
            else:
                return (None, None) # tried to see if a second attempt would work, but it didn't
    except Exception as boo:
        geocoding_logger.critical('##400## Try Block in returnGeocoderResult raised Exception {} from {}'.format(boo, address))
        return False

class AddressParser():
    '''
    parses addresses using the address parsing functions
    and provides methods to manage the valid and invalid addresses
    '''

    def __init__(self):
        self.errors = {} # cache of errors on the run throught the active file
        self.parsed = {} # cache of values on the run through the active file

    def parse(self, address, file_id=None):
        '''
        give it an address with extraneous details and it will give you
        a tuple of  ('unit number street', error_flags) or False
        '''
        key_list = list(self.errors.keys()) + list(self.parsed.keys())
        if address not in key_list and address is not (False or None):            
            worked, in_put, out_put  = full_address_parser(address, file_id)
            if worked:                
                self.parsed[in_put] = out_put # tuple of (parsed_address, flags)
                return out_put 
            else:
                self.errors[in_put] = out_put
                return False
        else:
            if address in self.errors:
                return False
            elif address == (False or None):
                return False
            else:
                return self.parsed[address]
    
    def return_simple_address(self, source_address, file_id):
        '''
        this function sidesteps the error checking steps
        and just tries to simplify an address string
        '''
        try:
            _, _, parsed_address = full_address_parser(source_address, file_id)
            simple_address, _ = parsed_address
            return simple_address
        except:
            return None
            
    def is_not_error(self, address):
        # need to insert some database ping here
        if address not in self.errors:
            return True
        else:
            return False

class Coordinates():
    '''
    provides a method for looking up an address via the google api
    and attributes to store information about that process    
    '''
    def __init__(self):
        self.api_key = myapikey
        self.can_proceed = True
        self.calls = 0
        self.coordinates = {}
        self.dict_template = {'Errors': {'Name_Type': False,
                                         'Dir_Type': False, 
                                         'City_Error': False, 
                                         'Eval_Flag': False,
                                         'MultiUnit': False}, 
                              'Address_Strings': set(), 
                              'Households': set()}

    def lookup(self, address):
        '''
        looks up an address and returns a named tuple
        of address string, house_number, street, lat, lng
        using the returnGeocoderResult function
        if the function returns False - we are at the limit
        if the function returns None some non fatal error 
        has been returned by google - better luck next time?
        '''
        address_tpl = namedtuple('address_tpl', 'g_address_str, house_number, street, city, lat, lng')
        if address is None:
            print('blank address provided to Coordinates.lookup() method')
            return address
        
        if address in self.coordinates:
            return self.coordinates[address]
        else:
            if self.can_proceed:
                response, result = returnGeocoderResult(address, self.api_key)
                #print(response, result)
                self.calls += 1
                if response == False: # returnGeocoderResult returns False when limit reached
                    self.can_proceed = False
                    return response
                if response == None:
                    return None
                if response == True and result != None: # Either True or False
                    
                    lat, lng = result.lat, result.lng
                    g_address_str = result.address
                    city = result.city
                    house_number = result.housenumber
                    street = result.street
                    response_tple = address_tpl(g_address_str,
                            house_number,
                            street,
                            city,
                            lat,
                            lng)
                    self.coordinates[address] = response_tple
                    if all(response_tple):

                        return response_tple
                    else:
                        return None
                
            else:
                raise Exception('Over_Query_Limit after {} calls'.format(self.calls))
   
    def add_coordinates(self, lat, lng):
        '''
        adds a coordinate to the tree structure we are using to store data about the coordinate
        '''
        if not self.coordinates[lat][lng]:
            self.coordinates[lat][lng] = self.dict_template
    
    def update_error(self, lat, lng, error_type):
        '''
        update an error in the coordinate tree. Defaults are:
        'Name_Type': False, # ave, street
        'Dir_Type': False,  # north, south
        'City_Error': False, # One address is Kitchener, another Toronto
        'Eval_Flag': False # a mismatch of name or dir type has occured
        '''
        self.coordinates[lat][lng]['Errors'][error_type] = True

    def add_address(self, lat, lng, address):
        '''
        add/update an address in the coordinate tree
        '''       
        self.coordinates[lat][lng]['Address_Strings'].udpate(address)
    
    def add_household(self, lat, lng, household):
        '''
        add a household to the coordinate tree
        '''
        self.coordinates[lat][lng]['Households'].update(household)        
    
    def __str__(self):
        return 'Coordinate Object. can_proceed  = {} and has made {} calls'.format(self.can_proceed, self.calls)

    def __iter__(self):
        for latitude in self.coordinates:
            for lg in self.coordinates[latitude]:
                yield (latitude, lg)

class SQLdatabase():
    
    '''
    Provides methods to create and manage a SQL database of address data

    'address' table collections source address data and matches with lat, lng
    coordinates

    'google_result' are key bits of data returned back about an address from
    google.  These can be though of as the canonical values for a set lat, lng

    'errors' collects source addresses that have some sort of error associated
    with them and a way of flagging potentially bad source addresses and
    bypassing them by fetching correct lat, lng and then pulling out the
    correct address data via a call to the 'google_result' table
    '''

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.name = None
        
    def connect_to(self, name, create = True):
        '''
        establish a connection and cursor and if necessary create the address, error
        and google_result tables
        '''
        if name:
            self.name = name
        try:
            self.conn = sqlite3.connect(name)
            self.cursor = self.conn.cursor()
            if create:
                self.cursor.execute("""CREATE TABLE IF NOT EXISTS address (source_street TEXT,
                                                                         source_city TEXT,
                                                                         lat REAL,
                                                                         lng REAL)""")
                self.conn.commit()
                self.cursor.execute("""CREATE TABLE IF NOT EXISTS google_result 
                                    (lat REAL,
                                     lng REAL,
                                     google_full_str TEXT,
                                     google_house_num TEXT,
                                     google_street TEXT,
                                     google_city TEXT,
                                     unit_flag BOOLEAN,
                                     dir_flag BOOLEAN,
                                     dir TEXT,
                                     post_type BOOLEAN,
                                     post TEXT)""")
                self.conn.commit()
                self.cursor.execute("""CREATE TABLE IF NOT EXISTS errors (source_street TEXT,
                                                                         source_city TEXT,
                                                                         lat REAL,
                                                                         lng REAL,
                                                                         city BOOLEAN,
                                                                         unit_flag BOOLEAN,
                                                                         dir_flag BOOLEAN,
                                                                         post_type BOOLEAN,
                                                                         parse_error BOOLEAN,
                                                                         use_google BOOLEAN)""")
                self.conn.commit()
        except:
            print('error with database connection')
         
    def insert_into_db(self, table, values):
        '''
        inserts values into one of the three tables in the address.db
        'address' 'google_result', 'errors'
        '''

        if not self.name:
            print('establish connection first')
            return False
        if table == 'address':
            self.cursor.execute('INSERT OR IGNORE INTO address VALUES (?,?,?,?)', values)
            self.conn.commit()

        if table == 'google_result':
            self.cursor.execute("""INSERT OR IGNORE INTO google_result VALUES
                                (?,?,?,?,?,?,?,?,?,?,?)""", values)
            self.conn.commit()

        if table == 'errors':
            self.cursor.execute('INSERT OR IGNORE INTO errors VALUES (?,?,?,?,?,?,?,?,?,?)', values)
            self.conn.commit()
        
    def is_in_db(self, parsed_address, source_city):
        '''
        this method checks to see if an address has been logged in the database already.
        First it looks in the address and then the error table
        for reference see:
        https://stackoverflow.com/questions/25387537/sqlite3-operationalerror-near-syntax-error
        sql always kills me on this and it takes me forever to find and recomprehend Martins answer
        :(
        '''

        self.cursor.execute("SELECT * FROM address WHERE source_street=? AND source_city=?",(parsed_address,source_city,))
        db_ping = self.cursor.fetchone()        

        if db_ping:
            return True  
        else:
            self.cursor.execute("SELECT * FROM errors WHERE source_street=? AND source_city=?",(parsed_address,source_city,))
            error_ping = self.cursor.fetchone()
            if error_ping:
                print('found result {} {} in error table'.format(parsed_address, source_city))
                return True
            else:
                return False

    def is_error(self, parsed_address, source_city):
        '''
        returns a boolean value if the given address, city combination has been entered in the 
        error table
        '''

        self.cursor.execute("SELECT * FROM errors WHERE source_street=? AND source_city=?",(parsed_address,source_city,))
        error_ping = self.cursor.fetchone()
        if error_ping:
            return True
        else:
            return False

    def lat_lng_in_db(self, lat, lng):
        '''
        this pings the db and sees if we have logged a google result for the base address yet
        '''
        self.cursor.execute("SELECT * FROM google_result WHERE lat=? AND lng=?",(lat, lng,))
        db_ping = self.cursor.fetchone()
        if db_ping:
            return True
        else:
            return False

    def get_coordinates(self, input_address, input_city):
        '''
        searches the address and errors table and google result table and if it finds an entry it returns the lat, lng
        returns a named tuple with attributes 'lat, lng, source, exists, status, address, city'
        if it finds a result in the address table it bounces back teh source address and source city
        if it finds an error, it looks in the google table and pulls out the google result address and city
        '''
        Coord_package = namedtuple('Coord_package', 'lat, lng, source, exists, status, address, city')
        result = None

        self.cursor.execute("SELECT lat, lng FROM address WHERE source_street=? AND source_city=?",(input_address, input_city,))
        result = self.cursor.fetchone()
              
        if result:
            return Coord_package(*result, 'address', True, 'valid', input_address, input_city)

        else:
            self.cursor.execute("SELECT lat, lng FROM errors WHERE source_street=? AND source_city=?",(input_address, input_city,))
            error_result = self.cursor.fetchone()
            if error_result:
                lt, lg = error_result
                self.cursor.execute("SELECT google_house_num, google_street, google_city FROM google_result WHERE lat=? AND lng=?", (lt,lg,))
                gt_res = self.cursor.fetchone() # google table result
                if gt_res:
                    n, s, c = gt_res
                    add = f'{n} {s}' # 'street_number street'
                    cit = f'{c}' # city
                    return Coord_package(lt, lg, 'errors', True, 'valid', add, cit)
                else:
                    return Coord_package(*error_result, None, True, 'valid', None, None)
            else:
                return Coord_package(None, None, None, False, 'no_results', None, None)

    def in_google_tab(self, lat, lng):
        self.cursor.execute("SELECT lat, lng FROM google_result WHERE lat=? AND lng=?", ((lat, lng,)))
        result = self.cursor.fetchone()
        if result:
            return True
        else:
            return False

    def get_address(self, lat, lng):
        '''
        takes a lat, lng and pulls out the unit number and street from the database as a tuple
        and returns a formatted string combining unit and street
        '''
        self.cursor.execute("SELECT google_house_num, google_street FROM google_result WHERE lat=? AND lng=?", (lat, lng,))
        result = self.cursor.fetchone()
        if result:
            return '{} {}'.format(result[0], result[1])
        else:
            return False

    def in_address_table(self, input_address, input_city):
        '''
        returns the values in the address table or False if none are present.
        Takes a parsed address and input City as parameters
        '''
        self.cursor.execute("SELECT * FROM address WHERE source_street=? AND source_city=?",(input_address, input_city,))
        db_ping = self.cursor.fetchone()
        if db_ping:
            return db_ping
        else:
            return False

    def pull_flags_at(self, lat, lng, table='google_result'):
        '''
        returns one source addresses and their flags at a given lat,lng
        this will allow us to identify partial addresses to follow up on
        returns: boolean values in a 4 named tuple (valid, errors, unit_flag, dir_flag, post_type)
        '''
        Flag_pack = namedtuple('Flag_pack', 'valid, error_free, unit_flag, dir_flag, post_type')
        self.cursor.execute(f"SELECT unit_flag, dir_flag, post_type FROM {table} WHERE lat=? AND lng =?",(lat,lng,))
        flag_query = self.cursor.fetchone()
        if flag_query:
            if any(flag_query):
                if not all(flag_query):
                    return Flag_pack(True, True, *flag_query)
                else:
                    return Flag_pack(True, False, *flag_query)      
        else:
            return Flag_pack(False, None, False, False, False)
        


    def set_unit_flag_in_db(self, lat, lng):
        '''
        updates the unit flag at lat, lng as True in table google_result
        '''
        # http://www.sqlitetutorial.net/sqlite-python/update/
        values = (True, lat, lng)
        self.cursor.execute('UPDATE google_result SET unit_flag=? WHERE lat=? AND lng=?', values)
        self.conn.commit()
        
    def close_db(self):
        '''
        closes the database
        '''
        self.conn.close()

class Source_Address():
    '''
    Aggregate the flags and methods related to parsing address strings,
    and finding geocodes or database values
    '''

    def __init__(self, address_string, city_string, applicant_ID):
        self.source_string = address_string 
        self.city = city_string
        self.applicant = applicant_ID
        self.decon_address = None # tuple result of address_parser.parse()
        self.flags = None # extracted from .decon_address[1]
        self.flagged_unit = None # relevant for the db - should we update the db with a unit flag? 
        self.in_bounds = None # boundary_checker()
        self.simplified_address = None # .decon_address[0]
        self.source_post_types = None # ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)
        self.s_evf = None
        self.error_dictionary = {'source_address_error': False, # could not parse source address !valid decon_address
                                 'post_parse_evf': False, # it says st. when it should be dr. etc. 
                                 'boundary_error': False, # invalid City
                                 'no_address_supplied' True # No address input
                                    }
        self.go_to_next_step = True  # a T|F flag for being error free
        

    def deconstruct_view(self, address_parser):
        '''
        cuts the address into bits and does some basic checks
        does the address seem valid 
        i.e. can we parse it?  
        Is the city in bounds?
        Is it a multiunit address?

        It sets all the values in .error_dictionary()
        '''
        self.decon_address = address_parser.parse(self.source_string, self.applicant)
        self.in_bounds = boundary_checker(self.city)
        
        try:
            self.simplified_address, self.flags = self.decon_address
            # flags = {'MultiUnit': False, 'Direction': False, 'PostType': False}   
        except:
            self.error_dictionary['source_address_error'] = True
            # print('address error for {}. check logs for {}'.format(self.applicant, self.source_string))

        if not self.in_bounds:
            # boundary_logger(self.applicant, self.city)
            self.error_dictionary['boundary_error'] = True
        if self.decon_address is not False and self.in_bounds == True:
            self.flagged_unit = self.flags['MultiUnit'] # True or False if it is a multi unit building
            self.source_post_types = parse_post_types(self.simplified_address)
            # ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)
            _, _, self.s_evf = self.source_post_types
            # is there a mismatch in the pt (st, dr etc) or dir (N,S,E,W) keys?  
            # An error or an outlier? Flag for followup
            if self.s_evf:
                self.error_dictionary['post_parse_evf'] = True # post type eval flag was set
                # write_to_logs(self.applicant, self.city, 'post_parse')  
 
    def return_error_flags(self):
        '''
        returns the dictionary of accumulated error flags for use in making
        decisions about logging
        '''
        return self.error_dictionary

    def return_go_status(self):
        '''
        Does a check of the errors and returns False if errors
        exist, or True, if they do not
        This is used in logic to procede with geocoding
        '''
        if self.source_string == None:
            # no address was input
            self.go_to_next_step = False
            self.error_dictionary['no_address_supplied'] = True
        if not self.decon_address:
            # the deconstruct_view method has not run, or had errors
            self.go_to_next_step = False
        if any(self.error_dictionary.values()):
            # is there at lease one error?
            self.go_to_next_step = False
        
        return self.go_to_next_step

class Database_View():
    '''
    Looks into the database and provides methods to 
    write, retrieve and interrogate the database
    '''
    def __init__(self, db):
        self.db = db

    def in_db(self, address, city, bool_flag=False):
        '''
        wraps around the call to see if a given address, city
        has previously been coded
        bool_flag = True if a True in db or False not in db is desired
        returns True or False if bool_flag is set True
        else returns tuple (lat, lng, source_table) 
        or False if no results
        '''
        Fail_package = namedtuple('Coord_package', 'lat, lng, source, exists, status, address, city')

        try: # look for database result
            dbr = self.db.get_coordinates(address, city)
            # a named tuple
            if bool_flag: # if we only want to know if it's been logged
                return dbr.exists
            else: # if we want to know lat, lng and source table
                if dbr.exists:
                    return dbr
                else:
                    print('address is not in database')
                    return dbr # (None, None, None, False, 'no_results')
        except:
            return Fail_package(None, None, None, False, 'failed', None, None)
    
    def google_tab_entry(self, lat, lng):
        return self.db.in_google_tab(lat, lng) # T | F

    def extract_flags(self, lat, lng):
        gf = self.db.pull_flags_at(lat, lng, 'google_result')
        ef = self.db.pull_flags_at(lat, lng, 'errors')
        if gf.valid:
            return gf
        else:
            return ef


    def write_db(self, input_tuple, table):
        '''
        take an input tuple
        input_tuple = (address, city, lat, lng)
        and write it to a table
        'address'
        'errors'
        'google_result'
        '''
        print(f'attempting to write {input_tuple} to db in the {table} table')
        try:
            self.db.insert_into_db(table, input_tuple)

        except:
            print(f'could not write {input_tuple} to dbase table {table}')

    def set_unit_flag(self, lat, lng):
        '''
        wraps the call to set the unit flag True in the database address table
        '''
        self.db.set_unit_flag_in_db(lat, lng)    

class Geocode_View():
    '''
    Provides methods to pass an address in and return geocodes either from 
    google or the database and a way to interrogate flags
    '''
    def __init__(self, input_a, input_c, c_m):
        self.input_address = input_a
        self.input_city = input_c
        self.c_m = c_m
        self.address_for_api = f'{input_a} {input_c} Ontario, Canada'
        self.coding_result = None # named tuple 'g_address_str,house_number,street,city,lat,lng'
        self.lat = None
        self.lng = None
        self.flags = {'at_limit': False, 'no_result': False, 'no_errors': False, 'gc_failed': True} 

    def gc_address(self):
        '''
        try and geocode the address
        address, city are input when the object is instantiated
        c_m is the coordinate_manager object that is passed in to be
        run with the address, city as inputs
        sets error flags as appropriate and lat, lng values
        '''
        #print(f'trying to geocode {self.input_address} {self.input_city}')
        try: 
            self.coding_result = self.c_m.lookup(self.address_for_api)
            self.flags['gc_failed'] = False
            self.flags['no_result'] = True
        except:
            print('geocoding attempt failed. Geocode_View.gc_address() failed')

        if self.coding_result == None:                    
            self.flags['no_result'] = True
        if self.coding_result == False:
            # we are at the limit - need to cool down
            self.flags['at_limit'] = True
        if self.coding_result:
            # named tuple
            #print(f'got {self.coding_result.lat} {self.coding_result.lng}')
            self.lat = self.coding_result.lat
            self.lng = self.coding_result.lng
            if not any(self.flags.values()):
                self.flags['no_errors'] = True
    
    def return_result(self):
        '''
        returns a package of values that can be used to make decisions
        '''
        Package = namedtuple('Package', 'status, lat, lng')
        if self.flags.get('no_errors', False):
            return Package('no_errors', self.lat, self.lng)
        if self.flags.get('at_limit', False):
            return Package('at_limit', None, None)
        if self.flags.get('no_result', False):
            return Package('no_result', None, None)
        if self.flags.get('gc_failed', False):
            return Package('null', None, None) # no coding attempt has been done

class line_obj_parser():
    '''
    I am a big dumb monster
    this object wraps the process of looking at an address
    parsing it, evaluating it, coding it and logging its bits into a db

    the VLO is the model
    the address object is the view
    this is the controller?
    '''    
    def __init__(self, line, fnamedict, dbase, co_d_m):
        self.line_object = Visit_Line_Object(line, fnamedict)
        self.flags = {} # aggregate the error flags through each processing step
        self.flagged_unit = False # delete after calling to self.SAO.flagged_unit
        self.coordinate_man = co_d_m
        self.decon_address = None
        self.simplified_address = None
        self.applicant = None
        self.address = None
        self.city = None
        self.google_post_types = None     
        self.post_type_tp = None
        self.coding_result = None
        self.cities_valid_and_matching = False
        self.can_proceed_to_gc = False # should try and geocode?                    
        self.pt_eval_errors = None # named tuple (status: 'valid'|'failed', error_free: T|F, sn_error, dt_error, fl_error)
        self.done_b4 = False
        self.in_table = None # returned from poll_db
        self.db_flags = None # flags extracted from database via .DBV.extract_flags() 'valid, unit_flag, dir_flag, post_type'
        self.lat = None
        self.lng = None
        self.should_write = False # should write to address table
        self.should_write_g = False # should write to google table
        self.should_write_uf = False # set unit flag in database
        self.should_write_et = False # write to error table
        self.google_pinged = False # did we hit google
        self.google_db_entry = False # is there an existing entry in the db?
        self.google_h_street = None # what is the 'house number street' from google i.e. 123 Main St
        self.google_city = None
        self.SAO = None # Source Address Object View
        self.GOO = None
        # SETUP OBJECTS
        self.DBV = Database_View(dbase)
        self.address, self.city, self.applicant = self.line_object.get_add_city_app()
        self.SAO = Source_Address(self.address, self.city, self.applicant)

    def __str__(self):
        return f"""source address: {self.address} source city: {self.city} applicant: {self.applicant}\nflags: {self.flags}\nIn error or address: {self.done_b4} In google_result: {self.google_db_entry}\n"""

    def deconstruct(self, add_parser):
        '''
        pulls the address appart and tries to 
        simplify the address, 
        check if the city is valid,
        look at the post type labels,
        check if the source is a valid input 
        '''  
        print('SA loaded with {} {} {}'.format(self.address, self.city, self.applicant))
        self.SAO.deconstruct_view(add_parser)
        
        self.flags.update(self.SAO.return_error_flags())
        #print('flags = {}'.format(self.flags))
        
        self.can_proceed_to_gc = self.SAO.return_go_status() # True if no errors or False if at least 1

        print('OK to do GC attempt: {}'.format(self.can_proceed_to_gc))
        if self.can_proceed_to_gc:
            self.simplified_address = self.SAO.simplified_address            
            #print(f'parsed and moving forward with {self.simplified_address}')

    def poll_db(self):
        '''
        see if there is a db value and if so get lat, lng as
        well as the source table
        if there is a db entry set values and return True
        else: return False
        '''
        # database view named tuple from .in_db call
        print(f'polling database with {self.simplified_address} {self.city}')
        dbv_nt = self.DBV.in_db(self.simplified_address, self.city, bool_flag=False)
        self.db_flags = self.DBV.extract_flags(self.lat, self.lng)
        
        if dbv_nt.status == 'valid':
            print(dbv_nt)
            if dbv_nt.exists:
                self.lat = dbv_nt.lat
                self.lng = dbv_nt.lng 
                self.done_b4 = True
                if dbv_nt.source == 'address':
                    self.cities_valid_and_matching = True
                else: # if an error
                    if dbv_nt.address: # but a valid google entry exists
                        self.google_h_street = dbv_nt.address
                        self.google_city = dbv_nt.city
                        self.cities_valid_and_matching = two_city_parser(self.city, self.google_city)[0]
                self.in_table = dbv_nt.source
                print('in database')
                return True
            else:
                print('not in database')
                return False
        else:
            self.done_b4 = False
            self.can_proceed_to_gc = True
            print('not in database')
            return False          

    def try_gc_api(self):
        '''
        if the .can_proceed_to_gc value has been set
		attempt to geocode an address  with the values stored
		in the Source Address object view
		
        '''
        print(f'trying to geocode {self.simplified_address} {self.city}')
        if self.can_proceed_to_gc:
            self.GOO = Geocode_View(self.simplified_address, self.city, self.coordinate_man)
            self.GOO.gc_address()
            self.flags.update(self.GOO.flags)
            self.lat = self.GOO.lat
            self.lng = self.GOO.lng
            self.should_write_g = True # we have a gc result, so we should write it to db
            self.google_pinged = True
            self.google_db_entry = self.DBV.google_tab_entry(self.lat, self.lng) # see if there is a database result somehow
            return True
        else:
            print('cannot proceed to geocoding step - flag not set True')
            self.flags.update(self.GOO.flags)
            return False

    def diff_results(self):
        '''
        compare database/google results with source address
        set a flag that provides clear guidance re: db write
        '''
        # IS DATABASE MISSING A UNIT FLAG  
        

        # returns 'valid, unit_flag, dir_flag, post_type'
        if self.SAO.flagged_unit and self.done_b4: # if the input string has a unit number and we have already coded it
             # and use that to get the unit flag from database
            if self.db_flags.valid and not self.db_flags.unit_flag: # if the input string has one, but the database does not, we need to update the database
                print(f'unit flag was missing from {self.address} in the database.')
                self.should_write_uf = True
        
                #dbase.set_unit_flag_in_db(lat, lng)
        if not self.SAO.flagged_unit and self.db_flags.unit_flag:
            self.flags['missing_unit_num'] = True

        # DIFF GOOGLE RESULT WITH SOURCE ADDRESS
        if self.google_pinged:
            self.flags['geocode_attempt'] = True              
            g_city = self.GOO.coding_result.city
            google_address = f'{self.GOO.coding_result.house_number} {self.GOO.coding_result.street}'
            self.google_h_street = google_address
            self.google_post_types = parse_post_types(google_address) # GOOGLE PT
            # evaluate source vs. google post types
            self.pt_eval_errors = evaluate_post_types(self.SAO.source_post_types, self.google_post_types)
            # returns named tuple 'status, error_free, sn_error, dt_error, fl_error')
            # ('valid'|'failed', T|F, T|F, T|F, T|F)
            if self.pt_eval_errors.status == 'failed':
                self.flags['pt_eval'] = 'failed'

            self.cities_valid_and_matching = two_city_parser(self.city, g_city)[0] # True or False
        else:
            print('no flags to set.  Google was not pinged')
            self.flags['geocode_attempt'] = False
            if self.google_city and self.google_h_street:
                self.google_post_types = parse_post_types(self.google_h_street) # GOOGLE PT
                self.pt_eval_errors = evaluate_post_types(self.SAO.source_post_types, self.google_post_types)
            else:
                null_tuple = namedtuple('null_tuple', 'status, error_free, sn_error, dt_error, fl_error')
                self.pt_eval_errors = null_tuple('null', False, False, False, False)

        # SET FLAGS TO GUIDE DB WRITE LOGIC
        
        if not self.done_b4:
            # do the cities match, + were there any errors in street type, direction or parse errors
            go_states = (self.cities_valid_and_matching, self.pt_eval_errors.error_free)
            if all(go_states):
                self.should_write = True
            else:
                if not self.in_table:
                    self.should_write_et = True        

    def attempt_db_write(self):
        '''
        review flags and write the values to the db tables that are appropriate

        LOGIC:
        do we need to set a unit flag?
            set unit flag in google result table
        do we have an address but no entry in the address table?
            write to the address table?
        do we have a google result but no entry in the google table?
            write to the google table
        do we have a google result, but there are errors?  
            write in the error table

        '''
        #print('attempting to write to db')
        base_tuple = (self.simplified_address, 
                      self.city, 
                      self.lat,
                      self.lng)
        #print(f'base tuple is {base_tuple}')
        wf = (self.should_write_uf,
              self.should_write,
              self.should_write_g,
              self.should_write_et)

       # print('Write unit flag: {} Write address: {} Write google: {} Write errors: {}'.format(*wf))
       # print(f'should write {sum(wf)} times')


        if self.should_write_uf: # if the diff says to set unit flag in db b/c it is missing
            self.DBV.set_unit_flag(self.lat, self.lng)            
        
        if self.should_write: # not in database already - no errors
            self.DBV.write_db(base_tuple, 'address')            

        # ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)
        if self.should_write_g and not self.google_db_entry: # flag set when a valid gc result returns in the try_gc_api method
            g_dir_type_tp, g_post_type_tp, _ = self.google_post_types     
            flagged_dir, dir_str = g_dir_type_tp # True/False, None or first letter of dir_type
            flagged_post_type, pt_str = g_post_type_tp # True/False, None or first letter of post_type 
            #print(f'coding result = {self.GOO.coding_result}')                        
            google_result = (self.GOO.lat, 
                    self.GOO.lng,
                    self.GOO.coding_result.g_address_str, # full string with Prov, Pcode et al.
                    self.GOO.coding_result.house_number,
                    self.GOO.coding_result.street,
                    self.GOO.coding_result.city,
                    self.SAO.flagged_unit, # did we identify the building has units?
                    flagged_dir, # NSEW? T | F
                    dir_str, # something from [N, S, E, W]?
                    flagged_post_type, # street, drive etc. T | F
                    pt_str # s, d etc. 
                    )
            print(f'google result = {google_result}')
            self.DBV.write_db(google_result, 'google_result')            
            
        if self.should_write_et: # set in the diff_results method
            city_error = False # cities don't match
            dir_error = False # N vs S
            post_error = False # St vs Ave
            parse_error = False # issues with the usa address tags
            use_google = False # in production overwrite source with google?
            if not self.cities_valid_and_matching:
                city_error = True
            if not self.pt_eval_errors.error_free:
                # pt_eval is a named tuple
                # 'status, error_free, sn_error, dt_error, fl_error'
                post_error = self.pt_eval_errors.sn_error
                dir_error = self.pt_eval_errors.dt_error
                parse_error = self.pt_eval_errors.fl_error
                unit_flag = self.should_write_uf

            e_tpl = (*base_tuple, city_error, unit_flag, dir_error, post_error, parse_error, use_google)
            self.DBV.write_db(e_tpl, 'errors')
    
    def log_results(self, meta_errors):
        '''
        This method reviews the various error flags and makes decisions on what
        to log.

        the meta log is where significant errors are consolidated so that it is
        easier to review source addresses and identify which ones may need to
        be reviewed and corrected.

        param meta_errors is a dictionary passed in of errors from failures in 
        the call stack of line object parser calls

        '''
        who = self.line_object.main_applicant_ID
        where = f'{self.address} {self.city}'
        parse_failed = meta_errors['d_parse']
        write_failed = meta_errors['dbase_write']
        # FIRST ORDER ERRORS with basic I/O        
        if parse_failed:
            meta_log.info(f'1,{who},at,{where},could not be parsed as input')
        if write_failed:
            meta_log.info(f'1,{who},at,{where},failed .db_attempt_write() call')
        if self.flags.get('source_address_error', False):
            meta_log.info(f'1,{who},at,{where},could not be deconstructed into a simplier address')

        # SECOND ORDER ISSUES with source inputs after some basic parsing
        if self.flags.get('post_parse_evf', False):
            meta_log.info(f'2,{who},at,{where},may have some address format/content issues')
        if self.flags.get('boundary_error', False):
            meta_log.info(f'2,{who},at,{where},has an invalid city as input')

        # THIRD ORDER ISSUES could not get a google result or could not compare source vs. database/google
        #if self.flags.get('no_result', False) and not self.done_b4:
        #    meta_log.info(f'3,{who},at,{where},returned no google result,Flags are as follows: {self.flags}')
        if self.flags.get('pt_eval',False):
            meta_log.info(f'3,{who},at,{where},failed attempt to compare with google or database, Flags are as follows: {self.flags}')
        
        # FOURTH ORDER ISSUES we found issues with the source when matched against google or database values
        # refernce is self.pt_eval_errors (status: 'valid'|'failed', error_free: T|F, sn_error, dt_error, fl_error)
        c_com = self.cities_valid_and_matching == True     
        c2 = self.db_flags.error_free == True or None
        c3 = self.pt_eval_errors.error_free == True     
        #print(f'pt_eval_errors: {self.pt_eval_errors} db_derived flags {self.db_flags}')
        dat_b = self.db_flags
        goo_p = self.pt_eval_errors
        
        if not all((c_com, c2, c3)):
            e = sum(((c_com != True),dat_b.unit_flag, (dat_b.dir_flag or goo_p.dt_error),(dat_b.post_type or goo_p.sn_error)))
            #print(c_com,dat_b.unit_flag, (dat_b.dir_flag or goo_p.dt_error),(dat_b.post_type or goo_p.sn_error)) 
            #print(f'write {e} times for {(c_com, c2, c3)}')
            if e > 0:
                meta_log.info(f'4,{who},{where},has,{e} error(s),City mismatch,{c_com != True },Missing Unit Number,{dat_b.unit_flag},Direction Error,{dat_b.dir_flag or goo_p.dt_error},Street Type Error,{dat_b.post_type or goo_p.sn_error}') 
        
        # SPECIAL CASES
        if self.flags.get('at_limit', False):
            meta_log.info(f'X,{who},at,{where},hit the google api wall')      

if __name__ == '__main__':
    coordinate_manager = Coordinates() # I lookup and manage coordinate data
    address_parser = AddressParser() # I strip out extraneous junk from address strings
    dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
    dbase.connect_to('Address.db', create=True) # testing = atest.db
    
    fnames = Field_Names(config.target) # I am header names
    fnames.init_index_dict() 
    export_file = Export_File_Parser(config.target, fnames) # I open a csv 
    # for testing use test_export.csv
    export_file.open_file()
    
    for line in export_file: # I am a csv object
        error_stack = {'d_parse': False, 'dbase_write': False}
        lop = line_obj_parser(line, fnames.ID, dbase, coordinate_manager) #.ID 
        try: # parse address
            lop.deconstruct(address_parser)
        except:
            print('could not successfully call deconstruct method')
            error_stack['d_parse'] = True  
        if not lop.poll_db(): # poll db - set lat,lng or...
            lop.try_gc_api() # attempt to geocode
        lop.diff_results() # compare source + db as well as source + google - set error flags
        try:
            lop.attempt_db_write()
        except:
            print('could not write to db.')
            error_stack['dbase_write'] = True

        lop.log_results(error_stack)
        print('############')

    dbase.close_db()
    print('proccess complete on source file {}'.format(config.target))
