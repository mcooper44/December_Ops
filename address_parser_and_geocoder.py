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
        if address not in key_list and address is not False:            
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
            elif address == False:
                return address
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
        address_tpl = namedtuple('address_tpl', 'g_address_str,house_number,street,city,lat,lng')
        if address in self.coordinates:
            return self.coordinates[address]
        else:
            if self.can_proceed:
                response, result = returnGeocoderResult(address, self.api_key)
                print(response, result)
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
                self.cursor.execute("""CREATE TABLE IF NOT EXISTS address (source_street text,
                                                                         source_city text,
                                                                         lat real,
                                                                         lng real)""")
                self.conn.commit()
                self.cursor.execute("""CREATE TABLE IF NOT EXISTS google_result 
                                    (lat real,
                                     lng real,
                                     google_full_str text,
                                     google_house_num text,
                                     google_street text,
                                     google_city text,
                                     unit_flag boolean,
                                     dir_flag boolean,
                                     dir text,
                                     post_type boolean,
                                     post text)""")
                self.conn.commit()
                self.cursor.execute("""CREATE TABLE IF NOT EXISTS errors (source_street text,
                                                                         source_city text,
                                                                         lat real,
                                                                         lng real)""")
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
            self.cursor.execute('INSERT INTO address VALUES (?,?,?,?)', values)
            self.conn.commit()
        if table == 'google_result':
            self.cursor.execute("""INSERT OR IGNORE INTO google_result VALUES
                                (?,?,?,?,?,?,?,?,?,?,?)""", values)
            self.conn.commit()
        if table == 'errors':
            self.cursor.execute('INSERT INTO errors VALUES (?,?,?,?)', values)
            self.conn.commit()
        
    def is_in_db(self, parsed_address, source_city):
        '''
        this method checks to see if an address has been logged in the database already.
        First it looks in the address and then the error table
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

    def get_coordinates(self, input_address, input_city, give_source=False):
        '''
        searches the address and then errors table and if it finds an entry it returns the lat, lng
        returns a named tuple with attributes 'lat, lng, source, exists'
        '''
        Coord_package = namedtuple('Coord_package', 'lat, lng, source, exists')
        self.cursor.execute("SELECT lat, lng FROM address WHERE source_street=? AND source_city=?",(input_address, input_city,))
        result = self.cursor.fetchone()
        if result:
            if give_source:
                return Coord_package(*result, 'address', True)
            else:
                return result
        else:
            self.cursor.execute("SELECT lat, lng FROM errors WHERE source_street=? AND source_city=?",(input_address, input_city,))
            error_result = self.cursor.fetchone()
            if error_result:
                if give_source:
                    return Coord_package(*error_result, 'errors', True)
                else:
                    return error_result
            else:
                return (False, False)

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

    def pull_flags_at(self, lat, lng):
        '''
        returns one source addresses and their flags at a given lat,lng
        this will allow us to identify partial addresses to follow up on
        returns: boolean values in a 4 named tuple (valid, unit_flag, dir_flag, post_type)
        '''
        Flag_pack = namedtuple('Flag_pack', 'valid, unit_flag, dir_flag, post_type')
        self.cursor.execute("SELECT unit_flag, dir_flag, post_type FROM google_result WHERE lat=? AND lng =?",(lat,lng,))
        flag_query = self.cursor.fetchone()
        if flag_query:
            return Flag_pack(True, *flag_query)
        else:
            return Flag_pack(False, False, False, False)

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
        self.in_bounds = None # boundary_checker()
        self.decon_address = None # tuple result of address_parser.parse()
        self.simplified_address = None # .decon_address[0]
        self.flags = None # extracted from .decon_address[1]
        self.flagged_unit = None # relevant for the db - should we update the db with a unit flag?
        self.source_post_types = None # ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)
        self.s_evf = None
        self.error_dictionary = {'source_address_error': False, # could not parse source address !valid decon_address
                                 'post_parse_evf': False, # it says st. when it should be dr. etc. 
                                 'boundary_error': False, # invalid City  
                                    }
        self.go_to_next_step = True

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
        return self.error_dictionary

    def return_go_status(self):
        '''
        Does a check of the errors and returns False if errors
        exist, or True, if they do not
        '''
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

    def in_db(self, address, city, bool_flag=True):
        '''
        wraps around the call to see if a given address, city
        has previously been coded
        bool_flag = True if a True in db or False not in db is desired
        returns True or False if bool_flag is set True
        else returns tuple (lat, lng, source_table) 
        or False if no results
        '''
        try: # look for database result
            dbr = self.db.get_coordinates(address, city, give_source=True)
            # a named tuple
            if bool_flag: # if we only want to know if it's been logged
                return dbr.exists
            else: # if we want to know lat, lng and source table
                if dbr.exists:
                    return (dbr.lat, dbr.lng, dbr.source)
                else:
                    print('there is an error')
                    return (False, False, False)
        except:
            return False

    def extract_flags(self, lat, lng):
        return self.db.pull_flags_at(lat, lng)

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
            return True
        except:
            print('could not write to dbase')
            return False

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
    def __init_pass(self, input_a, input_c, c_m):
        self.input_address = input_a
        self.input_city = input_c
        self.c_m = c_m
        self.address_for_api = f'{input_a} {input_c} Ontario, Canada'
        self.coding_result = None
        self.lat = None
        self.lng = None
        self.flags = {'at_limit': False, 'no_result': False, 'no_errors': False, 'no_attempt': True} 

    def gc_address(self):
        '''
        try and geocode the address
        address, city are input when the object is instantiated
        c_m is the coordinate_manager object that is passed in to be
        run with the address, city as inputs
        sets error flags as appropriate and lat, lng values
        '''
        print(f'trying to geocode {self.input_address} {self.input_city}')
         
        self.coding_result = self.c_m.lookup(self.address_for_api)
        self.flags['no_attempt'] = False

        if self.coding_result == None:                    
            self.flags['no_result'] = True
        if self.coding_result == False:
            # we are at the limit - need to cool down
            self.flags['at_limit'] = True
        if self.coding_result:
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
        if self.flags.get('no_attempt', False):
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
        self.gc_attempt = False
        self.cities_valid_and_matching = False
        self.can_proceed_to_gc = False # should try and geocode?
        self.rejected = False                       
        self.google_result = False
        self.pt_eval_errors = None # named tuple (status: 'valid'|'failed', error_free: T|F, sn_error, dt_error, fl_error)
        self.done_b4 = False
        self.in_table = None # returned from poll_db
        self.db_flags = None
        self.lat = None
        self.lng = None
        self.should_write = False # should write to address table
        self.should_write_g = False # should write to google table
        self.should_write_uf = False # set unit flag in database
        self.should_write_et = False # write to error table
        self.SAO = None # Source Address Object View
        self.GOO = None
        # SETUP OBJECTS
        self.DBV = Database_View(dbase)
        self.address, self.city, self.applicant = self.line_object.get_add_city_app()
        self.SAO = Source_Address(self.address, self.city, self.applicant)
        
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
        print('flags = {}'.format(self.flags))
        
        self.can_proceed_to_gc = self.SAO.return_go_status() # True if no errors or False if at least 1

        print('OK to do GC attempt: {}'.format(self.can_proceed_to_gc))
        if self.can_proceed_to_gc:
            self.simplified_address = self.SAO.simplified_address
            
            print(f'parsed and moving forward with {self.simplified_address}')

    def poll_db(self):
        '''
        see if there is a db value and if so get lat, lng as
        well as the source table
        if there is a db entry set values and return True
        else: return False
        '''
        # database view named tuple from .in_db call
        dbv_nt = self.DBV.in_db(self.address, self.city, bool_flag=False)
        if dbv_nt:
            if dbv_nt.exists:
                self.lat = dbv_nt.lat
                self.lng = dbv_nt.lng 
                self.done_b4 = True
                self.in_table = dbv_nt.source
                return True
        else:
            return False

    def try_gc_api(self):
        '''
        if the .can_proceed_to_gc value has been set
		attempt to geocode an address  with the values stored
		in the Source Address object view
		
        '''
        print(f'trying to geocode {self.simplified_address} {self.city}')
        if self.can_proceed_to_gc:
            self.GOO = Geocode_View(self.SAO.simplified_address, self.city, self.coordinate_man)
            self.GOO.gc_address()
            self.flags.update(self.GOO.flags)
            return True
        else:
            print('cannot proceed to geocoding step - flag not set True')
            return False

    def diff_results(self):
        '''
        compare database/google results with source address
        set a flag that provides clear guidance re: db write
        '''
        # IS DATABASE MISSING A UNIT FLAG   
        if self.SAO.flagged_unit and self.done_b4: # if the input string has a unit number and we have already coded it
            self.db_flags = self.DBV.extract_flags(self.lat, self.lng) # and use that to get the unit flag from database
            if self.db_flags.valid and not self.db_flags.unit_flag: # if the input string has one, but the database does not, we need to update the database
                print(f'unit flag was missing from {self.address} in the database.')
                self.should_write_uf = True
                #dbase.set_unit_flag_in_db(lat, lng)
        # DIFF GOOGLE RESULT WITH SOURCE ADDRESS
        if self.GOO.coding_result:
            self.flags['geocode_attempt'] = True              
            g_city = self.GOO.coding_result.city
            google_address = f'{self.GOO.coding_result.house_number} {self.GOO.coding_result.street}'
            self.google_post_types = parse_post_types(google_address) # GOOGLE PT
            # evaluate source vs. google post types
            self.pt_eval_errors = evaluate_post_types(self.SAO.source_post_types, self.google_post_types)
            if self.pt_eval_errors.status == 'failed':
                self.flags['pt_eval'] = 'failed'

            self.cities_valid_and_matching = two_city_parser(self.city, g_city) # True or False
        else:
            print('no flags to set.  There is no coding result')
            self.flags['geocode_attempt'] = False
        # SET FLAGS TO GUIDE DB WRITE LOGIC
        
        if not self.done_b4:
            go_states = (self.cities_valid_and_matching, self.pt_eval_errors.error_free)
            if all(go_states):
                self.should_write = True
        # INSERT LOGIC HERE TO WRITE TO GOOGLE TABLE
        # INSERT LOGIC HERE TO WRITE TO ERROR TABLE

    def attempt_db_write(self):
        '''
        assuming no issues, write the address to the db
        otherwise write to logs, and if we still have a valid google result
        code it in the database to avoid unecessary api pings afterwards
        if we have errors with an address note that to avoid going through this 
        process again and making api pings that go nowhere

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
        print('attempting to write to db')
        if self.should_write_uf: # if the diff says to set unit flag in db b/c it is missing
            self.DBV.set_unit_flag(self.lat, self.lng)
        
        if self.should_write: # not in database already - no errors
            base_tuple = (self.simplified_address, 
                           self.city, 
                           self.lat,
                           self.lng)
            self.DBV.write_db(base_tuple, 'address')
        # ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)
        if self.should_write_g:
                 
            flagged_dir, dir_str = self.g_dir_type_tp # True/False, None or first letter of dir_type
            flagged_post_type, pt_str = self.g_post_type_tp # True/False, None or first letter of post_type 
            google_result = None
            self.DBV.write_db(google_result, 'google')
               
            #google_result = (self.coding_result.lat, 
            #        self.coding_result.lng,
            #        self.coding_result.g_address_str,
            #        self.coding_result.house_number,
             #       self.coding_result.street,
             #       self.coding_result.city,
            #        self.flagged_unit, # did we identify the building has units?
             #       flagged_dir, # NSEW?
             #       dir_str, # something from [N, S, E, W]?
             #       flagged_post_type, # street, drive etc. 
             #       pt_str # s, d etc. 
             #       )
            if self.should_write_et:
                self.DBV.write_db(base_tuple, 'errors')

'''
                    dbase.insert_into_db('google_result', google_result)
            else:
                self.rejected = True # We can't log this as a correct address
                post_type_logger(self.applicant, self.SAO.source_post_types, self.g_post_type_tp)
        else:
                self.rejected = True # we can't log this as a correct address
                post_type_logger(self.applicant, self.SAO.source_post_types,
                                    self.g_post_type_tp)
                    
    else:
        self.rejected = True # we can't log this as a correct address
        two_city_logger(self.applicant, self.city, self.coding_result.city)
    
    if not self.error_free: # < - replace this with other logic
        # so, we coded a result, but after doing that we identified errors. To avoid
        # geocoding this address again we should drop it in the db for future 
        # reference and use
        if not dbase.lat_lng_in_db(self.coding_result.lat, self.coding_result.lng):
            if google_result:
                dbase.insert_into_db('google_result', self.google_result)
                if self.rejected:
                    # to avoid handling this address again we will save it in the error table
                    # and log it as having an error later if we try and code it again
                    dbase.insert_into_db('errors', address_dbase_input)   
'''
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
        error_stack = {'d_parse': None, 'dbase_write': None}
        lop = line_obj_parser(line, fnames.ID, dbase, coordinate_manager) #.ID 
        try: # parse address
            lop.deconstruct(address_parser)
        except:
            print('could not successfully call deconstruct method')
            error_stack['d_parse'] = True  
        if not lop.poll_db(): # poll db - set lat,lng or...
            lop.try_gc_api() # attempt to geocode
        lop.diff_results() # compare source + db as well as source + google - set error flags
        if not lop.done_b4: # not in the database already?
            #lop.set_address_flags()
            if lop.should_write:
                try:
                    lop.attempt_db_write()
                except:
                    print('could not write to db.')
                    error_stack['dbase_write'] = True
        # insert logging call here
    dbase.close_db()
    print('proccess complete on source file {}'.format(config.target))
