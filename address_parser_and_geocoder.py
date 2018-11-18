#!/usr/bin/python3.6
'''
this script provides classes and methods to access the 
database of geocoded address data and runs through a 
L2F export attempting to geocode all the addressses.  
It also creates error logs that can be parsed later to find 
bad source address inputs that can be looked up and 
manually corrected in l2f 

it is in need of some major refactoring love
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
import config	# secret api key source
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
geocoding_logger.setLevel(logging.ERROR)
#geocoding_log_formatter = logging.Formatter('%(asctime)s:%(filename)s:%(funcName)s:%(name)s:%(message)s')
geocoding_log_formatter = logging.Formatter('%(message)s')
geocoding_log_file_handler = logging.FileHandler(r'Logging/geocoding.log')
geocoding_log_file_handler.setFormatter(geocoding_log_formatter)
geocoding_logger.addHandler(geocoding_log_file_handler)

address_str_parse_logger = logging.getLogger('address_parser')
address_str_parse_logger.setLevel(logging.ERROR)
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
    refer to for full definitions of all the tags
    the flags var is for helping to flag addresses that have a direction or multiunits
    this is important because it allows us to identify addresses that are missing key
    features for down stream delivery
    as such, we return a tuple of the parsed address and the flags variable
    '''
    parse_keys = parsed_string.keys()
    built_string = str()  
    # this should become a tuple! dealing with a dictionary is not needed 
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
        self.errors = {}
        self.parsed = {}

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

    def get_coordinates(self, input_address, input_city):
        '''
        searches the address and then errors table and if it finds an entry it returns the lat, lng
        '''
        self.cursor.execute("SELECT lat, lng FROM address WHERE source_street=? AND source_city=?",(input_address, input_city,))
        result = self.cursor.fetchone()
        if result:
            return result
        else:
            self.cursor.execute("SELECT lat, lng FROM errors WHERE source_street=? AND source_city=?",(input_address, input_city,))
            error_result = self.cursor.fetchone()
            if error_result:
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
        returns: boolean values in a 3 tuple (unit_flag, dir_flag, post_type)
        '''
        self.cursor.execute("SELECT unit_flag, dir_flag, post_type FROM google_result WHERE lat=? AND lng =?",(lat,lng,))
        flag_query = self.cursor.fetchone()
        if flag_query:
            return flag_query
        else:
            return (False, False, False)

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

class line_obj_parser():
    '''
    I am a big dumb monster, but a slight improvement over how things were
    structured before.
    this object wraps the process of looking at an address
    parsing it, evaluating it, coding it and logging its bits into a db
    '''    
    def __init__(self, line,fnamedict):
        self.line_object = Visit_Line_Object(line,fnamedict)
        self.flagged_unit = False
        self.in_bounds = False
        self.decon_address = None
        self.simplified_address = None
        self.flags = None
        self.flagged_unit = False
        self.source_post_types = None 
        self.applicant = None
        self.address = None
        self.city = None
        self.s_evf = None 
        self.post_type_tp = None
        self.g_dir_type_tp = None 
        self.g_evf = None
        self.coding_result = None
        self.cities_valid_and_matching = False
        self.error_free = False # A reference for saving a correct address or recording in a dif db
        self.rejected = False                       
        self.google_result = False 
        self.pt_eval_errors = None
        self.done_b4 = False
        self.lat = None
        self.lng = None
        self.should_write = False

    def deconstruct(self):
        '''
        cuts the address into bits and does some basic checks on 
        city and multiunit status
        '''
        self.address, self.city, _ = self.line_object.get_address()
        self.applicant = self.line_object.get_applicant_ID()

        self.decon_address = address_parser.parse(self.address, self.applicant)
        self.in_bounds = boundary_checker(self.city)
        print('looking up {}'.format(self.applicant))
        print('they live at {} {}'.format(self.address, self.city))
        print('their deconstructed address is: {}'.format(self.decon_address))
        try:
            self.simplified_address, self.flags = self.decon_address
        
        except:
            print('address error for {}. check logs for {}'.format(self.applicant, self.address))

        if not self.in_bounds:
            boundary_logger(self.applicant, self.city)
        if self.decon_address is not False and self.in_bounds == True:

            self.flagged_unit = self.flags['MultiUnit']
            self.source_post_types = parse_post_types(self.simplified_address)
            _, _, self.s_evf = self.source_post_types
            if self.s_evf:
                write_to_logs(self.applicant, self.city, 'post_parse')

        else:            
            print('address error for {}. check logs for {}'.format(self.applicant, self.address))

    def try_gc_api(self):
        '''
        try and geocode the address
        '''
        print('trying to geocode {} {}'.format(self.simplified_address,
                                               self.city))
        if dbase.is_in_db(self.simplified_address, self.city) == False: #  and self.s_evf == False:
            print('they are not in the db')
            address_for_api = '{} {} Ontario, Canada'.format(self.simplified_address, self.city)
            print('looking up {}'.format(address_for_api))           
            self.coding_result = coordinate_manager.lookup(address_for_api)
            print('status is {}'.format(self.coding_result))
            self.should_write = True
            if self.coding_result == None:                    
                print('error in geocoding address for {}. check logs for {}'.format(self.applicant, self.simplified_address))
                # to avoid pinging google again for this bad address, we will log it as an error
                error_tuple = (self.simplified_address, self.city, 0, 0)
                dbase.insert_into_db('errors', error_tuple)
                    
            if self.coding_result == False:
                raise Exception('We are at coding limit!')
                # we are at the limit - cool down
            if self.coding_result:
                self.lat = self.coding_result.lat
                self.lng = self.coding_result.lng

        else:
            print('Did not code it they are in the db')
            try:

                lat, lng = dbase.get_coordinates(self.simplified_address,
                                                     self.city) # get the lat, lng
                print(lat, lng)
                self.lat = lat
                self.lng = lng
            except:
                print('could not find lat and long in the database')

            if dbase.is_error(self.simplified_address,self.city):
                print('they are in the error table')
                # we looked this address up, logged an error and made some database entries previously.
                address_str_parse_logger.error('##777## {} was previously coded with errors'.format(self.applicant))

            if self.flagged_unit: # if the input string has a unit number and we have already coded it
                try:

                    if not lat and lng:
                        address_str_parse_logger.error('##405## Attempting to find geocodes in google_table but not present. Check address {}'.format(self.address))
                    else:
                        is_coded = dbase.get_address(lat, lng)
                        if is_coded:
                            uf, f2, f3 = dbase.pull_flags_at(lat, lng) # and use that to get the unit flag from database

                            if not uf and any([f2, f3]): # if the input string has one, but the database does not, we need to update the database
                                print('unit flag was missing from {} in the database. We have added one for {}'.format(self.simplified_address,
                                                                                                                        self.applicant))
                                dbase.set_unit_flag_in_db(lat, lng)
                            
                except Exception as oops:
                    print('Error with {} at address: {}'.format(self.applicant, self.simplified_address))
                    print('It raised Error: {}'.format(oops))
            else:
                # we have already coded it and everything is fine
                print('we have already coded it?')
                self.done_b4 = True
                lt, lg = dbase.get_coordinates(self.simplified_address,
                                               self.city)
                print('lat {} long {}'.format(lt, lg))
    
    def set_address_flags(self):
        '''
        evaluate the result from google and the source address and see if there
        are potential issues
        '''
        print('setting flags now')
        if self.coding_result:              
            g_city = self.coding_result.city
            google_address = '{} {}'.format(self.coding_result.house_number,
                                            self.coding_result.street)
            google_post_types = parse_post_types(google_address) # GOOGLE PT
           
            self.g_post_type_tp, self.g_dir_type_tp, self.g_evf = google_post_types
            
            # evaluate source vs. google post types
            self.pt_eval_errors = evaluate_post_types(self.source_post_types, google_post_types)
            
            self.cities_valid_and_matching = two_city_parser(self.city, g_city) # True or False
        else:
            print('no flags to set.  There is no coding result')

    def attempt_db_write(self):
        '''
        assuming no issues, write the address to the db
        otherwise write to logs, and if we still have a valid google result
        code it in the database to avoid unecessary api pings afterwards
        if we have errors with an address note that to avoid going through this 
        process again and making api pings that go nowhere
        '''
        print('attempting to write to db')
        input_tuple = (self.simplified_address, 
                       self.city, 
                       self.lat,
                       self.lng)
        print(input_tuple)
        address_dbase_input = input_tuple

        if all(self.cities_valid_and_matching): # Cities input and returned match and are valid
            if not any([self.s_evf, self.g_evf]): # if the post type parsers didn't find any weirdness

                if not any(self.pt_eval_errors): # no post type errors are present: google vs source match
                    # we made it fam.  We can enter things in the database now
                    self.error_free = True # we made it through 3 layers of error checking!
                    
                    flagged_dir, dir_str = self.g_dir_type_tp # True/False, None or first letter of dir_type
                    flagged_post_type, pt_str = self.g_post_type_tp # True/False, None or first letter of post_type 

                    dbase.insert_into_db('address', address_dbase_input)
                    
                    # we have not logged this source address, but have we already logged the 
                    # lat, lng and google result in the table?
                    if not dbase.lat_lng_in_db(self.coding_result.lat,
                                               self.coding_result.lng):
                        google_result = (self.coding_result.lat, 
                                self.coding_result.lng,
                                self.coding_result.g_address_str,
                                self.coding_result.house_number,
                                self.coding_result.street,
                                self.coding_result.city,
                                self.flagged_unit, # did we identify the building has units?
                                flagged_dir, # NSEW?
                                dir_str, # something from [N, S, E, W]?
                                flagged_post_type, # street, drive etc. 
                                pt_str # s, d etc. 
                                )

                        dbase.insert_into_db('google_result', google_result)
                else:
                    self.rejected = True # We can't log this as a correct address
                    post_type_logger(self.applicant, self.source_post_types, self.g_post_type_tp)
            else:
                    self.rejected = True # we can't log this as a correct address
                    post_type_logger(self.applicant, self.source_post_types,
                                     self.g_post_type_tp)
                        
        else:
            self.rejected = True # we can't log this as a correct address
            two_city_logger(self.applicant, self.city, self.coding_result.city)
        
        if not self.error_free: 
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

if __name__ == '__main__':
    coordinate_manager = Coordinates() # I lookup and manage coordinate data
    address_parser = AddressParser() # I strip out extraneous junk from address strings
    dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
    dbase.connect_to('Address.db', create=True) # testing = atest.db
    
    fnames = Field_Names(config.target) # I am header names
    fnames.init_index_dict() 
    export_file = Export_File_Parser(config.target ,fnames) # I open a csv 
    # for testing us test_export.csv
    export_file.open_file()
    
    for line in export_file: # I am a csv object
        lop = line_obj_parser(line,fnames.ID) #.ID
        try:
            lop.deconstruct()
        except:
            print('could not deconstruct')
        if lop.decon_address:
            lop.try_gc_api()
        else:
            print('unable to deconstruct address')
        if not lop.done_b4:
            lop.set_address_flags()
            if lop.should_write:
                try:
                    lop.attempt_db_write()
                except:
                    print('could not write to db')
    dbase.close_db()
    print('proccess complete')
