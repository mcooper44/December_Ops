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

def full_address_parser(address):
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
                address_str_parse_logger.info('##80## Parsed {} with result {}'.format(address, p_add[0]))
                return usaparsed_street_address(True, addr, p_add)
            else:                
                # log address format error and flag for manual follow up
                address_str_parse_logger.error('##71## Could not derive Street Address from {}'.format(address))
                return usaparsed_street_address(False, addr, 'address_type Error')
                
        except usaddress.RepeatedLabelError:
            # log address format error and flag for manual follow up
            address_str_parse_logger.error('##72## RepeatedLabelError from {}'.format(address))            
            return usaparsed_street_address(False, addr, 'RepeatedLabelError')
        except KeyError:
            address_str_parse_logger.error('##73## KeyError from {}'.format(address))
            return usaparsed_street_address(False, addr, 'KeyError')            
            
    else:
        address_str_parse_logger.error('##74## Blank Field Error from {}'.format(address))
        return usaparsed_street_address(False, addr, 'Blank Field Error')
        # we can just skip blank lines

def returnGeocoderResult(address, myapikey=config, second_chance=True):
    """
    this function takes an address and passes it to googles geocoding
    api with the help of the Geocoder Library.
    it returns a geocoder object wrapped around the json response OR
    False if we are at the free query limit or some major exception 
    happens in the try block
    or None if there is an error with the api (sometimes it just does
    not work)
    """
    try_again = second_chance    
    try:            
        sleep(1)
        result = geocoder.google(address, key=myapikey)
        if result is not None:
            if result.status == 'OK':
                geocoding_logger.info('##500## {} is {}'.format(address, result.status))
                return result
            elif result.status == 'OVER_QUERY_LIMIT':
                geocoding_logger.warning('##402## {} yeilded {}'.format(address,result.status))
                return False
            else:
                geocoding_logger.warning('##403## Result is not OK or OVER with {} at {}'.format(result.status, address))
                return None
        else:
            geocoding_logger.warning('##401## Result is None with status {} on {}'.format(result.status, address))
            if try_again:
                print('got None from google api, trying again for {}'.format(address))
                sleep(10) # wait and try again once more after waiting
                returnGeocoderResult(address, myapikey=config, second_chance=False)
            else:
                return None # tried to see if a second attempt would work, but it didn't
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

    def parse(self, address):
        '''
        give it an address with extraneous details and it will give you
        a tuple of  ('unit number street', error_flags) or False
        '''
        key_list = list(self.errors.keys()) + list(self.parsed.keys())
        if address not in key_list and address is not False:
            
            worked, in_put, out_put  = full_address_parser(address)
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
    
    def return_simple_address(self, source_address):
        '''
        this function sidesteps the error checking steps
        and just tries to simplify an address string
        '''
        try:
            _, _, parsed_address = full_address_parser(source_address)
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
    An object for looking up and storing geocoded address information
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
                response = returnGeocoderResult(address, self.api_key)
                self.calls += 1
                if response == False: # returnGeocoderResult returns False when limit reached
                    self.can_proceed = False
                    return response
                if response == None:
                    return None
                if response.ok: # Either True or False
                    
                    lat, lng = response.lat, response.lng
                    g_address_str = response.address
                    city = response.city
                    house_number = response.housenumber
                    street = response.street
                    response_tple = address_tpl(g_address_str,
                            house_number,
                            street,
                            city,
                            lat,
                            lng)
                    self.coordinates[address] = response_tple
                    return response_tple
                
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
    # reference https://stackoverflow.com/questions/418898/sqlite-upsert-not-insert-or-replace

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.name = None
        
    def connect_to(self, name, create = True):
        '''
        establish a connection and cursor and if necessary create the address table
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
        except:
            print('error with database connection')
         
    def insert_into_db(self, table, values):
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

    def is_in_db(self, parsed_address, source_city):
        '''
        this method checks to see if an address has been logged in the database already.
        https://stackoverflow.com/questions/25387537/sqlite3-operationalerror-near-syntax-error
        sql always kills me on this and it takes me forever to find and recomprehend Martins answer
        :(
        '''

        self.cursor.execute("SELECT * FROM address WHERE source_street=? AND source_city=?",(parsed_address,source_city,))
        db_ping = self.cursor.fetchone()        
        if db_ping:
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
        
        self.cursor.execute("SELECT lat, lng FROM address WHERE source_street=? AND source_city=?",(input_address, input_city,))
        result = self.cursor.fetchone()
        if result:
            return result
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

    def pull_flags_at(self, lat, lng):
        '''
        returns all the source addresses and their flags at a given lat,lng
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

    def close_db(self):
        '''
        closes the database
        '''
        self.conn.close()

if __name__ == '__main__':
    # this is a beast, I know.  It should change but I'm 
    coordinate_manager = Coordinates() # I lookup and manage coordinate data
    address_parser = AddressParser() # I strip out extraneous junk from address strings
    dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
    dbase.connect_to('Address.db', create=True) # testing = atest.db
    
    fnames = Field_Names('header_config.csv') # I am header names
    fnames.init_index_dict() 
    export_file = Export_File_Parser('2018source.csv',fnames.ID) # I open a csv 
    # for testing us test_export.csv
    export_file.open_file()
    mile_stones = range(0, 30001, 50)
    ln_num = 1
    for line in export_file: # I am a csv object
        line_object = Visit_Line_Object(line,fnames.ID)
        address, city, _ = line_object.get_address()
        applicant = line_object.get_applicant_ID()
        
        ln_num += 1
        if ln_num in mile_stones:
            print('Processing {} at line number: {} at {}'.format(applicant, ln_num, strftime("%H:%M:%S", gmtime())))
        
        # deconstructed address -returns ('301 Front Street West', flags)
        # flags = {'MultiUnit': False, 'Direction': False, 'PostType': False} 
        decon_address = address_parser.parse(address) 
        in_bounds = boundary_checker(city)
        if not in_bounds:
            boundary_logger(applicant, city)
        # first layer error checking we can deconstruct the address and it's not out of bounds
        if decon_address is not False and in_bounds == True:
            simplified_address, flags = decon_address
            flagged_unit = flags['MultiUnit'] # True or False THIS UNIT FLAG IS SIGNIFICANT
            
            source_post_types = parse_post_types(simplified_address) # SOURCE PT's
            o_, o_, s_evf  = source_post_types # source evaluation flag i.e a pt parse error
            if s_evf:
                write_to_logs(applicant, city, 'post_parse') # The direction of street post type is wonky
            # second layer: if we have not coded it and there are no eval flags
            if dbase.is_in_db(simplified_address, city) == False and s_evf == False: 
                address_for_api = '{} {} Ontario, Canada'.format(simplified_address, city)
                ### HERE BE GEOCODING ###
                coding_result = coordinate_manager.lookup(address_for_api) # returns False, None or tuple
                if coding_result:              
                    g_city = coding_result.city
                    google_address = '{} {}'.format(coding_result.house_number, coding_result.street)
                    google_post_types = parse_post_types(google_address) # GOOGLE PT
                   
                    g_post_type_tp, g_dir_type_tp, g_evf = google_post_types
                    
                    # evaluate source vs. google post types
                    pt_eval_errors = evaluate_post_types(source_post_types, google_post_types)
                    
                    cities_valid_and_matching = two_city_parser(city, g_city) # True or False
                    
                    # ok we have looked at the source and google address and derived post types
                    # we have also looked at the cities and determined if they match and are valid
                    # we can start to use logic to decide what to put in the database
                    # ...

                    # third layer: google and source address construction and cities need to match
                    if all(cities_valid_and_matching): # Cities input and returned match and are valid
                        if not any([s_evf, g_evf]): # if the post type parsers didn't find any weirdness

                            if not any(pt_eval_errors): # no post type errors are present: google vs source match
                                # we made it fam.  We can enter things in the database now
                                flagged_dir, dir_str = g_dir_type_tp # True/False, None or first letter of dir_type
                                flagged_post_type, pt_str = g_post_type_tp # True/False, None or first letter of post_type                                

                                address_dbase_input = (simplified_address, 
                                                    city, 
                                                    coding_result.lat,
                                                    coding_result.lng)
                                dbase.insert_into_db('address', address_dbase_input)

                                google_result = (coding_result.lat, 
                                            coding_result.lng,
                                            coding_result.g_address_str,
                                            coding_result.house_number,
                                            coding_result.street,
                                            coding_result.city,
                                            flagged_unit, # did we identify the building has units?
                                            flagged_dir, # NSEW?
                                            dir_str, # something from [N, S, E, W]?
                                            flagged_post_type, # street, drive etc. 
                                            pt_str # s, d etc. 
                                            ) 
                                # we have not logged this source address, but have we already logged the 
                                # lat, lng and google result in the table?
                                if not dbase.lat_lng_in_db(coding_result.lat, coding_result.lng):
                                    dbase.insert_into_db('google_result', google_result)
                            else:
                                post_type_logger(applicant, source_post_types, google_post_types)
                                
                        else:
                            post_type_logger(applicant, source_post_types, google_post_types)
                        
                    else:
                        two_city_logger(applicant, city, g_city)
                        
                if coding_result == None:                    
                    print('error in geocoding address on line {} for {}. check logs for {}'.format(ln_num, applicant, simplified_address))
                    
                if coding_result == False:
                    raise Exception('We are at coding limit! We reached line {}'.format(ln_num))
                    # we are at the limit - cool down
            else:                              
                if flagged_unit: # if the input string has a unit number and we have already coded it
                    try:
                        lat, lng = dbase.get_coordinates(simplified_address, city) # get the lat, lng
                        if not lat and lng:
                            address_str_parse_logger.error('##405## Attempting to find geocodes in google_table but not present. Check address on line {}'.format(ln_num))
                        else:
                            uf, _, _ = dbase.pull_flags_at(lat, lng) # and use that to get the unit flag from database
                            if not uf: # if the input string has one, but the database does not, we need to update the database
                                print('unit flag was missing from {} in the database.  We have added one for {} at {}'.format(simplified_address,
                                                                                                                        applicant, ln_num))
                                dbase.set_unit_flag_in_db(lat, lng)
                                missing_unit_logger(applicant, address)
                    except Exception as oops:
                        print('Error with {} at address: {} on line {}'.format(applicant, simplified_address, ln_num))
                        print('It raised Error: {}'.format(oops))
        else:            
            print('address error on line {} for {}. check logs for {}'.format(ln_num, applicant, address))      
    dbase.close_db()
    print('Process complete')
