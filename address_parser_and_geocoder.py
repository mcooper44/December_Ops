import csv
import geocoder
import time
from collections import namedtuple
import usaddress
import string
import sqlite3
import logging
import config	# secret api key source
import db_data_models as dbdm # classes for dealing with L2F exports


#api key
myapikey = config.api_key

# error logging is handled by functions that carry out parsing and geocoding
# they pass False or None on to the objects using them and are handled by
# the objects
logging.basicConfig(filename='address_parse_coding.log',level=logging.INFO)

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
    return out[-1].strip().strip(updown) # strip white space and any letters i.e. 124B will now = 124

def address_builder(parsed_string):
    '''
    takes the ordered dictionary object (parsed_string) created by the usaddress.tag method 
    and builds a string out of the relevant tagged elements, stripping out the address number
    from the 123-44 Main Street formatted addresses using the street_number_parser() function
    refer to for full definitions of all the tags
    
    '''
    parse_keys = parsed_string.keys()
    built_string = str()  
    flags = {'MultiUnit': False, 'Direction': False}
    if 'AddressNumber' in parse_keys:
        source_value = parsed_string['AddressNumber']
        street_number = street_number_parser(source_value)
        if len(source_value) > street_number:
            flags['MultiUnit'] = True
        built_string = '{} {}'.format(built_string, street_number)
        
    if 'StreetNamePreDirectional' in parse_keys: # a direction before a street name e.g. North Waterloo Street
        built_string = '{} {}'.format(built_string, parsed_string['StreetNamePreDirectional'])
        
    if 'StreetName' in parse_keys:
        built_string = '{} {}'.format(built_string, parsed_string['StreetName'])
        
    if 'StreetNamePostType' in parse_keys: # a street type that comes after a street name, e.g. ‘Avenue’
        built_string = '{} {}'.format(built_string, parsed_string['StreetNamePostType'])
        
    if 'StreetNamePostDirectional' in parse_keys: # a direction after a street name, e.g. ‘North’
        source_string = parsed_string['StreetNamePostDirectional']
        flags['Direction'] = True
        built_string = '{} {}'.format(built_string, source_string)
        
    if 'StateName' in parse_keys:
        if 'PlaceName' in parse_keys: # City
            built_string = '{} {}, {}'.format(built_string, parsed_string['PlaceName'], parsed_string['StateName'])
        
    return (built_string.strip(), flags)  # strip out the leading white space

def full_address_parser(addr):
    '''
    takes a street address e.g. 123 Main Street and attempts to break it into 
    the relevant chunks
    usaddress.tag() returns a tuple of (OrderedDict, str) with the str being a designator of typex
    returns a tuple (True, original address, parsed address) or (False, original address, error code)       
    '''
    usaparsed_street_address = namedtuple('usaparsed_street_address','flag original return_value')
    if addr:
        try:
            tagged_address, address_type = usaddress.tag(addr)
            if address_type == 'Street Address':
                # parse the address with the other helper functions
                p_add = address_builder(tagged_address)
                return usaparsed_street_address(True, addr, p_add)
            else:                
                # log address format error and flag for manual follow up
                logging.info('Could not derive Street Address from {}'.format(addr))
                return usaparsed_street_address(False, addr, 'address_type Error')
                
        except usaddress.RepeatedLabelError:
            # log address format error and flag for manual follow up
            logging.info('RepeatedLabelError from {}'.format(addr))            
            return usaparsed_street_address(False, addr, 'RepeatedLabelError')
        except KeyError:
            logging.info('KeyError from {}'.format(addr))
            return usaparsed_street_address(False, addr, 'KeyError')            
            
    else:
        logging.info('Blank Field Error from {}'.format(addr))
        return usaparsed_street_address(False, addr, 'Blank Field Error')
        # we can just skip blank lines

def street_from_buzz(address_string):
    '''
    helper function for clipping the junk line out of the address string.
    '''
    split_line = address_string.partition(',')
    return split_line[0]

def returnGeocoderResult(address,myapikey):
    """
    this function takes an address and passes it to googles geocoding
    api with the help of the Geocoder Library.
    it returns a geocoder object wrapped around the json response
    """    
    try:            
        time.sleep(1)
        result = geocoder.google(address, key=myapikey)
        if result is not None:
            if result.status == 'OK':
                return result
            elif result.status == 'OVER_QUERY_LIMIT':
                logging.info('{} yeilded {}'.format(address,result.status))
                return False
            else:
                logging.info('Result is None with {} on {}'.format(result.status, address))
                return None
        else:
            return None
    except Exception as boo:
        logging.info('Exception {} raised'.format(boo))
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
        a string of 'unit number street' or False
        '''
        key_list = list(self.errors.keys()) + list(self.parsed.keys())
        if address not in key_list:
            # need to insert database management stuff here
            worked, in_put, out_put  = full_address_parser(address)
            if worked:
                self.parsed[in_put] = out_put
                return out_put
            else:
                self.errors[in_put] = out_put
                return False
        else:
            if address in self.errors:
                return False
            else:
                return self.parsed[address]
    
    def is_not_error(self, address):
        # need to insert some database ping here
        if address not in self.errors:
            return True
        else:
            return False

class Coordinates():
    def __init__(self):
        self.api_key = myapikey
        self.can_proceed = True
        self.at = {}
        self.calls = 0

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
                return address_tpl(g_address_str,
                        house_number,
                        street,
                        city,
                        lat,
                        lng)
            else:
                # huh. that's odd. Recieved something other than valid object, False or None
                raise Exception('Error in lookup method of Coordinates Class')
        else:
            raise Exception('Over_Query_Limit')
   
    def add_coordinates(self, address, address_tuple):
        '''
        takes a named tuple and adds it to the self.at dict with an 
        address string as the key
        '''
        self.at[address] = address_tuple

    def return_address(self, address):
        '''
        returns the values parsed from the google json response
        stored in the self.at dictionary
        otherwise it returns False if it has not been stored there
        '''
        if address in self.at:
            return self.at[address]
        else: 
            return False 
    
    def __str__(self):
        return 'Coordinate Object. can_proceed  = {} and has made {} calls'.format(self.can_proceed, self.calls)

class SQLdatabase():
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.name = None
        
    def connect_to(self, name, create = False):
        if name:
            self.name = name
        try:
            self.conn = sqlite3.connect(name)
            self.cursor = self.conn.cursor()
            if create:
                self.cursor.execute("""CREATE TABLE IF NOT EXISTS address (source_street text,
                                                                         source_city text,
                                                                         google_full_str text,
                                                                         google_house_num text,
                                                                         google_street text,
                                                                         google_city text,
                                                                         lat real,
                                                                         lng real)""")
                self.conn.commit()
        except:
            print('error with database connection')
         
    def insert_into_db(self, values):
        if not self.name:
            print('establish connection first')
            return False
        self.cursor.execute('INSERT INTO address VALUES (?,?,?,?,?,?,?,?)', values)
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

    def close_db(self):
        '''
        closes the database
        '''
        self.conn.close()

if __name__ == '__main__':
    coordinate_manager = Coordinates() # I lookup and manage coordinate data
    address_parser = AddressParser() # I strip out extraneous junk from address strings
    dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
    dbase.connect_to('atest.db')
    
    fnames = dbdm.Field_Names('header_config.csv') # I am header names
    fnames.init_index_dict() 
    export_file = dbdm.Export_File_Parser('test_export.csv',fnames.ID) # I open a csv
    export_file.open_file()

    for line in export_file.file_object: # I am a csv object
        line_object = dbdm.Visit_Line_Object(line,fnames.ID)
        address, city, _ = line_object.get_address()
        
        decon_address = address_parser.parse(address) # returns '301 Front Street West'
        if decon_address is not False:
            if dbase.is_in_db(decon_address, city) == False:
                address_for_api = '{} {} Ontario, Canada'.format(decon_address, city)
                coding_result = coordinate_manager.lookup(address_for_api)
                if coding_result:
                    # we have a successful result - log it in teh database
                    dbase_input = (decon_address, 
                                city, 
                                coding_result.g_address_str,
                                coding_result.house_number,
                                coding_result.street,
                                coding_result.city,
                                coding_result.lat,
                                coding_result.lng)
                    dbase.insert_into_db(dbase_input) 
                if coding_result == None:
                    pass
                    print('error in geocoding. check logs for {}'.format(decon_address))
                if coding_result == False:
                    pass
                    # we are at the limit - cool down
            else:
                print('already coded {}!'.format(decon_address))
        else:            
            print('error in parsing address. check logs for {} at {}'.format(address_parser.errors[address],address))
    dbase.close_db()
