import csv
import geocoder
import time
from collections import namedtuple
import usaddress
import string
import sqlite3
import config	
import logging

#api key
myapikey = config.api_key

logging.basicConfig(file_name='address_parse.log',level=logging.INFO)

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
            elif result.status == 'OVER_QUERY_LIMIT'
                logging.info('{} yeilded {}'.format(address,result.status)
                return False
            else:
                print('no result, returning null_list.  Status is: {}'.format(result.status))
                logging.info('Result is None with {} on {}'.format(result.status, address))
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
        if address not in self.parsed:
            # need to insert database management stuff here
            worked, in_put, out_put  = full_address_parser(address)
            if worked:
                self.parsed[in_put] = out_put
                return out_put
            else:
                self.errors[in_put] = out_put
                return False
    
    def is_not_error(self, address):
        # need to insert some database ping here
        if address not in self.errors:
            return True
        else:
            return False


class Coordinates():
    def __init__(self, api_key):
        self.api_key = api_key
        self.can_proceed = True
        self.at = {}

    def lookup(self, address):
        '''
        looks up an address and returns a tuple
        of address string, house_number, street, lat, lng
        using the returnGeocoderResult function
        '''
        address_tpl = namedtuple('address_tpl', 'g_address_str,house_number,street,city,lat,lng')
        if self.can_proceed:
            response = returnGeocoderResult(address, self.api_key)
            if response.ok:
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
            if response == False:
                self.can_proceed = False
                return response
            else:
                # insert log call here
                return None
   
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
    
    def has_been_coded(self, address_db):
        pass

def street_from_buzz(address_string):
    '''
    helper function for clipping the junk line out of the address string.
    '''
    split_line = address_string.partition(',')
    return split_line[0]

if name == __main__:
    coordinate_manager = Coordinates()
    address_parser = AddressParser() 
    # open file ...
    # parse visit line
    for line in line_obj:
        address, city, _ = line.get_address()
        decon_address = address_parser.parse(address)
        if decon_address is not False:
            address_for_api = '{} {} Ontario, Canada'.format(decon_address, city)
            coding_result = coordinate_manager.lookup(address_for_api)
            if coding_result:
                # we have a successful result - log it in teh database
            
            if coding_result == None:
                # we are not at the limit - some error occured.  try again?
            if coding_result == False:
                # we are at the limit - cool down 