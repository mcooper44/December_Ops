#####################################################################################
# This script is for comparing a csv database export against a database of previously 
# geo_coded addresses it looks at the flags in the database that were set when 
# the addresses were coded and compares the csv address (source) to see if there are 
# any mismatches for key address parts like direction, street type (ave, drive etc)
# and city.  It then logs and provides indications as to what is the error type.
# Name Type Error = Missing a Drive, Street etc.
# Direction Type Error = Missing a cardinal point direction (N,S,E,W)
# Eval Flag = there is a potential mismatch between Name Type or Direction Type
# i.e. google determining that 100 Main Street North vs. 100 Main Street West
# are equavalent when they are geocoded.
#####################################################################################

from address_parser_and_geocoder import AddressParser, SQLdatabase, Coordinates
from db_data_models import Field_Names, Export_File_Parser, Visit_Line_Object
import usaddress
import logging

logging.basicConfig(filename='database_error.log',level=logging.INFO)

def parse_post_types(address):
    '''
    This function provides output indicating the presence and equivalency of 
    different post street name types (ave, street etc) and directions (east, south etc.)

    The variation in street types and directions does not vary much for most of the letters
    so if 123 Queen Street East is input in one place we can test equivalence 
    to 123 Queen St E by checking the first letter of Street vs. St. and East vs. E
    this does break down for some of the letters but for this application it should be 
    sufficient.
    '''
    
    street_types = {'a': ('avenue', 'ave', 'av'),
                  'b': ('boulevard','blvd','bend', 'boul', 'blvrd'),
                  'c': ('ct', 'crt','crest','crescent','cres','cr','court','circle', 'crcl', 'cir', 'cl'),
                  'd': ('drive', 'dr', 'drv'),
                  'f': ('field', 'feild'),
                  'g': ('green'),
                  'h': ('Hwy', 'Highway', 'Heights'),
                  'l': ('lane', 'ln', 'line'),
                  'p': ('place', 'pl', 'pkway', 'pkwy', 'pk', 'parkway', 'park'),
                  'r': ('road', 'ridge', 'rd'),
                  's': ('street', 'st', 'st n', 'square', 'springs'),
                  't': ('trail', 'terrace'), 
                  'w': ('way', 'walk')}
    directions = {'n': ('north', 'n', 'nor', 'nth'),
                  's': ('south', 's'),
                  'e': ('east', 'e'),
                  'w': ('west', 'w', 'wst')}

    streetnameptype = False # ave st etc. are present
    streetnamepdir = False # north south east etc. are present
    eval_flag = False # is there a mismatch?
    street_key = None # what is the first letter of street type?
    dir_key = None # what is the first letter of the direction tag e.g. north?
    
    tagged_address = usaddress.tag(address)
    
    tags, type_tag = tagged_address
    if type_tag == 'Street Address':
    
        if 'StreetNamePostType' in tags.keys():
                
            tag_value = tags['StreetNamePostType']
            street_key = tag_value[0].lower()
            if street_key in street_types.keys():
                tvalue = tag_value.lower()
                if tvalue in street_types[street_key]:
                    streetnameptype = True
                else:
                    eval_flag = True 
            else:
                eval_flag = True            

        if 'StreetNamePostDirectional' in tags.keys():
                
            tag_value = tags['StreetNamePostDirectional']
            dir_key = tag_value[0].lower()
            if dir_key in directions.keys():
                tv = tag_value.lower()
                if tv in directions[dir_key]:
                    streetnamepdir = True
                else:
                    eval_flag = True 
            else:
                eval_flag = True
        
    else:
        print('invalid tag response')
        return None    
    
    return ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)

def evaluate_post_types(source_types, db_types):
    '''
    Takes two results from parse_post_types and evaluates equavelence
    source_types = the address we wish to compare against an address from the database
    db_types = the result of passing an address from the database to to parse_post_types

    returns a tuple of boolean values (street name error, direction error, mismatch flag error)
    True indicates an error, False indicates there is no mismatch and therefore
    no error
    '''
    source_nt_tpl, source_dt_tpl, s_e_flag = source_types
    db_nt_tpl, db_dt_tpl, db_e_flag = db_types

    sn_error = False # street name error
    dt_error = False # direction type error
    fl_error = False # eval_flag mismatch

    if source_nt_tpl != db_nt_tpl:
        sn_error = True
    if source_dt_tpl != db_dt_tpl:
        dt_error = True
    if s_e_flag != db_e_flag:
        fl_error = True

    return (sn_error, dt_error, fl_error)

def flag_checker(tpl_to_check, lst_of_tpls_to_check_against):
    '''
    looks for missing unit, direction or post type on the source
    this function recycles flag_match and returns False and flag 
    toggles or True, and None.
    True signifies that the address is correct
    False indicates the address has some errors
    '''
    missing_unit = False
    missing_dir = False
    missing_pt = False

    for tpl_to_check_against in lst_of_tpls_to_check_against:
        if tpl_to_check != tpl_to_check_against:
            # take a slice from the tuple containing the flags
            flag_match = evaluate_post_types(tpl_to_check[2:],
                                             tpl_to_check_against[2:])
            mu, md, mpt = flag_match
            if mu:
                missing_unit = mu
            if md:
                missing_dir = md
            if mpt:
                missing_pt = mpt
    udp_flags = (missing_unit, missing_dir, missing_pt)
    if any(udp_flags):
        return  (False, udp_flags)
    else:
        return (True, None)

def write_to_logs(applicant, flags=None, flag_type='boundary'):
    '''
    this function is used by the different flag checking functions to write different strings
    to the logs
    '''
    if flag_type == 'bound':
        print('Flag {} for out of bounds'.format(applicant))
    if flag_type == 'udp':
        u, d, p = flags
        print('Applicant: {} Unit Toggle: {} Dir Toggle: {} PostType Toggle: {}'.format(applicant,u,d,p))
    if flag_type == 'mismatch':
        o,t,th = flags
        logging.info('{} Name Type E = {} Dir Type E = {} Eval Flag = {}'.format(applicant, o, t, th))

def boundary_checker(city):
    '''
    takes a city name as a string and tests to see if it is in the list of
    cities to serve.  If it is, it returns True
    Otherwise it returns False
    '''
    in_bounds = ['kitchener', 'waterloo']
    lcity = city.lower()
    if lcity in in_bounds:
        return True
    else:
        return False

def boundary_logger(applicant, city, google=False):
    '''
    checks to see if the city is out of bounds.  If it is, it writes to the log.
    google flag is to indicate if the address being boundary checked is a 
    source address (=False) or an address returend by the google api
    This can sometimes come up if the source address is a valid for another
    community (often in another country), but not locally.  The fact that 
    King Street, Queen Street etc. are common street names in many communities 
    can certainly complicate things.
    '''
    if not boundary_checker(city):
        if not google:
            write_to_logs(applicant, flag_type='bound')
        else:
            applicant_string = 'Google result for {}'.format(applicant)
            write_to_logs(applicant_string, flag_type='bound')

def create_reference_object(flags, parsed_address, city):
    '''
    takes flags extracted from the address, an address and a city and reformats them into a tuple
    that can be compared against flags extracted from the database for the canonical address exemplar
    '''
    source_units_flag, source_dirs_flag, source_post_flag = flags['MultiUnit'], flags['Direction'], flags['PostType']
    reference = (parsed_address, city, source_units_flag, source_dirs_flag, source_post_flag)
    return reference

def missing_element_logger(applicant, flags, parsed_address, city, flags_from_db):
    '''
    Takes flags (unit, direction, post type) extracted from the source address, flags extracted 
    from the canonical object in the database and uses the create_reference_object to create a 
    template to use in the flag_checker function.
    If there is a mismatch of the flags, it then creates a log entry denoting where the issues are.    
    '''
    reference_object = create_reference_object(flags,parsed_address, city)
    flag_references = flag_checker(reference_object, flags_from_db) # tuple, list of tuples
    is_ok, toggles = flag_references
    if not is_ok:
        write_to_logs(applicant, toggles, 'udp')   

def post_type_logger(applicant, source_post_types, post_types_from_dbase):
    '''
    uses the evaluate_post_types function to evaluate if the street type or direction
    is mismatched in teh source address or if there is some error that merits follow up
    if there is, it will write to the logs
    '''
    post_type_evaluation = evaluate_post_types(source_post_types, post_types_from_dbase)
    if any(post_type_evaluation): # if any of the flags were mismatched
        one, two, three = post_type_evaluation
        logging.info("""{} Name Type Error = {} Direction Type Error = {} Eval Flag
                     = {}""".format(applicant, one, two, three))

def google_parser(g_address, g_city):
    '''
    this function runs a google address through the different error checking functions
    and returns a consolidated set of flags to log in the database and check
    against the input string
    '''
    boundary_value = boundary_checker(g_city) # either True or False
    post_types = parse_post_types(g_address) #  ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)
    pass # this will later encapsulate something to handle the source and google strings     


if __name__ == '__main__':
    address_parser = AddressParser() # I strip out extraneous junk from address strings and set type flags
    
    dbase = SQLdatabase() # I have methods to access a database and return geocode/address info
    dbase.connect_to('atest.db', create=True)
    
    fnames = Field_Names('header_config.csv') # I am header names
    fnames.init_index_dict()
    
    export_file = Export_File_Parser('test_export.csv',fnames.ID) # I open a csv export from a cloud database
    export_file.open_file()

    for line in export_file: # for line in the csv file object yielded via the __iter__ method    
        line_object = Visit_Line_Object(line,fnames.ID)
        address, city, _ = line_object.get_address()
        applicant = line_object.get_applicant_ID()
        parsed_address, flags = address_parser.parse(address)
        lat, lng = dbase.get_coordinates(parsed_address, city)
        flags_from_db = dbase.pull_flags_at(lat, lng) # list of tuples from the database
        address_from_dbase =  dbase.get_address(lat, lng)# e.g. 100 Regina St
        
        boundary_logger(applicant, city) # check to see if the city is out of bounds
        
        # then look to see if any items are missing and if they are, log them       
        missing_element_logger(applicant, flags, parsed_address, city, flags_from_db)

        # finally check to see if anything is there, but doesn't match the
        # canonical object e.g. input address is 100 Regina st West,
        # but shoudl be ... St South.  IF so, log it all!!!!!!!!
        post_types_from_dbase = parse_post_types(address_from_dbase)
        source_post_types = parse_post_types(parsed_address) # is there a street type and/or direction? 
        post_type_logger(applicant, source_post_types, post_types_from_dbase) 
    
    dbase.close_db()
