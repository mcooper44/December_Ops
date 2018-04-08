#####################################################################################
# This script is for comparing a csv database export against a database of previously 
# geo_coded addresses it looks at the flags in the database that were set when 
# the addresses were coded and compares the csv address to see if there are any
# mismatches for key address parts like direction, street type (ave, drive etc)
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

def write_to_logs(applicant, flags=None, flag_type='boundary'):
    if flag_type == 'bound':
        print('Flag {} for out of bounds'.format(applicant))
    if flag_type == 'udp':
        u, d, p = flags
        print('Applicant: {} Unit Toggle: {} Dir Toggle: {} PostType Toggle:
              {}'.format(applicant,u,d,p)
    if flag_type == 'mismatch':
        o,t,th = flags
        logging.info('{} Name Type E = {} Dir Type E = {} Eval Flag = {}'.format(appilcant, o, t, th))

if __name__ == '__main__':
    address_parser = AddressParser() # I strip out extraneous junk from address strings and set type flags
    
    coordinate_manager = Coordinates()

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

        city_flag = boundary_checker(city)
        if not city_flag:
            write_to_logs(applicant, city_flag, 'bound')
        #### to do: build a separate table in the database and store the google
        #### returned address as a canonical tagged reference to compare
        #### addresses in our database that google can geocode but which may be
        #### missing key features or have the wrong type Missing Ave, or Says N
        #### rather than S which may be the correct address
        decon_address = address_parser.parse(address) # returns ('301 Front Street West', flags) or False
        if decon_address:
            parsed_address, flags = decon_address
            source_post_types = parse_post_types(parsed_address) # is there a street type and/or direction?
            coords_from_db = dbase.get_coordinates(parsed_address, city)
            if coords_from_db:
                lat, lng = coords_from_db
                flags_from_db = dbase.pull_flags_at(lat, lng) # list of tuples from the database
                # create a reference to compare the flagged addresses from the database with                
                source_units_flag, source_dirs_flag, source_post_flag = flags['MultiUnit'], flags['Direction'], flags['PostType']
                reference_object = (parsed_address, city, source_units_flag, source_dirs_flag, source_post_flag)
                # check to see if anything is missing
                flag_referenes = flag_checker(reference_object, returned_tuple)
                is_ok, toggles = flag_references
                if not is_ok:
                    write_to_logs(applicant, toggles, 'udp')
                # check to see if anything is there, but doesn't match the
                # canonical object e.g. input address is 100 Regina st West,
                # but shoudl be ... St South.
                address_from_dbase = returned_tuple[0] # e.g. 100 Regina St
                post_types_from_dbase = parse_post_types(address_from_dbase)

                post_type_evaluation = evaluate_post_types(source_post_types, post_types_from_dbase)

                if any(post_type_evaluation): # if any of the flags were mismatched
                    one, two, three = post_type_evaluation
                    print('Name Type Error = {} Direction Type Error = {} Eval Flag = {}'.format(one, two, three))
            else:
                raise Exception('Not_Logged_In_Database_{}'.format(applicant))
        else:
            raise Exception('Address_Parse_Error_{}'.format(applicant))
    dbase.close_db()
