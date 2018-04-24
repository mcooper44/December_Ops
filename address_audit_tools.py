
####################################################################################
# these functions are used to do error checking on source address strings and the 
# address strings that come out of the geocoding pipeline. 
# The major quality issues that come up in sending and recieving address strings
# are missing cardinal directions (N,S,E,W) missing appartment/unit numbers
# on multi-unit residences, missing or incorrect street name types (Ave, Street, Rd)
#
####################################################################################

import usaddress
import logging

address_audit_log = logging.getLogger(__name__)
address_audit_log.setLevel(logging.ERROR)
#address_log_formatter = logging.Formatter('%(asctime)s:%(filename)s:%(funcName)s:%(name)s:%(message)s')
address_log_formatter = logging.Formatter('%(message)s')
address_log_file_handler = logging.FileHandler('address_audit_errors.log')
address_log_file_handler.setFormatter(address_log_formatter)
address_audit_log.addHandler(address_log_file_handler)

def parse_post_types(address):
    '''
    This function provides output indicating the presence and equivalency of 
    different post street name types (ave, street etc) and directions (east, south etc.)

    The variation in street types and directions does not vary much for most of the letters
    so if 123 Queen Street East is input in one place we can test equivalence 
    to 123 Queen St E by checking the first letter of Street vs. St. and East vs. E
    this does break down for some of the letters but for this application it should be 
    sufficient.
    returns a tuple ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)
    eval_flag indicates the presence of an unmapped or potentially wrong usadress tag outcome
    '''
    
    street_types = {'a': ('avenue', 'ave', 'av'),
                  'b': ('boulevard','blvd','bend', 'boul', 'blvrd'),
                  'c': ('ct', 'crt','crest','crescent','cres','cr','court','circle', 'crcl', 'cir', 'cl'),
                  'd': ('drive', 'dr', 'drv'),
                  'f': ('field', 'feild'),
                  'g': ('green'),
                  'h': ('hwy', 'highway', 'heights'),
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
    eval_flag = False # is there a mismatch in the pt or dir keys?
    street_key = None # what is the first letter of street type?
    dir_key = None # what is the first letter of the direction tag e.g. north?
    
    tagged_address = usaddress.tag(address)
    
    tags, type_tag = tagged_address
    if type_tag == 'Street Address':
    
        if 'StreetNamePostType' in tags.keys():
                
            tag_value = tags['StreetNamePostType']
            street_key = tag_value[0].lower() # Street becomes s
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
        print('attempting to parse post types. Got invalid tag response for {}'.format(address))
        return (None, None, True)
    
    return ((streetnameptype, street_key), (streetnamepdir, dir_key), eval_flag)

def evaluate_post_types(source_types, db_types):
    '''
    Takes two results from parse_post_types and evaluates equavelence
    source_types = the address we wish to compare against an address from the database
    db_types = the result of passing an address from the database to to parse_post_types

    returns a tuple of boolean values (street name error, direction error, mismatch flag error)
    True indicates an error, False indicates there is no mismatch and therefore no error
    
    (street name type errors, direction type errors, a name or dir dictionary matching error)
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
    if s_e_flag:
        fl_error = True
    if db_e_flag:
        fl_error = True

    return (sn_error, dt_error, fl_error)

def flag_checker(tpl_to_check, tpl_to_check_against):
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

    
    if tpl_to_check != tpl_to_check_against:
        
        flag_match = evaluate_post_types(tpl_to_check,
                                            tpl_to_check_against)
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

def write_to_logs(applicant, flags=None, flag_type=None):
    '''
    this function is used by the different flag checking functions to write different strings
    to the logs with assoicated error codes
    '''
    if flag_type == 'bound':
        address_audit_log.error('##10## {} flag for out of bounds'.format(applicant))
    if flag_type == 'udp': # source is missing flags - esp. unit
        u, d, p = flags
        address_audit_log.error('##30## {} is missing  unit {} Direction {} PostType {}'.format(applicant,u,d,p))
    if flag_type == 'mismatch':
        o,t,th = flags
        address_audit_log.error('##20## {} Returned a mismatch. Following Errors are True Name Type E = {} Dir Type E = {} Eval Flag = {}'.format(applicant, o, t, th))
    if flag_type == 'two_city':
        address_audit_log.error(flags) # in this case flags = a string from the two_city_logger
    if flag_type == 'g_bound':
        address_audit_log.error('##15## {} returned invalid city {} on google result'.format(applicant, flags))
    if flag_type == 'post_parse':
        address_audit_log.error('##25## {} returned a error flag.  Follow up with the address {}'.format(applicant, flags))
    if not flag_type:
        raise Exception('FLAG NOT PRESENT in write_to_logs call')

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
            write_to_logs(applicant, city, flag_type='g_bound')

def create_reference_object(flags):
    '''
    takes flags extracted from the address, an address and a city and reformats them into a tuple
    that can be compared against flags extracted from the database for the canonical address exemplar
    '''
    source_units_flag, source_dirs_flag, source_post_flag = flags['MultiUnit'], flags['Direction'], flags['PostType']
    reference = (source_units_flag, source_dirs_flag, source_post_flag)
    return reference

def missing_element_logger(applicant, flags, parsed_address, city, flags_from_db):
    '''
    Takes flags (unit, direction, post type) extracted from the source address, flags extracted 
    from the canonical object in the database and uses the create_reference_object to create a 
    template to use in the flag_checker function.
    If there is a mismatch of the flags, it then creates a log entry denoting where the issues are.    
    '''
    reference_object = create_reference_object(flags)
    flag_references = flag_checker(reference_object, flags_from_db) # tuple, tuple of database flags
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
        write_to_logs(applicant, post_type_evaluation, 'mismatch')

def two_city_parser(source_city, g_city):
    '''
    this function determines if the source and google city checks
    are valid and then uses some logic to determine if they are 
    equavalent and if not indicate that they are not 
    returns True (they both match) or False (they do not match)
    '''
    source_value = boundary_checker(source_city) # either True or False
    google_value = boundary_checker(g_city)

    def letter_match(source_city, g_city):
        sc = source_city.lower()
        gc = g_city.lower()
        # b/c there are only 2 valid cities, we can see if the first letters
        # are the same to test equivalence.
        if sc[0] == gc[0]: 
            return True # they both match
        else:
            return False # they don't match
    
    # matching, source is valid, google is valid
    return (letter_match(source_city, g_city), source_value, google_value)
       
def two_city_logger(applicant, source_city, g_city):
    result = two_city_parser(source_city, g_city)
    log_string = None
    if not all(result): # cities match and are both valid
        matching, sv, gv = result
        if all([sv, gv]): # if source city and google are valid cities
            if not matching: # but don't match
                log_string = """##11## {} returned valid city, but source: {} does not match google: {}""".format(applicant, 
                                                                                                           source_city, 
                                                                                                           g_city)
        if not all([sv,gv]): # if source or google are not valid
            if not sv: # if source is not valid 
                log_string = """##12## {} has an invalid source City: {}""".format(applicant, 
                                                                            source_city)
            if not gv: # if we got an odd geocoding result
                log_string = """##13## {} source city {} returned invalid google city {}""".format(applicant, 
                                                                                             source_city, 
                                                                                             g_city)
    if log_string:
        write_to_logs(applicant, log_string, 'two_city')
    if not log_string:
        raise Exception('ERROR parsing google vs. source address for {}'.format(applicant))
        
