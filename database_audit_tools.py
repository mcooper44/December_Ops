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

from address_parser_and_geocoder import AddressParser, SQLdatabase
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
    
    street_types = {'a': ('avenue', 'ave', 'ave est', 'av'),
                  'b': ('boulevard','blvd','bend', 'boul'),
                  'c': ('Ct', 'crt','crest','crescent','cres','cr','court','circle', 'crcl', 'cir', 'cl'),
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
    street_key = None # what is the first letter of street?
    dir_key = None # what is the first letter of north?
    
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




if __name__ == '__main__':
    address_parser = AddressParser() # I strip out extraneous junk from address strings and set type flags
    
    dbase = SQLdatabase() # I have methods to access a database and return geocode/address info
    dbase.connect_to('atest.db', create=True)
    
    fnames = Field_Names('header_config.csv') # I am header names
    fnames.init_index_dict() 
    
    export_file = Export_File_Parser('test_export.csv',fnames.ID) # I open a csv export from a cloud database
    export_file.open_file()

    for line in export_file.file_object: # for line in the csv file object       
               
        line_object = Visit_Line_Object(line,fnames.ID)
        address, city, _ = line_object.get_address()
        applicant = line_object.get_applicant_ID()

        cities_served = ('kitchener', 'waterloo')
        # check boundary error issues
        if city.lower() not in cities_served:
            print('Flag {} for out of bounds'.format(applicant))

        decon_address = address_parser.parse(address) # returns ('301 Front Street West', flags) or False
        if decon_address:
            parsed_address, flags = decon_address
            source_post_types = parse_post_types(parsed_address) # is there a street type and/or direction?
            coords_from_db = dbase.get_coordinates(parsed_address, city)
            if coords_from_db:
                lat, lng = coords_from_db
                flags_from_db = dbase.pull_flags_at(lat, lng) # 
                # create a reference to compare the flagged addresses from the database with                
                source_units_flag, source_dirs_flag, source_post_flag = flags['MultiUnit'], flags['Direction'], flags['PostType']
                reference_object = (parsed_address, city, source_units_flag, source_dirs_flag, source_post_flag)
                
                for returned_tuple in flags_from_db:                    

                    if reference_object != returned_tuple:
                        # check Unit or Direction Errors
                        _, _, db_uf, db_df, db_pf = returned_tuple
                        logging.info('{} has these flags: Unit Flag {} Direction Flag {} Post Type Flag {}'.format(applicant, db_uf, db_df, db_pf))
                        print('{} does not equal {}'.format(reference_object,returned_tuple))
                    else:                        
                        print('{} equals {}'.format(reference_object,returned_tuple))

                    address_from_dbase = returned_tuple[0] # e.g. 100 Regina St
                    post_types_from_dbase = parse_post_types(address_from_dbase)
                    
                    post_type_evaluation = evaluate_post_types(source_post_types, post_types_from_dbase)
                    
                    if any(post_type_evaluation): # if any of the flags were mismatched
                        one, two, three = post_type_evaluation
                        print('Name Type Error = {} Direction Type Error = {} Eval Flag = {}'.format(one, two, three))
                        logging.info('Name Type Error = {} Direction Type Error = {} Eval Flag = {}'.format(one, two, three))
            
            else:                
                raise Exception('Not_Logged_In_Database_{}'.format(applicant))
        else:            
            raise Exception('Address_Parse_Error_{}'.format(applicant))

    dbase.close_db()
