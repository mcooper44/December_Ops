from address_parser_and_geocoder import AddressParser, SQLdatabase
from db_data_models import Field_Names, Export_File_Parser, Visit_Line_Object
import usaddress

def parse_post_types(address):
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
    eval_flag = False
    try:
        tagged_address = usaddress.tag(address)
        tags, type_tag = tagged_address
        if type_tag == 'Street Address':            
            if 'StreetNamePostType' in tags.keys():
                tag_value = tags['StreetNamePostType']
                if tag_value[0].lower() in street_types.keys():
                    if tag_value.lower() in street_types[tag_value[0]]:
                        streetnameptype = True
                else:
                    eval_flag = True
            
            else:
                # nothing to see here
                pass
            if 'StreetNamePostDirectional' in tags.keys():
                tag_value = tags['StreetNamePostDirectional']
                if tag_value[0].lower() in directions.keys():
                    if tag_value.lower() in directions[tag_value[0]]:
                        streetnamepdir = True
                else:
                    eval_flag = True
        
        else:
            print('invalid tag response')
    except:
        print('we could not parse!')
    
    return (streetnameptype, streetnamepdir, eval_flag)


if __name__ == '__main__':
    address_parser = AddressParser() # I strip out extraneous junk from address strings
    
    dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
    dbase.connect_to('atest.db', create=True)
    
    fnames = Field_Names('header_config.csv') # I am header names
    fnames.init_index_dict() 
    
    export_file = Export_File_Parser('test_export.csv',fnames.ID) # I open a csv
    export_file.open_file()

    for line in export_file.file_object: # I am a csv object
        line_object = Visit_Line_Object(line,fnames.ID)
        address, city, _ = line_object.get_address()
        applicant = line_object.get_applicant_ID()
                
        decon_address = address_parser.parse(address) # returns ('301 Front Street West', flags) or False
        if decon_address:
            parsed_address, flags = decon_address
            source_post_types = parse_post_type(parsed_address)
            coords_from_db = dbase.get_coordinates(parsed_address, city)
            if coords_from_db:
                lat, lng = coords_from_db
                flags_from_db = dbase.pull_flags_at(lat, lng)
                # create a reference to compare the flagged addresses from the database with                
                source_units_flag, source_dirs_flag = flags['MultiUnit'], flags['Direction']
                reference_object = (parsed_address, city, source_units_flag, source_dirs_flag)

                for returned_tuple in flags_from_db:
                    cities_served = ('kitchener', 'waterloo' )
                    if city.lower() not in cities_served:
                        print('Flag {} for out of bounds'.format(applicant))

                    if reference_object != returned_tuple:
                        print("ERROR WITH {}".format(applicant))
                        print('{} does not equal {}'.format(reference_object,returned_tuple))
                    else:
                        print('Everything is fine with {}'.format(applicant))
                        print('{} equals {}'.format(reference_object,returned_tuple))

                    address_from_dbase = returned_tuple[0]
                    post_types_from_dbase = parse_post_type(address_from_dbase)
                    if source_post_types != post_types_from_database:
                        print('{} has mismatched post types'.format(applicant))

            else:                
                raise Exception('Not_Logged_In_Database_{}'.format(applicant))
        else:            
            raise Exception('Address_Parse_Error_{}'.format(applicant))



    dbase.close_db()
