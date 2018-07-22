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
# At the moment it does not, but it should provide methods to rewrite the csv with
# corrected address information
#####################################################################################

from address_parser_and_geocoder import AddressParser, SQLdatabase, Coordinates
from db_data_models import Field_Names, Export_File_Parser, Visit_Line_Object
from address_audit_tools import boundary_logger
from address_audit_tools import missing_element_logger
from address_audit_tools import parse_post_types
from address_audit_tools import post_type_logger

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
        #print('we are starting with {} living at {}, {}'.format(applicant, address, city))
        parsed_address, flags = address_parser.parse(address)
        #print('their parsed address is {} and has flags {}'.format(parsed_address, flags))
        lat, lng = dbase.get_coordinates(parsed_address, city)
        if any([lat, lng]):
            #print('coordinates are {}, {}'.format(lat, lng))
            flags_from_db = dbase.pull_flags_at(lat, lng) # 3 tuple from the database
            #print('we have pulled database flags for those coordinates are they are {}'.format(flags_from_db))
            address_from_dbase =  dbase.get_address(lat, lng)# e.g. 100 Regina St
            
            #print('the address logged in the database at coordinates {} {} is: {}'.format(lat, lng, address_from_dbase))

            boundary_logger(applicant, city) # check to see if the city is out of bounds

            # then look to see if any items are missing and if they are, log them       
            missing_element_logger(applicant, flags, parsed_address, city, flags_from_db)

            # finally check to see if anything is there, but doesn't match the
            # canonical object e.g. input address is 100 Regina st West,
            # but shoudl be ... St South.  IF so, log it all!!!!!!!!
            post_types_from_dbase = parse_post_types(address_from_dbase)
            source_post_types = parse_post_types(parsed_address) # is there a street type and/or direction? 
            post_type_logger(applicant, source_post_types, post_types_from_dbase)
        else:
            print('{} with {} address has not been logged in the database'.format(applicant, address)) 
    
    dbase.close_db()