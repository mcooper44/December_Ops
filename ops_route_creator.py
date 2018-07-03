import sqlite3
from basket_sorting_Geocodes import Delivery_Household
from basket_sorting_Geocodes import Delivery_Routes
from basket_sorting_Geocodes import Route_Database
from basket_sorting_Geocodes import Delivery_Household_Collection 
from collections import namedtuple
from address_parser_and_geocoder import Coordinates  
from address_parser_and_geocoder import SQLdatabase
from address_parser_and_geocoder import AddressParser
from db_data_models import Field_Names
from db_data_models import Visit_Line_Object
from db_data_models import Export_File_Parser 


Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')

'''
Hello.  I am the deliverator.  I am here to create routes
this script will run through a l2f export file that has been cleaned up
after iterative address fixing passes and quality control via the
address_parser... script and careful digesting of logs.

open the csv and strip out the households line by line
log them into a database in the correct structure for later

run through the households in the database and add them to a
Delivery_Household_Collection

iterate through the collection of households to create routes
log the routes in the database in a separate table

the database will then yeild the materials necessary to make the route cards
and should be more portable

'''

address_parser = AddressParser() # I strip out extraneous junk from address strings

dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
dbase.connect_to('Address.db', create=True) # testing = atest.db
    
fnames = Field_Names('2018sourcec.csv') # I am header names
fnames.init_index_dict() 
export_file = Export_File_Parser('2018sourcec.csv',fnames.ID) # I open a csv 
    # for testing us test_export.csv
export_file.open_file()

a2018routes = Delivery_Routes(7, 1) 
delivery_households = Delivery_Household_Collection()

for line in export_file: # I am a csv object
    line_object = Visit_Line_Object(line,fnames.ID)
    address, city, _ = line_object.get_address()
    applicant = line_object.get_applicant_ID()
    family_size = line_object.visit_household_Size
    add2 = None
    diet = None
    email = None
    phone = None
    try:
        simple_address, _ = address_parser.parse(address, applicant)
        if simple_address:
            lt, lg =  dbase.get_coordinates(simple_address, city)   
            if all([lt, lg]):
                # insert into database or jump to building households
                # delivery households need to hold more data points if they
                # will be a structure to hand data over to the delivery cards
                # et al.
                delivery_households.add_household(applicant, None, family_size, lt, lg)
    except:
        print('error attempting to get geocodes from db with {} {}'.format(address, city))

a2018routes.sort_method(delivery_households)
for house in delivery_households:
    print(house.return_route())

    # insert into database methods go here









