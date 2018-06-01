import sqlite3
from basket_sorting_Geocodes import Delivery_Household
from basket_sorting_Geocodes import Delivery_Routes

Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')

'''
Hello.  I am the deliverator.  I am here to create routes
this script will run through a l2f export file that has been cleaned up
after iterative address fixing passes and quality control via the
address_parser... script and careful digesting of logs.
It will find the geocoordinates of each hh and create a datastructure of 
Delivery_Household's to feed to the Delivery_Routes class for routing

TO DO: refactor basket_sorting... to separate out the log to db stuff
into a separte route db class
also add an iter method to the Delivery_Routes Class so we can cycle through
it and drop the routes into a DB
'''

coordinate_manager = Coordinates() # I lookup and manage coordinate data
address_parser = AddressParser() # I strip out extraneous junk from address strings

dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
dbase.connect_to('Address.db', create=True) # testing = atest.db
    
fnames = Field_Names('2018source.csv') # I am header names
fnames.init_index_dict() 
export_file = Export_File_Parser('2018source.csv',fnames.ID) # I open a csv 
    # for testing us test_export.csv
export_file.open_file()

2018routes = Delivery_Routes(7, 1) 


hh_dict = {}

for line in export_file: # I am a csv object
    line_object = Visit_Line_Object(line,fnames.ID)
    address, city, _ = line_object.get_address()
    applicant = line_object.get_applicant_ID()
    family size = line_object.visit_household_Size

    simple_address, _ = address_parser.parse(address, applicant)
    lt, lg =  coordinate_manager.get_coordinates(simple_address, city)   
    if all(lt, lg):
        hh_dict[file_id] = Delivery_Household(applicant, None, family_size, lt, lg)

2018routes.get_status()
2018routes.get_route_collection(hh_dict))
2018routes.sort_method()
2018routes.create_route_db('2018.db')
2018routes.log_route_in_db()








