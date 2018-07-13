import sqlite3
from collections import namedtuple
import logging

from basket_sorting_Geocodes import Delivery_Household
from basket_sorting_Geocodes import Delivery_Routes
from basket_sorting_Geocodes import Route_Database
from basket_sorting_Geocodes import Delivery_Household_Collection 

from address_parser_and_geocoder import Coordinates  
from address_parser_and_geocoder import SQLdatabase
from address_parser_and_geocoder import AddressParser

from db_data_models import Field_Names
from db_data_models import Visit_Line_Object
from db_data_models import Export_File_Parser 
from db_data_models import Person

from delivery_card_creator import Delivery_Slips


Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')

ops_logger = logging.getLogger('ops')
ops_logger.setLevel(logging.INFO)
ops_logger_formatter = logging.Formatter('%(message)s')
ops_log_file_handler = logging.FileHandler('Logging/ops.log')
ops_log_file_handler.setFormatter(ops_logger_formatter)
ops_logger.addHandler(ops_log_file_handler)

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

address_dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
address_dbase.connect_to('Address.db', create=True) # testing = atest.db

route_database = Route_Database('2018rdb.db')

fnames = Field_Names('2018sourcec.csv') # I am header names
export_file = Export_File_Parser('2018sourcec.csv',fnames.ID) # I open a csv 
export_file.open_file()

a2018routes = Delivery_Routes(7, 1) 
delivery_households = Delivery_Household_Collection()

slips = Delivery_Slips('2018_test.xlsx')

# open the source file and parse the households out of it
# store the main applicant info, address etc. as well as family member details
# for use later if needed.

for line in export_file: # I am a csv object
    line_object = Visit_Line_Object(line,fnames.ID)
    summary = line_object.get_HH_summary() # returns a named tuple
    applicant = summary.applicant
    address = summary.address
    city = summary.city
    family_size = summary.size
    simple_address, _ = address_parser.parse(address, city) 
   
    try:
        lt, lg = address_dbase.get_coordinates(simple_address, city)   
        if all([lt, lg]):
            # insert base information needed to build a route and card
            delivery_households.add_household(applicant, None, family_size,
                                                  lt, lg, summary)
            ops_logger.info('{} has lat {} lng {}'.format(applicant, lt, lg))
        else:
            ops_logger.error('{} has no geocodes and will not be included in the routes'.format(applicant))
    except Exception as errr:
        print('Error attempting to find coordinates with {}. Raised:\
              {}'.format(applicant, errr))

    # add individual details for each family member to the d_h object
    # if present.
    try:
        if line_object.has_family():
            family = line_object.get_family_members(fnames) # returns [tuples]
            # ID, fname, lname, dob, age, gender, ethno, disability
            delivery_households.add_hh_family(applicant, family)
            ops_logger.info('{} has family and they have been stored in dhh object'.format(applicant))
    except Exception as oops:
        print('attempting to deal with family for {} but encountered {}'.format(applicant, oops))


# Sort the Households into Routes and 
# pass the route numbers and labels back into the delivery households
# object
a2018routes.sort_method(delivery_households)

# populate the database with summary and route data
for house in delivery_households:
    app, rn, rl = house.return_route()

    summ = house.return_summary()
    # parse out the summary data
    applicant = summ.applicant
    fname = summ.fname
    lname = summ.lname
    email = summ.email
    phone = summ.phone
    address = summ.address
    add2 = summ.address2
    city = summ.city
    family_size = summ.size
    diet = summ.diet
    card_sum = None
    if not route_database.prev_routed(applicant):
        route_database.add_route(app, rn, rl)
        ops_logger.info('{} has been added to rt db'.format(applicant))
    else:
        ops_logger.error('{} has been added to route db already'.format(applicant)) 
    if not route_database.fam_prev_entered(applicant):
        route_database.add_family((applicant,
                               fname,
                               lname,
                               email,
                               phone,
                               address,
                               add2,
                               city,
                               family_size,
                               diet))
        ops_logger.info('{} has been logged to applicants db'.format(applicant))
    else:
        ops_logger.error('{} has prev. been logged to applicants db'.format(applicant)) 
    # need to do a check in the database to verify that they have not been
    # added already
    if house.family_members:
        for person in house.family_members:
            pid = person[0]
            if not route_database.fam_member_prev_entered(pid):
                person_o = Person(person)
                # insert ID, Fname, Lname, Age
                base = person_o.get_base_profile()
                route_database.add_family_member(applicant,base)
                ops_logger.info('added {} to family db table'.format(person))
            else:
                ops_logger.info('{} already exists in family table'.format(pid))


# lets print some slips!
# TODO

current_rt = 0
for house in delivery_households.route_iter():
    rt =  house.return_route() # the route info
    summ = house.return_summary() # the HH info (name, address etc.)
    
    rt_str = str(rt[1]) # because the route number is an int
    if rt[1] > current_rt:
        rt_card_summary = delivery_households.route_summaries.get(rt_str, None) 
        if rt_card_summary:
            slips.add_route_summary_card(rt_card_summary)
            current_rt += 1
    slips.add_household(rt, summ) # adds another card to the file
    ops_logger.info('{} added to card stack'.format(rt))

route_database.close_db()
address_dbase.close_db()
slips.close_worksheet()




