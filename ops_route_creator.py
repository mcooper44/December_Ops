#!/usr/bin/python3.6

import sqlite3
from collections import namedtuple
import logging
from datetime import datetime

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
from db_parse_functions import itr_joiner

from kw_neighbourhoods import Neighbourhoods

from delivery_card_creator import Delivery_Slips
from delivery_binder import Binder_Sheet
from delivery_binder import Office_Sheet 

from sponsor_reports import Report_File

Client = namedtuple('Client', 'size location')
Geolocation = namedtuple('Geolocation', 'lat long')

ops_logger = logging.getLogger('ops')
ops_logger.setLevel(logging.INFO)
ops_logger_formatter = logging.Formatter('%(message)s')
ops_log_file_handler = logging.FileHandler('Logging/ops.log')
ops_log_file_handler.setFormatter(ops_logger_formatter)
ops_logger.addHandler(ops_log_file_handler)
ops_logger.info('Running new session {}'.format(datetime.now()))

nr_logger = logging.getLogger('nr') # not routed
nr_logger.setLevel(logging.INFO)
nr_logger_formatter = logging.Formatter('%(message)s')
nr_log_file_handler = logging.FileHandler('Logging/nr.log')
nr_log_file_handler.setFormatter(ops_logger_formatter)
nr_logger.addHandler(nr_log_file_handler)
nr_logger.info('Running new session {}'.format(datetime.now()))

add_log = logging.getLogger('add') # address related
add_log.setLevel(logging.INFO)
add_log_formatter = logging.Formatter('%(message)s')
add_log_file_handler = logging.FileHandler('Logging/add.log')
add_log_file_handler.setFormatter(add_log_formatter)
add_log.addHandler(add_log_file_handler)
add_log.info('Running new session {}'.format(datetime.now()))
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
class NotCoded(Exception):
    '''
    This Exception class is a custom class to communicate that 
    an address has not been geocoded previously and that they
    should!
    '''
    pass


address_parser = AddressParser() # I strip out extraneous junk from address strings
coordinate_manager = Coordinates()

address_dbase = SQLdatabase() # I recieve the geocoded information from parsed address strings
address_dbase.connect_to('Address.db', create=True) # testing = atest.db

route_database = Route_Database('2018rdb.db')

fnames = Field_Names('nov5.csv') # I am header names
export_file = Export_File_Parser('nov5.csv',fnames) # I open a csv 
export_file.open_file()

a2018routes = Delivery_Routes(7, 1)  # Configure the max number of boxes and
                                     # the starting route number
# delivery households go into this object
delivery_households = Delivery_Household_Collection()
# record all the sponsor details into this object by key
sponsored_households = {'DOON': Delivery_Household_Collection(),
                        'SERTOMA' : Delivery_Household_Collection(),
                        'REITZEL' : Delivery_Household_Collection()
                       }

k_w = Neighbourhoods(r'City of Waterloo and Kitchener Planning district Geometry.json')
k_w.extract_shapes() # get shapes ready to test points

slips = Delivery_Slips('2018_test.xlsx') # source file for the households

# open the source file and parse the households out of it
# store the main applicant info, address etc. as well as family member details
# for use later if needed.
# NB: the export file needs to be cleaned and should only include HH
# that have requested services from us.  This script will just strip
# all the HH from the file and make route cards for them.

for line in export_file: # I am a csv object
    line_object = Visit_Line_Object(line,fnames.ID, december_flag = True)
    is_xmas = line_object.is_christmas_hamper()
    summary = line_object.get_HH_summary() # returns a named tuple
    applicant = summary.applicant
    is_routed = route_database.prev_routed(applicant)
    
    sponsored, food_sponsor, gift_sponsor = line_object.is_sponsored_hamper()
    
    
    # extract summary from the visit line
    # this will be inserted into a HH object and provides the key
    # bits of info that we need to build a route card
    
    print('applicant: {} christmas status: {} route status: {}'.format(applicant, 
                                                                       is_xmas,
                                                                       is_routed))
    print('sponsored: {} by {} and-or {}'.format(sponsored, 
                                                 food_sponsor,
                                                 gift_sponsor))
    if (is_xmas or sponsored) and not is_routed:
        address = summary.address
        pos_code = summary.postal
        city = summary.city
        family_size = summary.size
        simple_address = None
        print('is xmas {} or sponsored {}'.format(is_xmas, sponsored))
        try:
            # attempt to strip out extraneous details from the address
            # such as unit number etc. 
            simple_address, _ = address_parser.parse(address, applicant) 
            print('simple_address: {}'.format(simple_address))
        except Exception as a_err:
            add_log.error('{} raised {} from {}'.format(applicant, a_err, address))
            nr_logger.error('{} raised an error during address parse'.format(applicant))
        # ping database with simle address  to see if there is a geocoded address
        try:
            lt, lg = address_dbase.get_coordinates(simple_address, city)   
            if all([lt, lg]): # if there is a previously geocoded address
                # insert base information needed to build a route and card
                n_hood = k_w.find_in_shapes(lt, lg) # find neighbourhood
                add_log.info('{} is in this neighbourhood: {}'.format(applicant,
                                                                        n_hood))
                # create a HH object and insert the summary we need to build 
                # a route (lt, lg)a route card(summary). 
                # and later a route summary (n_hood)
                if not food_sponsor: # if not sponsored by doon or reitzel
                    delivery_households.add_household(applicant, None, 
                                                  family_size,
                                                  lt, lg, summary, 
                                                  n_hood)

                    add_log.info('{} has lat {} lng {}'.format(applicant, 
                                                               lt, lg))
                if food_sponsor:
                    # sponsors should be held in a dictionary
                    # record the sponsor in the datastructure
                    # and then later we will create reports based
                    # of the data for each sponsor
                    print('added {} to food sponsor {}'.format(applicant,
                                                               food_sponsor))
                    sponsored_households[food_sponsor].add_household(applicant,
                                                                     None,
                                                                     family_size,
                                                                     lt, lg,
                                                                     summary,
                                                                     n_hood)
                if gift_sponsor:
                    print('added {} to gift sponsor {}'.format(applicant,
                                                               gift_sponsor))
                    print('gift sponsors are: ')
                    sponsored_households[gift_sponsor]
                    print('of type  {}'.format(type(sponsored_households[gift_sponsor])))
                    sponsored_households[gift_sponsor].add_household(applicant,
                                                                     None,
                                                                     family_size,
                                                                     lt, lg,
                                                                     summary,
                                                                     n_hood)
            else: # if we have not geocoded the address
            # we need to raise and exception.  It is better to 
            # run the geocoding script first and dealing with potential errors
                raise NotCoded('{} has not been geocoded! Run the gc script 1st'.format(address))
                
        except Exception as errr:
            nr_logger.error('{} has raised {} and was not routed'.format(applicant, 
                                                                         errr))
        # add individual details for each family member to the d_h object
        # if present.
        try:
            if line_object.has_family():
                family = line_object.get_family_members(fnames) # returns [tuples]
                print(family)
                # ID, fname, lname, dob, age, gender, ethno, disability
                delivery_households.add_hh_family(applicant, family)
                ops_logger.info('{} has family and they have been stored in dhh object'.format(applicant))
                if food_sponsor:
                    sponsored_households[food_sponsor].add_hh_family(applicant,
                                                                    family)
                if gift_sponsor:
                    sponsored_households[gift_sponsor].add_hh_family(applicant,
                                                                    family)
        except Exception as oops:
            nr_logger.error('{} has family, but they were not stored in dhh object, due to {}'.format(applicant, oops))
        
        ### TO DO: insert try/except block here to add kids to the sponsor list
    
    else:
        nr_logger.info('{} was not routed. is xmas = {} route = {}'.format(applicant, 
                                                                           is_xmas, 
                                                                           is_routed))

# Sort the Households into Routes and 
# pass the route numbers and labels back into the delivery households
# object
starting_rn = route_database.return_last_rn() # find last rn
a2018routes.start_count = int(starting_rn) + 1 # reset rn to resume from last route
a2018routes.sort_method(delivery_households) # start sorting

print('picking up after route number: {}'.format(starting_rn))

# populate the database with summary and route data
for house in delivery_households:
    applicant, rn, rl = house.return_route()
    # get the summary from the HH object
    # created by the visit_line
    summ = house.return_summary()  
    # parse out the summary data
    fname = summ.fname
    lname = summ.lname
    email = summ.email
    phone = itr_joiner(summ.phone)
    address = summ.address
    add2 = summ.address2
    city = summ.city
    family_size = summ.size
    diet = summ.diet
    n_hd = house.neighbourhood
    # add household to the summary data
    app_tupe = (applicant,
                fname,
                lname,
                family_size,
                phone,
                email,
                address,
                add2,
                city,
                diet,
                n_hd,)
    ops_logger.info('{}'.format(app_tupe))                              
    if not route_database.prev_routed(applicant):
        route_database.add_route(applicant, rn, rl)
        ops_logger.info('{} has been added to rt db'.format(applicant))
    else:
        ops_logger.error('{} has been added to route db already'.format(applicant)) 
    if not route_database.fam_prev_entered(applicant):
        route_database.add_family(app_tupe) # add info for routecard
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
# and the delivery binder
# and some sponsor reports

route_binder = Binder_Sheet('2018_route_binder.xlsx') # 
ops_ref = Office_Sheet('2018_operations_reference.xlsx')
# the sponsor report dictionary will produce a report for each organization
s_report = {'DOON': Report_File('2018_sponsor_report_DOON.xlsx'),
            'SERTOMA': Report_File('2018_sponsor_report_SERTOMA.xlsx'),
            'REITZEL': Report_File('2018_sponsor_report_REITZEL.xlsx')
           }


current_rt = int(starting_rn) - 1 # to keep track of the need to print 
                                    # a summary card or not
for house in delivery_households.route_iter():
    rt =  house.return_route() # the route info (fid, rn, rl)
    summ = house.return_summary() # the HH info (name, address etc.)
    fam = house.family_members    

    ops_ref.add_line(rt, summ, fam) # delivery binder
    ops_logger.info('Added {} in route {} to ops reference'.format(rt[0],
                                                                   rt[1]))
    # the following two lines need to be pulled out into a separate 
    # pipeline - we need a hh structure for true sponsored households
    #s_report.add_household(summ, fam) # sponsor report
       
    rt_str = str(rt[1]) # because the route number is an int 
    # decide if now is the right time to insert a route summay on the stack
    rt_card_summary = delivery_households.route_summaries.get(rt_str, None) 
    if rt[1] > current_rt: # if we have the next route number
        if rt_card_summary: # and the summary is there
            # add a summary card because we are at the start of a new route
            slips.add_route_summary_card(rt_card_summary)
            current_rt += 1

            route_binder.add_route(rt_card_summary) # add an entry to the route
                                                    # binder as well
            ops_logger.info('route {} added to stack and route_binder'.format(rt[1]))
        else:
            ops_logger.error('missed a route card summary for rn {}'.format(rt))
    slips.add_household(rt, summ) # adds another card to the file
    ops_logger.info('{} added to card stack'.format(rt))

# insert data into sponsor reports
for sponsor_group in sponsored_households.keys():
    print('operating on {}'.format(sponsor_group))
    for household in sponsored_households[sponsor_group].route_iter():
        rt = household.return_route()
        summ = household.return_summary() # the HH info (name, address etc.)
        fam = household.family_members
        print('attempting to write {} with family {}'.format(rt[0], fam))
        s_report[sponsor_group].add_household(summ, fam) # sponsor report
        ops_logger.info('Added {} to {} sponsor report'.format(rt[0],
                                                               sponsor_group))


route_database.close_db()
address_dbase.close_db()
slips.close_worksheet()
route_binder.close_worksheet()
ops_ref.close_worksheet() 
for x in s_report.keys():
    s_report[x].close_worksheet()

